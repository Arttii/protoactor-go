package remote

import (
	"net"
	"net/http"
	"time"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/extensions"
	"github.com/asynkron/protoactor-go/log"
	"github.com/asynkron/protoactor-go/remote/gen/genconnect"
	"golang.org/x/net/context"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

var extensionId = extensions.NextExtensionID()

type Remote struct {
	actorSystem  *actor.ActorSystem
	s            *http.Server
	edpReader    *endpointReader
	edpManager   *endpointManager
	config       *Config
	kinds        map[string]*actor.Props
	activatorPid *actor.PID
	blocklist    *BlockList
}

func NewRemote(actorSystem *actor.ActorSystem, config *Config) *Remote {
	r := &Remote{
		actorSystem: actorSystem,
		config:      config,
		kinds:       make(map[string]*actor.Props),
		blocklist:   NewBlockList(),
	}
	for k, v := range config.Kinds {
		r.kinds[k] = v
	}

	actorSystem.Extensions.Register(r)

	return r
}

//goland:noinspection GoUnusedExportedFunction
func GetRemote(actorSystem *actor.ActorSystem) *Remote {
	r := actorSystem.Extensions.Get(extensionId)

	return r.(*Remote)
}

func (r *Remote) ExtensionID() extensions.ExtensionID {
	return extensionId
}

func (r *Remote) BlockList() *BlockList { return r.blocklist }

// Start the remote server
func (r *Remote) Start() {
	l, err := net.Listen("tcp", r.config.Address())

	if err != nil {
		panic(err)
	}

	var address string
	if r.config.AdvertisedHost != "" {
		address = r.config.AdvertisedHost
	} else {
		address = l.Addr().String()
	}

	r.actorSystem.ProcessRegistry.RegisterAddressResolver(r.remoteHandler)
	r.actorSystem.ProcessRegistry.Address = address
	plog.Info("Starting remote with address", log.String("address", address))

	r.edpManager = newEndpointManager(r)
	r.edpManager.start()

	r.edpReader = newEndpointReader(r)

	mux := http.NewServeMux()

	path, handler := genconnect.NewRemotingHandler(r.edpReader, r.config.ConnectHandlerOptions...)

	mux.Handle(path, handler)

	plog.Info("Starting Proto.Actor server", log.String("address", address))

	srv := &http.Server{
		Addr: address,
		Handler: h2c.NewHandler(
			r.config.ConnectCorsOptions.Handler(mux),
			&http2.Server{},
		),
		TLSConfig:         r.config.ConnectClientTLSConfig,
		ReadHeaderTimeout: r.config.ConnectServerHTTPOptions.ReadHeaderTimeout,
		ReadTimeout:       r.config.ConnectServerHTTPOptions.ReadTimeout,
		WriteTimeout:      r.config.ConnectServerHTTPOptions.WriteTimeout,
		MaxHeaderBytes:    r.config.ConnectServerHTTPOptions.MaxHeaderBytes,
	}
	r.s = srv
	go srv.Serve(l)
}

func (r *Remote) Shutdown(graceful bool) {
	if graceful {
		// TODO: need more graceful
		r.edpReader.suspend(true)
		r.edpManager.stop()

		// For some reason GRPC doesn't want to stop
		// Setup timeout as workaround but need to figure out in the future.
		// TODO: grpc not stopping
		c := make(chan bool, 1)
		go func() {
			r.s.Shutdown(context.Background())
			c <- true
		}()

		select {
		case <-c:
			plog.Info("Stopped Proto.Actor server")
		case <-time.After(time.Second * 10):
			r.s.Close()
			plog.Info("Stopped Proto.Actor server", log.String("err", "timeout"))
		}
	} else {
		r.s.Close()
		plog.Info("Killed Proto.Actor server")
	}
}

func (r *Remote) SendMessage(pid *actor.PID, header actor.ReadonlyMessageHeader, message interface{}, sender *actor.PID, serializerID int32) {
	rd := &remoteDeliver{
		header:       header,
		message:      message,
		sender:       sender,
		target:       pid,
		serializerID: serializerID,
	}
	r.edpManager.remoteDeliver(rd)
}
