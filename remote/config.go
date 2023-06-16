package remote

import (
	"crypto/tls"
	"fmt"
	"net/http"
	"time"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/bufbuild/connect-go"
	"github.com/rs/cors"
)

func defaultConfig() *Config {
	return &Config{
		AdvertisedHost: "",

		EndpointManagerBatchSize: 1000,
		EndpointWriterQueueSize:  1000000,
		EndpointManagerQueueSize: 1000000,
		Kinds:                    make(map[string]*actor.Props),
		MaxRetryCount:            5,
		Scheme:                   "http",
		ConnectServerHTTPOptions: HTTPServerOptions{
			ReadHeaderTimeout: time.Second,
			ReadTimeout:       5 * time.Minute,
			WriteTimeout:      5 * time.Minute,
			MaxHeaderBytes:    8 * 1024, // 8KiB
		},
		ConnectClientHTTPOptions: HTTPClientOptions{
			ReadIdleTimeout:  15 * time.Second,
			PingTimeout:      0,
			WriteByteTimeout: 0,
		},
		ConnectCorsOptions: cors.New(cors.Options{
			AllowedMethods: []string{
				http.MethodHead,
				http.MethodGet,
				http.MethodPost,
				http.MethodPut,
				http.MethodPatch,
				http.MethodDelete,
			},
			AllowOriginFunc: func(origin string) bool {
				// Allow all origins, which effectively disables CORS.
				return true
			},
			AllowedHeaders: []string{"*"},
			ExposedHeaders: []string{
				// Content-Type is in the default safelist.
				"Accept",
				"Accept-Encoding",
				"Accept-Post",
				"Connect-Accept-Encoding",
				"Connect-Content-Encoding",
				"Content-Encoding",
				"Grpc-Accept-Encoding",
				"Grpc-Encoding",
				"Grpc-Message",
				"Grpc-Status",
				"Grpc-Status-Details-Bin",
			},
			// Let browsers cache CORS information for longer, which reduces the number
			// of preflight requests. Any changes to ExposedHeaders won't take effect
			// until the cached data expires. FF caps this value at 24h, and modern
			// Chrome caps it at 2h.
			MaxAge: int(2 * time.Hour / time.Second),
		}),
	}
}

func newConfig(options ...ConfigOption) *Config {
	config := defaultConfig()
	for _, option := range options {
		option(config)
	}
	return config
}

// Address returns the address of the remote
func (rc Config) Address() string {
	return fmt.Sprintf("%v:%v", rc.Host, rc.Port)
}

// Configure configures the remote
func Configure(host string, port int, options ...ConfigOption) *Config {
	c := newConfig(options...)
	c.Host = host
	c.Port = port

	return c
}

type HTTPServerOptions struct {
	ReadHeaderTimeout time.Duration
	ReadTimeout       time.Duration
	WriteTimeout      time.Duration
	MaxHeaderBytes    int
}
type HTTPClientOptions struct {
	ReadIdleTimeout  time.Duration
	PingTimeout      time.Duration
	WriteByteTimeout time.Duration
}

// Config is the configuration for the remote
type Config struct {
	Host           string
	Port           int
	AdvertisedHost string

	ConnectClientOptions     []connect.ClientOption
	ConnectHandlerOptions    []connect.HandlerOption
	ConnectServerHTTPOptions HTTPServerOptions
	ConnectServerTLSConfig   *tls.Config
	ConnectClientHTTPOptions HTTPClientOptions
	ConnectClientTLSConfig   *tls.Config

	ConnectCorsOptions       *cors.Cors
	EndpointWriterBatchSize  int
	EndpointWriterQueueSize  int
	EndpointManagerBatchSize int
	EndpointManagerQueueSize int
	Kinds                    map[string]*actor.Props
	MaxRetryCount            int
	Scheme                   string
}
