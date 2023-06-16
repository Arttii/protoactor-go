package remote

import (
	"crypto/tls"

	"github.com/rs/cors"
)

type ConfigOption func(config *Config)

// WithEndpointWriterBatchSize sets the batch size for the endpoint writer
func WithEndpointWriterBatchSize(batchSize int) ConfigOption {
	return func(config *Config) {
		config.EndpointWriterBatchSize = batchSize
	}
}

// WithEndpointWriterQueueSize sets the queue size for the endpoint writer
func WithEndpointWriterQueueSize(queueSize int) ConfigOption {
	return func(config *Config) {
		config.EndpointWriterQueueSize = queueSize
	}
}

// WithEndpointManagerBatchSize sets the batch size for the endpoint manager
func WithEndpointManagerBatchSize(batchSize int) ConfigOption {
	return func(config *Config) {
		config.EndpointManagerBatchSize = batchSize
	}
}

// WithEndpointManagerQueueSize sets the queue size for the endpoint manager
func WithEndpointManagerQueueSize(queueSize int) ConfigOption {
	return func(config *Config) {
		config.EndpointManagerQueueSize = queueSize
	}
}

// WithAdvertisedHost sets the advertised host for the remote
func WithAdvertisedHost(address string) ConfigOption {
	return func(config *Config) {
		config.AdvertisedHost = address
	}
}

// WithHTTPScheme sets the http/https scheme for the remote
func WithHTTPScheme(scheme string) ConfigOption {
	return func(config *Config) {
		config.Scheme = scheme
	}
}

// WithHTTPServeOptions sets the http server options
func WithHTTPServeOptions(options HTTPServerOptions) ConfigOption {
	return func(config *Config) {
		config.ConnectServerHTTPOptions = options
	}
}

// WithCServerTlsConfig sets the https server config
func WithCServerTlsConfig(tlsConfig *tls.Config) ConfigOption {
	return func(config *Config) {
		config.ConnectServerTLSConfig = tlsConfig
	}
}

// WithHTTPClientOptions sets the http client options
func WithHTTPClientOptions(options HTTPClientOptions) ConfigOption {
	return func(config *Config) {
		config.ConnectClientHTTPOptions = options
	}
}

// WithClientTlsConfig sets the https client config
func WithClientTlsConfig(tlsConfig *tls.Config) ConfigOption {
	return func(config *Config) {
		config.ConnectClientTLSConfig = tlsConfig
	}
}

// WithCors sets the cors
func WithCors(cors *cors.Cors) ConfigOption {
	return func(config *Config) {
		config.ConnectCorsOptions = cors
	}
}

// WithKinds adds the kinds to the remote
func WithKinds(kinds ...*Kind) ConfigOption {
	return func(config *Config) {
		for _, k := range kinds {
			config.Kinds[k.Kind] = k.Props
		}
	}
}
