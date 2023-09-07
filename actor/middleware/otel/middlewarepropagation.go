package otel

import (
	"github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/actor/middleware/propagator"
	"go.opentelemetry.io/otel/trace"
)

func TracingMiddleware(t trace.Tracer) actor.SpawnMiddleware {
	return propagator.New().
		WithItselfForwarded().
		WithSpawnMiddleware(SpawnMiddleware(t)).
		WithSenderMiddleware(SenderMiddleware(t)).
		WithReceiverMiddleware(ReceiverMiddleware(t)).
		SpawnMiddleware
}
