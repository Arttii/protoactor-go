package otel

import (
	"github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/log"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

func SpawnMiddleware(tracer trace.Tracer) actor.SpawnMiddleware {
	return func(next actor.SpawnFunc) actor.SpawnFunc {
		return func(actorSystem *actor.ActorSystem, id string, props *actor.Props, parentContext actor.SpawnerContext) (pid *actor.PID, e error) {
			self := parentContext.Self()
			pid, err := next(actorSystem, id, props, parentContext)
			if err != nil {
				logger.Debug("SPAWN got error trying to spawn", log.Stringer("PID", self), log.TypeOf("ActorType", parentContext.Actor()), log.Error(err))
				return pid, err
			}
			if self != nil {
				spanContext := getActiveSpan(self.String())
				if spanContext != nil {
					setParentSpan(pid, spanContext)
					span := trace.SpanFromContext(spanContext)
					span.AddEvent("spawned", trace.WithAttributes(
						attribute.String("SpawnPID", pid.String()),
					))

					logger.Debug("SPAWN found active span", log.Stringer("PID", self), log.TypeOf("ActorType", parentContext.Actor()), log.Stringer("SpawnedPID", pid))
				} else {
					logger.Debug("SPAWN no active span on parent", log.Stringer("PID", self), log.TypeOf("ActorType", parentContext.Actor()), log.Stringer("SpawnedPID", pid))
				}
			} else {
				logger.Debug("SPAWN no parent pid", log.Stringer("SpawnedPID", pid))
			}
			return pid, err
		}
	}
}
