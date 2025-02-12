package opentracing

import (
	"github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/log"
	"github.com/opentracing/opentracing-go"
)

func SenderMiddleware() actor.SenderMiddleware {
	return func(next actor.SenderFunc) actor.SenderFunc {
		return func(c actor.SenderContext, target *actor.PID, envelope *actor.MessageEnvelope) {
			span := getActiveSpan(c.Self())

			if span == nil {
				logger.Debug("OUTBOUND No active span", log.Stringer("PID", c.Self()), log.TypeOf("ActorType", c.Actor()), log.TypeOf("MessageType", envelope.Message))
				next(c, target, envelope)
				return
			}

			span.SetTag("span.kind", "client")

			span.SetTag("client", c.Self().String())
			span.SetTag("server", target.String())

			err := opentracing.GlobalTracer().Inject(span.Context(), opentracing.TextMap, opentracing.TextMapWriter(&messageEnvelopeWriter{MessageEnvelope: envelope}))
			if err != nil {
				logger.Debug("OUTBOUND Error injecting", log.Stringer("PID", c.Self()), log.TypeOf("ActorType", c.Actor()), log.TypeOf("MessageType", envelope.Message))
				next(c, target, envelope)
				return
			}

			logger.Debug("OUTBOUND Successfully injected", log.Stringer("PID", c.Self()), log.TypeOf("ActorType", c.Actor()), log.TypeOf("MessageType", envelope.Message))
			next(c, target, envelope)
		}
	}
}
