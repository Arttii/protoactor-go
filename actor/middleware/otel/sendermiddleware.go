package otel

import (
	"fmt"
	"strings"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/log"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

func SenderMiddleware(tracer trace.Tracer) actor.SenderMiddleware {
	return func(next actor.SenderFunc) actor.SenderFunc {
		return func(c actor.SenderContext, target *actor.PID, envelope *actor.MessageEnvelope) {
			spanContext := getActiveSpan("OUTBOUND-" + c.Self().String())

			if spanContext == nil {
				logger.Debug("OUTBOUND No active span", log.Stringer("PID", c.Self()), log.TypeOf("ActorType", c.Actor()), log.TypeOf("MessageType", envelope.Message))

				msg := fmt.Sprintf("OUTBOUND-%T/%T", c.Actor(), envelope.Message)
				newSpanContext, span := tracer.Start(spanContext, strings.ReplaceAll(msg, "*", ""), trace.WithSpanKind(trace.SpanKindClient))
				setActiveSpan("OUTBOUND-"+c.Self().String(), newSpanContext)
				span.SetAttributes(attribute.String("span.kind", "client"),
					attribute.String("client", c.Self().String()),
					attribute.String("server", target.String()),
					attribute.String("peer.service", c.Self().String()),
				)
				defer func() {
					logger.Debug("OUTBOUND Finishing span", log.Stringer("PID", c.Self()), log.TypeOf("ActorType", c.Actor()), log.TypeOf("MessageType", envelope.Message))
					span.End()
					clearActiveSpan("OUTBOUND-" + c.Self().String())
				}()

				next(c, target, envelope)
				return
			}
			span := trace.SpanFromContext(spanContext)

			span.SetAttributes(attribute.String("span.kind", "client"),
				attribute.String("client", c.Self().String()),
				attribute.String("server", target.String()),
				attribute.String("peer.service", c.Self().String()),
			)

			//span := trace.SpanFromContext(spanContext)

			propagator := otel.GetTextMapPropagator()
			propagator.Inject(spanContext, envelope.Header)
			// _, span = otel.Tracer(name).Start(context.Background(),"yolo",trace.WithAttributes(
			// 	attribute.KeyValue()
			// ))

			// err := opentracing.GlobalTracer().Inject(span.SpanContext(), opentracing.TextMap, opentracing.TextMapWriter(&messageEnvelopeWriter{MessageEnvelope: envelope}))
			// if err != nil {
			// 	logger.Debug("OUTBOUND Error injecting", log.Stringer("PID", c.Self()), log.TypeOf("ActorType", c.Actor()), log.TypeOf("MessageType", envelope.Message))
			// 	next(c, target, envelope)
			// 	return
			// }

			logger.Debug("OUTBOUND Successfully injected", log.Stringer("PID", c.Self()), log.TypeOf("ActorType", c.Actor()), log.TypeOf("MessageType", envelope.Message))

			defer func() {
				logger.Debug("OUTBOUND Finishing span", log.Stringer("PID", c.Self()), log.TypeOf("ActorType", c.Actor()), log.TypeOf("MessageType", envelope.Message))
				span.End()
				clearActiveSpan("OUTBOUND-" + c.Self().String())
			}()

			next(c, target, envelope)
		}
	}
}
