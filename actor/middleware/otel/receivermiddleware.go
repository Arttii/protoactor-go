package otel

import (
	"context"
	"fmt"
	"strings"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/log"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// var tracers = sync.Map{}

func ReceiverMiddleware(tracer trace.Tracer) actor.ReceiverMiddleware {

	return func(next actor.ReceiverFunc) actor.ReceiverFunc {
		return func(c actor.ReceiverContext, envelope *actor.MessageEnvelope) {

			// var tracer trace.Tracer
			// tracerI, ok := tracers.Load(c.Self().Id)
			// if !ok {
			// 	tracer = tp.Tracer(c.Self().Id)
			// 	tracers.Store(c.Self().Id, tracer)
			// } else {
			// 	tracer = tracerI.(trace.Tracer)
			// }

			propagator := otel.GetTextMapPropagator()
			spanContext := context.Background()
			propagator.Extract(spanContext, envelope.Header)

			//opentracing.GlobalTracer().Extract(opentracing.TextMap, opentracing.TextMapReader(&messageHeaderReader{ReadOnlyMessageHeader: envelope.Header}))
			if spanContext == context.Background() {
				logger.Debug("INBOUND No spanContext found", log.Stringer("PID", c.Self()))
				// next(c)
			}
			// else if err != nil {
			// 	logger.Debug("INBOUND Error", log.Stringer("PID", c.Self()), log.Error(err))
			// 	next(c, envelope)
			// 	return
			// }
			var span trace.Span
			var newSpanContext context.Context
			switch envelope.Message.(type) {
			case *actor.Started:
				parentContext := getAndClearParentSpan(c.Self())
				if parentContext != nil {

					msg := fmt.Sprintf("INBOUND-%T/%T", c.Actor(), envelope.Message)
					newSpanContext, span = tracer.Start(parentContext, strings.ReplaceAll(msg, "*", ""), trace.WithSpanKind(trace.SpanKindServer))

					logger.Debug("INBOUND Found parent span", log.Stringer("PID", c.Self()), log.TypeOf("ActorType", c.Actor()), log.TypeOf("MessageType", envelope.Message))
				} else {
					logger.Debug("INBOUND No parent span", log.Stringer("PID", c.Self()), log.TypeOf("ActorType", c.Actor()), log.TypeOf("MessageType", envelope.Message))
				}
			case *actor.Stopping:
				var parentContext context.Context
				if c.Parent() != nil {
					parentContext = getStoppingSpan(c.Parent())
				}
				if parentContext != nil {

					newSpanContext, span = tracer.Start(parentContext, fmt.Sprintf("INBOUND-%T/stopping", c.Actor()), trace.WithSpanKind(trace.SpanKindServer))
				} else {

					newSpanContext, span = tracer.Start(parentContext, fmt.Sprintf("INBOUND-%T/stopping", c.Actor()), trace.WithSpanKind(trace.SpanKindServer))
				}
				setStoppingSpan(c.Self(), newSpanContext)
				span.SetAttributes(attribute.String("ActorPID", c.Self().String()),
					attribute.String("ActorType", fmt.Sprintf("INBOUND-%T", c.Actor())),
					attribute.String("MessageType", fmt.Sprintf("INBOUND-%T", envelope.Message)))

				_, stoppingHandlingSpan := tracer.Start(newSpanContext, "stopping-handling")
				next(c, envelope)
				stoppingHandlingSpan.End()
				return
			case *actor.Stopped:
				newSpanContext = getAndClearStoppingSpan(c.Self())
				next(c, envelope)
				if newSpanContext != nil {
					trace.SpanFromContext(newSpanContext).End()
				}
				return
			}
			if newSpanContext == nil && spanContext == context.Background() {
				logger.Debug("INBOUND No spanContext. Starting new span", log.Stringer("PID", c.Self()), log.TypeOf("ActorType", c.Actor()), log.TypeOf("MessageType", envelope.Message))

				msg := fmt.Sprintf("INBOUND-%T/%T", c.Actor(), envelope.Message)
				newSpanContext, span = tracer.Start(context.Background(), strings.ReplaceAll(msg, "*", ""), trace.WithSpanKind(trace.SpanKindServer))

			}
			if newSpanContext == nil {
				logger.Debug("INBOUND Starting span from parent", log.Stringer("PID", c.Self()), log.TypeOf("ActorType", c.Actor()), log.TypeOf("MessageType", envelope.Message))

				msg := fmt.Sprintf("INBOUND-%T/%T", c.Actor(), envelope.Message)
				newSpanContext, span = tracer.Start(spanContext, strings.ReplaceAll(msg, "*", ""), trace.WithSpanKind(trace.SpanKindServer))
			}

			setActiveSpan("INBOUND-"+c.Self().String(), newSpanContext)
			span.SetAttributes(attribute.String("ActorPID", c.Self().String()),
				attribute.String("ActorType", fmt.Sprintf("INBOUND-%T", c.Actor())),
				attribute.String("MessageType", fmt.Sprintf("INBOUND-%T", envelope.Message)))

			span.SetAttributes(attribute.String("span.kind", "server"),
				attribute.String("server", c.Self().String()),
				attribute.String("client", envelope.Sender.String()),
				attribute.String("peer.service", c.Self().String()),
			)

			defer func() {
				logger.Debug("INBOUND Finishing span", log.Stringer("PID", c.Self()), log.TypeOf("ActorType", c.Actor()), log.TypeOf("MessageType", envelope.Message))
				span.End()
				clearActiveSpan("INBOUND-" + c.Self().String())
			}()

			next(c, envelope)
		}
	}
}
