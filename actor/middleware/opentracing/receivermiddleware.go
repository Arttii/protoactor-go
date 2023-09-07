package opentracing

import (
	"fmt"
	"strings"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/log"
	"github.com/opentracing/opentracing-go"
)

func ReceiverMiddleware() actor.ReceiverMiddleware {
	return func(next actor.ReceiverFunc) actor.ReceiverFunc {
		return func(c actor.ReceiverContext, envelope *actor.MessageEnvelope) {
			spanContext, err := opentracing.GlobalTracer().Extract(opentracing.TextMap, opentracing.TextMapReader(&messageHeaderReader{ReadOnlyMessageHeader: envelope.Header}))
			if err == opentracing.ErrSpanContextNotFound {
				logger.Debug("INBOUND No spanContext found", log.Stringer("PID", c.Self()), log.Error(err))
				// next(c)
			} else if err != nil {
				logger.Debug("INBOUND Error", log.Stringer("PID", c.Self()), log.Error(err))
				next(c, envelope)
				return
			}
			var span opentracing.Span
			switch envelope.Message.(type) {
			case *actor.Started:
				parentSpan := getAndClearParentSpan(c.Self())
				if parentSpan != nil {
					name := strings.ReplaceAll(fmt.Sprintf("%T/%T", c.Actor(), envelope.Message), "*", "")
					span = opentracing.StartSpan(name, opentracing.ChildOf(parentSpan.Context()))
					logger.Debug("INBOUND Found parent span", log.Stringer("PID", c.Self()), log.TypeOf("ActorType", c.Actor()), log.TypeOf("MessageType", envelope.Message))
				} else {
					logger.Debug("INBOUND No parent span", log.Stringer("PID", c.Self()), log.TypeOf("ActorType", c.Actor()), log.TypeOf("MessageType", envelope.Message))
				}
			case *actor.Stopping:
				var parentSpan opentracing.Span
				if c.Parent() != nil {
					parentSpan = getStoppingSpan(c.Parent())
				}
				if parentSpan != nil {
					name := strings.ReplaceAll(fmt.Sprintf("%T/stopping", c.Actor()), "*", "")
					span = opentracing.StartSpan(name, opentracing.ChildOf(parentSpan.Context()))
				} else {
					name := strings.ReplaceAll(fmt.Sprintf("%T/stopping", c.Actor()), "*", "")
					span = opentracing.StartSpan(name)
				}
				setStoppingSpan(c.Self(), span)
				span.SetTag("ActorPID", c.Self())
				span.SetTag("ActorType", fmt.Sprintf("%T", c.Actor()))
				span.SetTag("MessageType", fmt.Sprintf("%T", envelope.Message))
				stoppingHandlingSpan := opentracing.StartSpan("stopping-handling", opentracing.ChildOf(span.Context()))
				next(c, envelope)
				stoppingHandlingSpan.Finish()
				return
			case *actor.Stopped:
				span = getAndClearStoppingSpan(c.Self())
				next(c, envelope)
				if span != nil {
					span.Finish()
				}
				return
			}
			if span == nil && spanContext == nil {
				logger.Debug("INBOUND No spanContext. Starting new span", log.Stringer("PID", c.Self()), log.TypeOf("ActorType", c.Actor()), log.TypeOf("MessageType", envelope.Message))
				name := strings.ReplaceAll(fmt.Sprintf("%T/%T", c.Actor(), envelope.Message), "*", "")

				span = opentracing.StartSpan(name)
			}
			if span == nil {
				logger.Debug("INBOUND Starting span from parent", log.Stringer("PID", c.Self()), log.TypeOf("ActorType", c.Actor()), log.TypeOf("MessageType", envelope.Message))
				name := strings.ReplaceAll(fmt.Sprintf("%T/%T", c.Actor(), envelope.Message), "*", "")
				span = opentracing.StartSpan(name, opentracing.ChildOf(spanContext))
			}

			setActiveSpan(c.Self(), span)
			span.SetTag("ActorPID", c.Self())
			span.SetTag("ActorType", fmt.Sprintf("%T", c.Actor()))
			span.SetTag("MessageType", fmt.Sprintf("%T", envelope.Message))

			span.SetTag("span.kind", "server")

			span.SetTag("server", c.Self().String())
			span.SetTag("client", envelope.Sender.String())

			defer func() {
				logger.Debug("INBOUND Finishing span", log.Stringer("PID", c.Self()), log.TypeOf("ActorType", c.Actor()), log.TypeOf("MessageType", envelope.Message))
				span.Finish()
				clearActiveSpan(c.Self())
			}()

			next(c, envelope)
		}
	}
}
