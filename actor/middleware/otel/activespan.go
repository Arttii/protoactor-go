package otel

import (
	"context"
	"fmt"
	"sync"

	"github.com/asynkron/protoactor-go/actor"
	"go.opentelemetry.io/otel/trace"
)

// const name = "protoactor-go"

// var tracer = otel.Tracer(name)
var activeSpan = sync.Map{}

func getActiveSpan(pid string) context.Context {
	value, ok := activeSpan.Load(pid)
	if !ok {
		return nil
	}

	span, _ := value.(context.Context)

	return span
}

func clearActiveSpan(pid string) {
	activeSpan.Delete(pid)
}

func setActiveSpan(pid string, span context.Context) {
	activeSpan.Store(pid, span)
}

func GetActiveSpan(ctx actor.Context, tracer trace.Tracer) context.Context {
	spanContext := getActiveSpan(ctx.Self().String())
	if spanContext == nil {
		spanContext, _ = tracer.Start(context.Background(), fmt.Sprintf("%T/%T", ctx.Actor(), ctx.Message()))
		// TODO: Fix finding the real span always or handle no-span better on receiving side
		// span = trace.SpanFromContext(context.
		//span.SetName()

	}

	return spanContext
}
