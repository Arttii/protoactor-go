package otel

import (
	"context"
	"sync"

	"github.com/asynkron/protoactor-go/actor"
)

var parentSpans = sync.Map{}

func getAndClearParentSpan(pid *actor.PID) context.Context {
	value, ok := parentSpans.Load(pid)
	if !ok {
		return nil
	}

	parentSpans.Delete(pid)

	span, _ := value.(context.Context)

	return span
}

func setParentSpan(pid *actor.PID, span context.Context) {
	parentSpans.Store(pid, span)
}
