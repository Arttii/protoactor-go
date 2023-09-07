package otel

import (
	"context"
	"sync"

	"github.com/asynkron/protoactor-go/actor"
)

var stoppingSpans = sync.Map{}

func getAndClearStoppingSpan(pid *actor.PID) context.Context {
	value, ok := stoppingSpans.Load(pid)
	if !ok {
		return nil
	}
	stoppingSpans.Delete(pid)
	return value.(context.Context)
}

func getStoppingSpan(pid *actor.PID) context.Context {
	value, ok := stoppingSpans.Load(pid)
	if !ok {
		return nil
	}
	return value.(context.Context)
}

func setStoppingSpan(pid *actor.PID, span context.Context) {
	stoppingSpans.Store(pid, span)
}
