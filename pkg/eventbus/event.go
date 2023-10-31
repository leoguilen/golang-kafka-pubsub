package eventbus

import (
	"context"
	"reflect"
)

type IEvent interface {
	GetMetadata() map[string]interface{}
	GetData() interface{}
	Type() string
}

type Event[TData interface{}] struct {
	Metadata map[string]interface{}
	Data     TData
}

func (e *Event[TData]) GetData() interface{} {
	return e.Data
}

func (e *Event[TData]) GetMetadata() map[string]interface{} {
	return e.Metadata
}

func (e *Event[TData]) Type() string {
	return reflect.TypeOf(e).String()
}

type IEventHandler[TEvent IEvent] interface {
	Handle(context.Context, TEvent) error
}

type IEventBus interface {
	Publish(context.Context, string, IEvent) error
	Subscribe(context.Context, string, ...reflect.Type) error
}
