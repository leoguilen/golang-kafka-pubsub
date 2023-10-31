package events

import (
	"context"
	"fmt"

	"github.com/go-playground/validator/v10"
	"github.com/leoguilen/golang-kafka-pubsub/pkg/eventbus"
)

var _validator *validator.Validate = validator.New(validator.WithRequiredStructEnabled())

type GreetingReceivedModel struct {
	Message string `validate:"required"`
}

type GreetingReceivedEvent struct {
	eventbus.Event[GreetingReceivedModel]
}

func NewGreetingReceivedEvent(message string, metadata map[string]interface{}) *GreetingReceivedEvent {
	return &GreetingReceivedEvent{
		Event: eventbus.Event[GreetingReceivedModel]{
			Metadata: metadata,
			Data:     GreetingReceivedModel{Message: message},
		},
	}
}

type GreetingReceivedEventHandler struct{}

func NewGreetingReceivedHandler() *GreetingReceivedEventHandler {
	return &GreetingReceivedEventHandler{}
}

func (*GreetingReceivedEventHandler) Handle(ctx context.Context, evt *GreetingReceivedEvent) error {
	if err := _validator.Struct(evt.Data); err != nil {
		return fmt.Errorf("invalid event data: %w", err)
	}

	fmt.Printf("[GreetingReceivedEventHandler][Info] - Handling event: %+v\n", evt)
	return nil
}
