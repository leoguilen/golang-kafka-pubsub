package main

import (
	"context"
	"fmt"
	"reflect"

	"github.com/leoguilen/golang-kafka-pubsub/configs"
	"github.com/leoguilen/golang-kafka-pubsub/internal/events"
	"github.com/leoguilen/golang-kafka-pubsub/pkg/kafka"
)

var _config *configs.Config

func init() {
	var err error
	if _config, err = configs.New(); err != nil {
		panic(fmt.Errorf("[INIT] - failed to load config: %w", err))
	}
}

func main() {
	bus, err := kafka.Inject(_config)
	if err != nil {
		panic(fmt.Errorf("[MAIN] - failed to inject kafka event bus: %w", err))
	}

	panic(bus.Subscribe(context.Background(), "greetings", reflect.TypeOf(events.GreetingReceivedEvent{}), reflect.TypeOf(events.GreetingReceivedEventHandler{})))
}
