//go:build wireinject
// +build wireinject

package kafka

import (
	"github.com/google/wire"
	"github.com/leoguilen/golang-kafka-pubsub/configs"
	"github.com/leoguilen/golang-kafka-pubsub/pkg/eventbus"
)

func Inject(cfg *configs.Config) (eventbus.IEventBus, error) {
	panic(wire.Build(ProviderSet))
}
