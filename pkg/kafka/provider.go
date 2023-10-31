package kafka

import (
	"sync"

	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/wire"
	"github.com/leoguilen/golang-kafka-pubsub/configs"
	"github.com/leoguilen/golang-kafka-pubsub/pkg/eventbus"
)

var (
	bus     *KafkaEventBus
	busOnce sync.Once

	ProviderSet wire.ProviderSet = wire.NewSet(
		ProvideKafkaConfig,
		ProvideKafkaEventBus,
		wire.Bind(new(eventbus.IEventBus), new(*KafkaEventBus)),
	)
)

func ProvideKafkaConfig(cfg *configs.Config) *ckafka.ConfigMap {
	return &ckafka.ConfigMap{
		"bootstrap.servers":  cfg.Kafka.BootstrapServers,
		"group.id":           cfg.Kafka.GroupID,
		"auto.offset.reset":  cfg.Kafka.AutoOffsetReset,
		"enable.auto.commit": cfg.Kafka.EnableAutoCommit,
	}
}

func ProvideKafkaEventBus(cm *ckafka.ConfigMap) *KafkaEventBus {
	busOnce.Do(func() {
		bus = NewKafkaEventBus(cm)
	})
	return bus
}
