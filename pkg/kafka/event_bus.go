package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"

	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/leoguilen/golang-kafka-pubsub/pkg/eventbus"
)

type KafkaEventBus struct {
	ConfigMap  *ckafka.ConfigMap
	Serializer *KafkaSerializer
}

func NewKafkaEventBus(cm *ckafka.ConfigMap) *KafkaEventBus {
	return &KafkaEventBus{
		ConfigMap:  cm,
		Serializer: NewKafkaSerializer(),
	}
}

func (bus *KafkaEventBus) Publish(ctx context.Context, topic string, e eventbus.IEvent) error {
	p, err := ckafka.NewProducer(bus.ConfigMap)
	if err != nil {
		return fmt.Errorf("[KafkaEventBus][Error] - Failed to create producer: %s", err)
	}
	defer p.Close()

	var msg *ckafka.Message
	if msg, err = buildKafkaMessage(topic, e); err != nil {
		return err
	}

	deliveryChan := make(chan ckafka.Event)
	if err = p.Produce(msg, deliveryChan); err != nil {
		return fmt.Errorf("[KafkaEventBus][Error] - Failed to produce message: %s", err)
	}

	if err = checkMessageDeliveryStatus(deliveryChan); err != nil {
		return err
	}

	return nil
}

func (bus *KafkaEventBus) Subscribe(ctx context.Context, topic string, types ...reflect.Type) error {
	if len(types) != 2 {
		return fmt.Errorf("[KafkaEventBus][Error] - Invalid number of types: %d", len(types))
	}

	eventType := types[0]
	handlerType := types[1]
	if !strings.Contains(handlerType.String(), eventType.String()) {
		return fmt.Errorf("[KafkaEventBus][Error] - Invalid event type %s for handler %s", eventType.String(), handlerType.String())
	}

	c, err := ckafka.NewConsumer(bus.ConfigMap)
	if err != nil {
		fmt.Printf("[KafkaEventBus][Error] - Failed to create consumer: %s\n", err)
		return err
	}
	defer c.Close()

	err = c.Subscribe(topic, nil)
	if err != nil {
		fmt.Printf("[KafkaEventBus][Error] - Failed to subscribe to topic: %s\n", err)
		return err
	}

	fmt.Printf("[KafkaEventBus][Info] - Subscribed to topic %s and waiting for messages...\n", topic)

	for {
		ev := c.Poll(100)
		if ev == nil {
			continue
		}

		switch e := ev.(type) {
		case *ckafka.Message:
			defer func() {
				if err := recover(); err != nil {
					fmt.Printf("[KafkaEventBus][Error] - Failed to handle event: %s\n", err)
				}
			}()

			fmt.Printf("[KafkaEventBus][Debug] - Received message: %v\n", string(e.Value))

			val, err := bus.Serializer.Deserialize(e, eventType)
			if err != nil {
				fmt.Printf("[KafkaEventBus][Error] - Failed to deserialize message: %s\n", err)
				continue
			}

			event := reflect.New(eventType).Interface()
			reflect.ValueOf(event).Elem().Set(reflect.ValueOf(val))

			handler := reflect.New(handlerType).Interface()
			res := reflect.ValueOf(handler).MethodByName("Handle").Call([]reflect.Value{reflect.ValueOf(ctx), reflect.ValueOf(event)})
			if len(res) > 0 && !res[0].IsNil() {
				fmt.Printf("[KafkaEventBus][Error] - Failed to handle event: %s\n", res[0].Interface().(error))
				continue
			}

			tp, err := c.CommitMessage(e)
			if err != nil {
				fmt.Printf("[KafkaEventBus][Error] - Failed to commit message: %s\n", err)
				continue
			}

			fmt.Printf("[KafkaEventBus][Info] - Message committed: %v\n", tp)
		case ckafka.Error:
			if e.IsFatal() {
				panic(fmt.Errorf("[KafkaEventBus][Error] - All brokers are down: %v", e))
			}
			return fmt.Errorf("[KafkaEventBus][Error] - Received error: %v", e)
		default:
			fmt.Printf("[KafkaEventBus][Info] - Ignoring event: %v\n", e)
		}
	}
}

func buildKafkaMessage(topic string, e eventbus.IEvent) (*ckafka.Message, error) {
	value, err := json.Marshal(e.GetData())
	if err != nil {
		return nil, fmt.Errorf("[KafkaEventBus][Error] - Failed to serialize event data: %s", err)
	}

	metadata := e.GetMetadata()

	headers := make([]ckafka.Header, 0, len(metadata))
	for k, v := range metadata {
		headers = append(headers, ckafka.Header{
			Key:   k,
			Value: []byte(fmt.Sprintf("%v", v)),
		})
	}

	return &ckafka.Message{
		TopicPartition: ckafka.TopicPartition{Topic: &topic, Partition: ckafka.PartitionAny},
		Headers:        headers,
		Key:            []byte(e.Type()),
		Value:          value,
		Timestamp:      time.Now().UTC(),
	}, nil
}

func checkMessageDeliveryStatus(deliveryChan chan ckafka.Event) error {
	for e := range deliveryChan {
		switch ev := e.(type) {
		case *ckafka.Message:
			if ev.TopicPartition.Error != nil {
				return fmt.Errorf("[KafkaEventBus][Error] - Delivery failed: %v", ev.TopicPartition.Error)
			} else {
				fmt.Printf("[KafkaEventBus][Info] - Delivered message to topic %s [%d] at offset %v\n",
					*ev.TopicPartition.Topic, ev.TopicPartition.Partition, ev.TopicPartition.Offset)
				return nil
			}
		}
	}
	defer close(deliveryChan)
	return nil
}
