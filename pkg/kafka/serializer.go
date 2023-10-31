package kafka

import (
	"encoding/json"
	"errors"
	"reflect"
	"strings"

	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type KafkaSerializer struct{}

func NewKafkaSerializer() *KafkaSerializer {
	return &KafkaSerializer{}
}

func (*KafkaSerializer) Deserialize(msg *ckafka.Message, eventType reflect.Type) (interface{}, error) {
	var data interface{}
	if err := json.Unmarshal(msg.Value, &data); err != nil {
		return nil, err
	}

	headers := make(map[string]interface{})
	headers["timestamp"] = msg.Timestamp.Unix()
	for _, h := range msg.Headers {
		headers[h.Key] = string(h.Value)
	}

	event := reflect.ValueOf(reflect.New(eventType).Interface()).Elem()

	_, hasMetadataField := event.Type().FieldByName("Metadata")
	_, hasDataField := event.Type().FieldByName("Data")
	if !hasMetadataField && !hasDataField {
		return nil, errors.New("invalid event type")
	}

	event.FieldByName("Metadata").Set(reflect.ValueOf(headers))
	for k, v := range data.(map[string]interface{}) {
		event.FieldByName("Data").FieldByNameFunc(func(s string) bool {
			return strings.EqualFold(s, k)
		}).Set(reflect.ValueOf(v))
	}

	return event.Interface(), nil
}

func (*KafkaSerializer) Serialize(value interface{}) ([]byte, error) {
	return json.Marshal(value)
}
