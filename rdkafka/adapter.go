package rdkafka

import (
	instana "github.com/instana/go-sensor"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

const kafkaBrokerHeader = "x-instana-kafka-b"

func NewProducer(config *kafka.ConfigMap, sensor *instana.Sensor) (*Producer, error) {
	producer, err := kafka.NewProducer(config)
	if err != nil {
		return nil, err
	}
	return &Producer{
		Producer: producer,
		sensor:   sensor,
	}, nil
}

func NewConsumer(config *kafka.ConfigMap, sensor *instana.Sensor) (*Consumer, error) {
	consumer, err := kafka.NewConsumer(config)
	if err != nil {
		return nil, err
	}
	return &Consumer{
		Consumer: consumer,
		sensor:   sensor,
	}, nil
}

type kafkaHeaderAdapter struct {
	msg *kafka.Message
}

func (k *kafkaHeaderAdapter) ForeachKey(fn func(key, val string) error) error {
	for _, header := range k.msg.Headers {
		if err := fn(header.Key, string(header.Value)); err != nil {
			return err
		}
	}
	return nil
}

func (k *kafkaHeaderAdapter) Set(key, val string) {
	k.msg.Headers = append(k.msg.Headers, kafka.Header{
		Key:   key,
		Value: []byte(val),
	})
}
