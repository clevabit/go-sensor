package rdkafka

import (
	"fmt"
	instana "github.com/instana/go-sensor"
	ot "github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

type Producer struct {
	*kafka.Producer
	sensor   *instana.Sensor
}

func (p *Producer) Produce(parentSpan ot.Span, msg *kafka.Message, deliveryChan chan kafka.Event) error {
	span := p.sensor.NewChildSpan("kafka", parentSpan)
	defer span.Finish()

	span.SetTag(string(ext.SpanKind), string(ext.SpanKindProducerEnum))
	span.SetTag(string(ext.MessageBusDestination), msg.TopicPartition.Topic)
	span.SetTag("kafka.service", msg.TopicPartition.Topic)
	span.SetTag("kafka.access", "send")

	p.sensor.WithTracer(func(tracer ot.Tracer) {
		if err := tracer.Inject(span.Context(), ot.TextMap, &kafkaHeaderAdapter{msg: msg}); err != nil {
			fmt.Printf("Instana failed to inject headers into Kafka message, may result in disconnected traces")
		}
	})

	return p.Producer.Produce(msg, deliveryChan)
}
