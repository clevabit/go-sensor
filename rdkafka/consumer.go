package rdkafka

import (
	instana "github.com/instana/go-sensor"
	ot "github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	"github.com/pkg/errors"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

type Consumer struct {
	*kafka.Consumer
	sensor *instana.Sensor
}

func (c *Consumer) NewFollowFromSpan(operationName string, msg *kafka.Message) (ot.Span, error) {
	ret := make(chan interface{}, 1)
	defer close(ret)
	c.sensor.WithTracer(func(tracer ot.Tracer) {
		spanContext, err := tracer.Extract(ot.TextMap, &kafkaHeaderAdapter{msg: msg})
		if err != nil {
			ret <- errors.Errorf("error while reading span context: %v\n", err)
		} else {
			span := c.sensor.NewSpan(operationName, ot.FollowsFrom(spanContext))
			span.SetTag(string(ext.SpanKind), string(ext.SpanKindConsumerEnum))
			span.SetTag("kafka.service", msg.TopicPartition.Topic)
			span.SetTag("kafka.access", "consume")

			ret <- span
		}
	})
	v := <-ret
	if err, ok := v.(error); ok {
		return nil, err
	}
	return v.(ot.Span), nil
}
