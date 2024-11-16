package main

import (
	"github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestStartProducer(t *testing.T) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true

	config.Producer.Return.Errors = true

	producer, err := sarama.NewSyncProducer([]string{"127.0.0.1:9092"}, config)
	assert.NoError(t, err, "Failed to create Kafka Producer")
	defer func(producer sarama.SyncProducer) {
		err := producer.Close()
		if err != nil {

		}
	}(producer)

	msg := &sarama.ProducerMessage{
		Topic: "test-topic",
		Value: sarama.StringEncoder("Test message"),
	}
	partition, offset, err := producer.SendMessage(msg)

	assert.NoError(t, err, "Failed to send message")
	assert.GreaterOrEqual(t, partition, int32(0), "Invalid partition")
	assert.GreaterOrEqual(t, offset, int64(0), "Invalid offset")

}
