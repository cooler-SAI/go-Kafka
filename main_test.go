package main

import (
	"github.com/IBM/sarama"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func logInit() {
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	log.Info().Msg("Init zerolog...")
}

func TestStartProducer(t *testing.T) {
	logInit()

	log.Info().Msg("Starting Test Kafka Producer application...")
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true

	config.Producer.Return.Errors = true

	log.Info().Msg("Init topic and start sync...")
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

	log.Info().Msg("Test Producer Kafka Successfully Initialized")

}
