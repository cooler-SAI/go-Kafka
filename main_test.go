package main

import (
	"github.com/IBM/sarama"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
	"time"
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

	log.Info().Msg("Initializing topic and starting sync producer...")
	producer, err := sarama.NewSyncProducer([]string{"127.0.0.1:9092"}, config)
	assert.NoError(t, err, "Failed to create Kafka Producer")
	defer func(producer sarama.SyncProducer) {
		err := producer.Close()
		if err != nil {
			log.Error().Err(err).Msg("Error closing Kafka producer")
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

func TestStartConsumer(t *testing.T) {
	logInit()

	log.Info().Msg("Starting Test Kafka Consumer application...")
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	consumer, err := sarama.NewConsumer([]string{"localhost:9092"}, config)
	assert.NoError(t, err, "Failed to create Kafka Consumer")
	defer func(consumer sarama.Consumer) {
		err := consumer.Close()
		if err != nil {
			log.Error().Err(err).Msg("Error closing Kafka consumer")
		}
	}(consumer)

	log.Info().Msg("Initializing partition consumers for topic 'first-topic'...")
	partitions, err := consumer.Partitions("first-topic")
	assert.NoError(t, err, "Failed to retrieve partitions for topic")

	messageSent := "Test message"
	done := make(chan bool)

	for _, partition := range partitions {
		go func(partition int32) {
			partitionConsumer, err := consumer.ConsumePartition("first-topic",
				partition, sarama.OffsetNewest)
			assert.NoError(t, err, "Failed to start partition consumer")
			defer func(partitionConsumer sarama.PartitionConsumer) {
				err := partitionConsumer.Close()
				if err != nil {
					log.Error().Err(err).Msg("Error closing Kafka partition consumer")
				}
			}(partitionConsumer)

			for msg := range partitionConsumer.Messages() {
				log.Info().Msgf("Message received: topic=%s, partition=%d, offset=%d, value=%s",
					msg.Topic, msg.Partition, msg.Offset, string(msg.Value))
				assert.Equal(t, messageSent, string(msg.Value), "Received message "+
					"does not match expected value")
				done <- true
				return
			}
		}(partition)
	}

	go func() {
		log.Info().Msg("Starting producer for message sending during consumer test...")
		producerConfig := sarama.NewConfig()
		producerConfig.Producer.Return.Successes = true

		producer, err := sarama.NewSyncProducer([]string{"localhost:9092"}, producerConfig)
		if err != nil {
			log.Error().Err(err).Msg("Failed to create Kafka producer")
			return
		}
		defer func(producer sarama.SyncProducer) {
			err := producer.Close()
			if err != nil {

			}
		}(producer)

		partition, offset, err := producer.SendMessage(&sarama.ProducerMessage{
			Topic: "first-topic",
			Value: sarama.StringEncoder(messageSent),
		})
		if err != nil {
			log.Error().Err(err).Msg("Failed to send message")
			return
		}
		log.Info().Msgf("Message sent successfully: topic=first-topic,"+
			" partition=%d, offset=%d", partition, offset)
	}()

	select {
	case <-done:
		log.Info().Msg("Test Consumer Kafka Successfully Completed")
	case <-time.After(10 * time.Second):
		t.Fatal("Test timed out waiting for Kafka message")
	}
}
