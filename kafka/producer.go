package kafka

import (
	"github.com/IBM/sarama"
	"github.com/rs/zerolog/log"
	"go-Kafka/config"
	"time"
)

func StartProducer(cfg config.KafkaConfig) {
	kafkaConfig := sarama.NewConfig()
	kafkaConfig.Producer.Return.Successes = cfg.Producer.SuccessReturn

	producer, err := sarama.NewSyncProducer(cfg.Brokers, kafkaConfig)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to start Kafka producer")
	}
	defer func(producer sarama.SyncProducer) {
		err := producer.Close()
		if err != nil {
			log.Error().Err(err).Msg("Failed to close Kafka producer")
		}
	}(producer)

	for {
		msg := &sarama.ProducerMessage{
			Topic: cfg.Topic,
			Value: sarama.StringEncoder("Hello from Kafka producer!"),
		}
		partition, offset, err := producer.SendMessage(msg)
		if err != nil {
			log.Error().Err(err).Msg("Failed to send message")
		} else {
			log.Info().Msgf("Message is stored in topic(%s)/partition(%d)/offset(%d)",
				cfg.Topic, partition, offset)
		}
		time.Sleep(3 * time.Second)
	}
}
