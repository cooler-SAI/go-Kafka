package kafka

import (
	"github.com/IBM/sarama"
	"github.com/rs/zerolog/log"
	"time"
)

func StartProducer() {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer([]string{"localhost:9092"}, config)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to start Kafka producer")
	}
	defer func(producer sarama.SyncProducer) {
		err := producer.Close()
		if err != nil {

		}
	}(producer)

	for {
		msg := &sarama.ProducerMessage{
			Topic: "first-topic",
			Value: sarama.StringEncoder("Hello from Kafka producer!"),
		}
		partition, offset, err := producer.SendMessage(msg)
		if err != nil {
			log.Error().Err(err).Msg("Failed to send message")
		} else {
			log.Log().Msg("Producer:")
			log.Info().Msgf("Message is stored in topic(%s)/partition(%d)/offset(%d)",
				"first-topic", partition, offset)
		}
		time.Sleep(3 * time.Second)
	}
}
