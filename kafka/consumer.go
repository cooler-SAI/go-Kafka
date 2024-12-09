package kafka

import (
	"github.com/IBM/sarama"
	"github.com/rs/zerolog/log"
	"go-Kafka/config"
)

func StartConsumer(cfg config.KafkaConfig) {
	kafkaConfig := sarama.NewConfig()
	kafkaConfig.Consumer.Return.Errors = cfg.Consumer.ReturnErrors

	consumer, err := sarama.NewConsumer(cfg.Brokers, kafkaConfig)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to start Kafka consumer")
	}
	defer func(consumer sarama.Consumer) {
		err := consumer.Close()
		if err != nil {
			log.Error().Err(err).Msg("Failed to close Kafka consumer")
		}
	}(consumer)

	partitions, err := consumer.Partitions(cfg.Topic)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to get partitions for topic")
	}

	for _, partition := range partitions {
		go func(partition int32) {
			partitionConsumer, err := consumer.ConsumePartition(cfg.Topic,
				partition, sarama.OffsetNewest)
			if err != nil {
				log.Fatal().Err(err).Msgf("Failed to start partition consumer for partition %d", partition)
			}
			defer func(partitionConsumer sarama.PartitionConsumer) {
				err := partitionConsumer.Close()
				if err != nil {
					log.Error().Err(err).Msg("Failed to close partition consumer")
				}
			}(partitionConsumer)

			for msg := range partitionConsumer.Messages() {
				log.Info().Msgf("Received message from partition %d: %s",
					partition, string(msg.Value))
			}
		}(partition)
	}

	select {}
}
