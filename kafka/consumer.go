package kafka

import (
	"github.com/IBM/sarama"
	"github.com/rs/zerolog/log"
)

func StartConsumer() {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	consumer, err := sarama.NewConsumer([]string{"localhost:9092"}, config)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to start Kafka consumer")
	}
	defer func(consumer sarama.Consumer) {
		err := consumer.Close()
		if err != nil {

		}
	}(consumer)

	partitions, err := consumer.Partitions("first-topic")
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to get partitions for topic")
	}

	for _, partition := range partitions {
		go func(partition int32) {
			partitionConsumer, err := consumer.ConsumePartition("first-topic",
				partition, sarama.OffsetNewest)
			if err != nil {
				log.Fatal().Err(err).Msgf("Failed to start partition consumer "+
					"for partition %d", partition)
			}
			defer func(partitionConsumer sarama.PartitionConsumer) {
				err := partitionConsumer.Close()
				if err != nil {

				}
			}(partitionConsumer)

			for msg := range partitionConsumer.Messages() {
				log.Log().Msg("Consumer:")
				log.Info().Msgf("Received message from partition %d: %s",
					partition, string(msg.Value))
			}
		}(partition)
	}

	select {}
}
