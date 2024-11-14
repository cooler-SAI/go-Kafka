package main

import (
	"github.com/IBM/sarama"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"os"
	"time"
)

func startConsumer() {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	consumer, err := sarama.NewConsumer([]string{"localhost:9092"}, config)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to start consumer")
	}
	defer func(consumer sarama.Consumer) {
		err := consumer.Close()
		if err != nil {

		}
	}(consumer)

	partitionConsumer, err := consumer.ConsumePartition("first-topic",
		0, sarama.OffsetNewest)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to start partition consumer")
	}
	defer func(partitionConsumer sarama.PartitionConsumer) {
		err := partitionConsumer.Close()
		if err != nil {

		}
	}(partitionConsumer)

	for msg := range partitionConsumer.Messages() {
		log.Info().Msgf("Message on topic: %s, partition: %d, offset: %d\n",
			msg.Topic, msg.Partition, msg.Offset)
	}

}

func main() {

	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	log.Info().Msg("Hello Kafka Go!")

	go startConsumer()

	for {
		time.Sleep(3 * time.Second)
		log.Info().Msg("Scanning.....")
	}
}
