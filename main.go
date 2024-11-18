package main

import (
	"github.com/IBM/sarama"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func startProducer() {
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

func startConsumer() {
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

func main() {
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	log.Info().Msg("Starting Kafka Go application...")

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	go startConsumer()
	go startProducer()

	<-stop

	log.Error().Msg("Shutting down Kafka Go application...")
	log.Error().Msg("Shutting down Kafka Producer...")
	log.Error().Msg("Shutting down Kafka Consumer...")

}
