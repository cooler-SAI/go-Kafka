package main

import (
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"go-Kafka/kafka"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})

	log.Info().Msg("Loading configuration...")
	if err := LoadConfig(); err != nil {
		log.Fatal().Err(err).Msg("Failed to load configuration")
	}

	log.Info().Msg("Starting Kafka Go application...")

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	go kafka.StartProducer()
	go kafka.StartConsumer()

	<-stop

	log.Error().Msg("Shutting down Kafka Go application...")
	log.Error().Msg("Shutting down Kafka Producer...")
	log.Error().Msg("Shutting down Kafka Consumer...")

}
