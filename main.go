package main

import (
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"os"
	"time"
)

func main() {

	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})

	log.Info().Msg("Hello Kafka Go!")

	for {
		time.Sleep(3 * time.Second)
		log.Info().Msg("Scanning.....")
	}
}
