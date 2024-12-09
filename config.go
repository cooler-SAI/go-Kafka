package main

import "github.com/spf13/viper"

type KafkaConfig struct {
	Brokers  []string `mapstructure:"brokers"`
	Topic    string   `mapstructure:"topic"`
	Producer struct {
		SuccessReturn bool `mapstructure:"success_return"`
	} `mapstructure:"producer"`
	Consumer struct {
		ReturnErrors bool `mapstructure:"return_errors"`
	} `mapstructure:"consumer"`
}

type Config struct {
	Kafka KafkaConfig `mapstructure:"kafka"`
}

var AppConfig Config

func LoadConfig() error {
	viper.SetConfigName("config")
	viper.SetConfigType("yaml")
	viper.AddConfigPath(".")

	if err := viper.ReadInConfig(); err != nil {
		return err
	}

	if err := viper.Unmarshal(&AppConfig); err != nil {
		return err
	}

	return nil
}
