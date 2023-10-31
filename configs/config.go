package configs

import (
	"errors"
	"os"

	"github.com/joho/godotenv"
)

var (
	_requiredEnvVariables = []string{
		"ENV",
		"PORT",
		"KAFKA_BOOTSTRAP_SERVERS",
		"KAFKA_GROUP_ID",
	}
)

type Config struct {
	Env   string
	Port  string
	Kafka struct {
		BootstrapServers string
		AutoOffsetReset  string
		GroupID          string
		EnableAutoCommit bool
	}
}

func New() (*Config, error) {
	if err := godotenv.Load(); err != nil && os.Getenv("ENV") == "" {
		return nil, err
	}

	for _, key := range _requiredEnvVariables {
		if env := os.Getenv(key); env == "" {
			return nil, errors.New("missing required environment variable: " + key)
		}
	}

	return &Config{
		Env:  os.Getenv("ENV"),
		Port: os.Getenv("PORT"),
		Kafka: struct {
			BootstrapServers string
			AutoOffsetReset  string
			GroupID          string
			EnableAutoCommit bool
		}{
			BootstrapServers: os.Getenv("KAFKA_BOOTSTRAP_SERVERS"),
			AutoOffsetReset:  os.Getenv("KAFKA_AUTO_OFFSET_RESET"),
			GroupID:          os.Getenv("KAFKA_GROUP_ID"),
			EnableAutoCommit: os.Getenv("KAFKA_ENABLE_AUTO_COMMIT") == "true",
		},
	}, nil
}
