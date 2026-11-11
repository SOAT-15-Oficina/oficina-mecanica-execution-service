package config

import (
	"strings"
	"time"

	"github.com/spf13/viper"
)

type ServerConfig struct {
	Environment string
	Port        string
	BaseURL     string
}

type AWSConfig struct {
	Endpoint string
	Region   string
}

type StoreConfig struct {
	TableName string
}

type MessagingConfig struct {
	InboxQueueURL  string
	EventsTopicARN string
	PollWait       time.Duration
}

type Config struct {
	Server    *ServerConfig
	AWS       *AWSConfig
	Store     *StoreConfig
	Messaging *MessagingConfig
}

func Load() (*Config, error) {
	v := viper.New()
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()
	v.SetDefault("SERVER_PORT", "8080")
	v.SetDefault("AWS_DEFAULT_REGION", "sa-east-1")
	v.SetDefault("DYNAMODB_TABLE_NAME", "execution-tickets")
	v.SetDefault("MESSAGING_POLL_WAIT", "20s")

	return &Config{
		Server: &ServerConfig{
			Environment: v.GetString("SERVER_ENVIRONMENT"),
			Port:        v.GetString("SERVER_PORT"),
			BaseURL:     v.GetString("APP_BASE_URL"),
		},
		AWS: &AWSConfig{
			Endpoint: v.GetString("AWS_ENDPOINT_URL"),
			Region:   v.GetString("AWS_DEFAULT_REGION"),
		},
		Store: &StoreConfig{
			TableName: v.GetString("DYNAMODB_TABLE_NAME"),
		},
		Messaging: &MessagingConfig{
			InboxQueueURL:  v.GetString("MESSAGING_INBOX_QUEUE_URL"),
			EventsTopicARN: v.GetString("MESSAGING_EVENTS_TOPIC_ARN"),
			PollWait:       v.GetDuration("MESSAGING_POLL_WAIT"),
		},
	}, nil
}
