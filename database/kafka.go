package database

import (
	"context"
	"os"
	"relay-go/m/logger"
	"sync"

	"github.com/Shopify/sarama"
)

var (
	kafkaClient sarama.SyncProducer
	kafkaOnce   sync.Once
)

// InitKafka initializes the Kafka connection
func InitKafka() error {
	var initErr error
	kafkaOnce.Do(func() {
		ctx := context.Background()
		brokers := []string{os.Getenv("KAFKA_BROKERS")}

		config := sarama.NewConfig()
		config.Producer.Return.Successes = true
		config.Producer.Return.Errors = true

		kafkaClient, initErr = sarama.NewSyncProducer(brokers, config)
		if initErr != nil {
			return
		}

		logger.Info(ctx, "kafka", "Successfully connected to Kafka")
	})
	return initErr
}

// GetKafkaClient returns the Kafka client
func GetKafkaClient() sarama.SyncProducer {
	if kafkaClient == nil {
		logger.Fatal(context.Background(), "kafka", "Kafka client has not been initialized. Call InitKafka() first.", nil)
	}
	return kafkaClient
}

// CloseKafka closes the Kafka connection
func CloseKafka() {
	if kafkaClient != nil {
		kafkaClient.Close()
		logger.Info(context.Background(), "kafka", "Kafka connection closed")
	}
}
