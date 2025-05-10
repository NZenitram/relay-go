package database

import (
	"log"
	"os"
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
		brokers := []string{os.Getenv("KAFKA_BROKERS")}

		config := sarama.NewConfig()
		config.Producer.Return.Successes = true
		config.Producer.Return.Errors = true

		kafkaClient, initErr = sarama.NewSyncProducer(brokers, config)
		if initErr != nil {
			return
		}

		log.Println("Successfully connected to Kafka")
	})
	return initErr
}

// GetKafkaClient returns the Kafka client
func GetKafkaClient() sarama.SyncProducer {
	if kafkaClient == nil {
		log.Fatal("Kafka client has not been initialized. Call InitKafka() first.")
	}
	return kafkaClient
}

// CloseKafka closes the Kafka connection
func CloseKafka() {
	if kafkaClient != nil {
		kafkaClient.Close()
		log.Println("Kafka connection closed")
	}
}
