package database

import (
	"fmt"
	"log"
	"time"
)

// DataStoreType represents the type of data store configuration
type DataStoreType string

const (
	// MySQLKafka represents MySQL + Kafka configuration
	MySQLKafka DataStoreType = "mysql_kafka"
	// DynamoDB represents DynamoDB-only configuration
	DynamoDB DataStoreType = "dynamodb"
)

var (
	// CurrentDataStore holds the active data store configuration
	CurrentDataStore DataStoreType
)

// InitDataStores initializes the appropriate data stores based on availability
func InitDataStores() error {
	// Initialize Redis first
	if err := InitRedis(); err != nil {
		log.Printf("Failed to initialize Redis: %v", err)
		// Continue without Redis, we'll fall back to direct DynamoDB access
	}

	// Try to initialize MySQL
	if err := InitDB(); err != nil {
		log.Printf("Failed to initialize MySQL: %v", err)
		// Continue without MySQL
	}

	// Try to initialize Kafka
	if err := InitKafka(); err != nil {
		log.Printf("Failed to initialize Kafka: %v", err)
		// Continue without Kafka
	}

	// Initialize DynamoDB
	if err := InitDynamoDB(); err != nil {
		return fmt.Errorf("failed to initialize DynamoDB: %v", err)
	}

	return nil
}

// tryInitMySQL attempts to initialize MySQL and returns any error
func tryInitMySQL() error {
	// Set a timeout for MySQL connection attempt
	timeout := time.After(5 * time.Second)
	done := make(chan error, 1)

	go func() {
		done <- InitDB()
	}()

	select {
	case err := <-done:
		return err
	case <-timeout:
		return fmt.Errorf("MySQL connection timeout")
	}
}

// tryInitKafka attempts to initialize Kafka and returns any error
func tryInitKafka() error {
	// Set a timeout for Kafka connection attempt
	timeout := time.After(5 * time.Second)
	done := make(chan error, 1)

	go func() {
		done <- InitKafka()
	}()

	select {
	case err := <-done:
		return err
	case <-timeout:
		return fmt.Errorf("Kafka connection timeout")
	}
}

// tryInitDynamoDB attempts to initialize DynamoDB and returns any error
func tryInitDynamoDB() error {
	// Set a timeout for DynamoDB connection attempt
	timeout := time.After(5 * time.Second)
	done := make(chan error, 1)

	go func() {
		done <- InitDynamoDB()
	}()

	select {
	case err := <-done:
		return err
	case <-timeout:
		return fmt.Errorf("DynamoDB connection timeout")
	}
}

// CloseDataStores closes all active data store connections
func CloseDataStores() {
	log.Println("Closing data store connections...")

	switch CurrentDataStore {
	case MySQLKafka:
		CloseDB()
		CloseKafka()
	case DynamoDB:
		// DynamoDB doesn't need explicit closing
		log.Println("DynamoDB connections closed")
	}

	log.Println("All data store connections closed")
}

// IsMySQLKafkaMode returns true if we're using MySQL + Kafka configuration
func IsMySQLKafkaMode() bool {
	return CurrentDataStore == MySQLKafka
}

// IsDynamoDBMode returns true if we're using DynamoDB-only configuration
func IsDynamoDBMode() bool {
	return CurrentDataStore == DynamoDB
}
