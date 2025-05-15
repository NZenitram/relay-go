package database

import (
	"context"
	"fmt"
	"relay-go/m/logger"
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
	ctx := context.Background()
	var redisErr, dynamoErr error

	// Initialize Redis first
	if err := InitRedis(); err != nil {
		redisErr = err
		logger.Warning(ctx, "database", "Failed to initialize Redis", err)
		// Continue without Redis, we'll fall back to direct DynamoDB access
	}

	// Try to initialize MySQL
	if err := InitDB(); err != nil {
		logger.Error(ctx, "database", "Failed to initialize MySQL", err)
		// Continue without MySQL
	}

	// Try to initialize Kafka
	if err := InitKafka(); err != nil {
		logger.Error(ctx, "database", "Failed to initialize Kafka", err)
		// Continue without Kafka
	}

	// Initialize DynamoDB
	if err := InitDynamoDB(); err != nil {
		dynamoErr = err
		logger.Error(ctx, "database", "Failed to initialize DynamoDB", err)
	}

	// If both Redis and DynamoDB failed, log a critical error
	if redisErr != nil && dynamoErr != nil {
		logger.Error(ctx, "database", "Critical: Both Redis and DynamoDB initialization failed", fmt.Errorf("redis error: %v, dynamodb error: %v", redisErr, dynamoErr))
		return fmt.Errorf("critical: both Redis and DynamoDB initialization failed: redis error: %v, dynamodb error: %v", redisErr, dynamoErr)
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
	ctx := context.Background()
	logger.Error(ctx, "database", "Closing data store connections...", nil)

	switch CurrentDataStore {
	case MySQLKafka:
		CloseDB()
		CloseKafka()
	case DynamoDB:
		// DynamoDB doesn't need explicit closing
		logger.Error(ctx, "database", "DynamoDB connections closed", nil)
	}

	logger.Error(ctx, "database", "All data store connections closed", nil)
}

// IsMySQLKafkaMode returns true if we're using MySQL + Kafka configuration
func IsMySQLKafkaMode() bool {
	return CurrentDataStore == MySQLKafka
}

// IsDynamoDBMode returns true if we're using DynamoDB-only configuration
func IsDynamoDBMode() bool {
	return CurrentDataStore == DynamoDB
}
