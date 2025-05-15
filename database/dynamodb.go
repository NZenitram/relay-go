package database

import (
	"context"
	"os"
	"relay-go/m/logger"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

var (
	dynamoClient *dynamodb.Client
	dynamoOnce   sync.Once
)

// InitDynamoDB initializes the DynamoDB connection
func InitDynamoDB() error {
	var initErr error
	dynamoOnce.Do(func() {
		ctx := context.Background()
		endpoint := os.Getenv("DYNAMODB_ENDPOINT")
		region := os.Getenv("AWS_REGION")
		accessKey := os.Getenv("AWS_ACCESS_KEY_ID")
		secretKey := os.Getenv("AWS_SECRET_ACCESS_KEY")

		// Load AWS configuration
		cfg, err := config.LoadDefaultConfig(context.TODO(),
			config.WithRegion(region),
		)
		if err != nil {
			initErr = err
			return
		}

		// If we're using local DynamoDB, use the provided credentials
		if endpoint != "" {
			cfg.Credentials = aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider(
				accessKey,
				secretKey,
				"", // session token is not needed for local development
			))
		}

		// Create DynamoDB client with custom endpoint if provided
		dynamoClient = dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
			if endpoint != "" {
				o.BaseEndpoint = aws.String(endpoint)
			}
		})

		// Verify connection by listing tables
		_, err = dynamoClient.ListTables(context.TODO(), &dynamodb.ListTablesInput{})
		if err != nil {
			initErr = err
			return
		}

		if initErr != nil {
			return
		}

		logger.Info(ctx, "dynamodb", "Successfully connected to DynamoDB", nil)
	})
	return initErr
}

// GetDynamoClient returns the DynamoDB client
func GetDynamoClient() *dynamodb.Client {
	if dynamoClient == nil {
		logger.Fatal(context.Background(), "dynamodb", "DynamoDB client has not been initialized. Call InitDynamoDB() first.", nil)
	}
	return dynamoClient
}
