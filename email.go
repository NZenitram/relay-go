package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type User struct {
	ID     int
	APIKey string
}

func validateAPIKey(db *sql.DB, apiKey string) (*User, error) {
	var user User
	err := db.QueryRow("SELECT id, api_key FROM users WHERE api_key = ?", apiKey).Scan(&user.ID, &user.APIKey)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, errors.New("invalid API key")
		}
		return nil, fmt.Errorf("database query failed: %v", err)
	}

	return &user, nil
}

func validateAPIKeyDynamoDB(client *dynamodb.Client, apiKey string) (*User, error) {
	// Query DynamoDB for the user with the given API key
	result, err := client.Query(context.TODO(), &dynamodb.QueryInput{
		TableName:              aws.String("users"),
		IndexName:              aws.String("api_key-index"),
		KeyConditionExpression: aws.String("api_key = :apiKey"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":apiKey": &types.AttributeValueMemberS{Value: apiKey},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("dynamodb query failed: %v", err)
	}

	if len(result.Items) == 0 {
		return nil, errors.New("invalid API key")
	}

	// Extract user ID and API key from the DynamoDB item
	userIDStr := result.Items[0]["id"].(*types.AttributeValueMemberS).Value
	apiKeyStr := result.Items[0]["api_key"].(*types.AttributeValueMemberS).Value

	// Convert string ID to int
	var userID int
	_, err = fmt.Sscanf(userIDStr, "%d", &userID)
	if err != nil {
		return nil, fmt.Errorf("invalid user ID format: %v", err)
	}

	return &User{
		ID:     userID,
		APIKey: apiKeyStr,
	}, nil
}

func storeEmailRequest(db *sql.DB, userID int, messageID string) error {
	_, err := db.Exec(`
        INSERT INTO email_requests (user_id, message_id)
        VALUES (?, ?)
    `, userID, messageID)

	if err != nil {
		return fmt.Errorf("failed to store email request: %v", err)
	}

	return nil
}

func storeEmailRequestDynamoDB(client *dynamodb.Client, userID int, messageID string) error {
	// Create the email request item
	item := map[string]types.AttributeValue{
		"id":         &types.AttributeValueMemberS{Value: messageID},
		"user_id":    &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", userID)},
		"created_at": &types.AttributeValueMemberS{Value: time.Now().UTC().Format(time.RFC3339)},
	}

	// Put the item in DynamoDB
	_, err := client.PutItem(context.TODO(), &dynamodb.PutItemInput{
		TableName: aws.String("email_requests"),
		Item:      item,
	})
	if err != nil {
		return fmt.Errorf("failed to store email request in DynamoDB: %v", err)
	}

	return nil
}

func logInvalidAttempt(apiKey string) {
	log.Printf("WARNING: Invalid API Key attempt: %s", apiKey)
}
