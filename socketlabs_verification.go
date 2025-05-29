package main

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"relay-go/m/logger"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func verifySocketLabsWebhookAndFindUser(db *sql.DB, headers http.Header) (int, int, error) {
	// SocketLabs typically uses API key authentication in headers
	authHeader := headers.Get("Authorization")
	if authHeader == "" {
		authHeader = headers.Get("X-API-Key")
	}

	logger.Info(context.Background(), "socketlabs-verification", "Auth Header", map[string]interface{}{
		"auth_header": authHeader,
	})

	if authHeader == "" {
		return 0, 0, fmt.Errorf("no authorization header found")
	}

	// Extract API key (remove Bearer prefix if present)
	apiKey := authHeader
	if len(authHeader) > 7 && authHeader[:7] == "Bearer " {
		apiKey = authHeader[7:]
	}

	var userID, espID int
	err := db.QueryRow(`
        SELECT user_id, esp_id 
        FROM email_service_providers 
        WHERE provider_name = 'socketlabs' 
        AND socketlabs_webhook_api_key = ?
    `, apiKey).Scan(&userID, &espID)

	if err != nil {
		if err == sql.ErrNoRows {
			return 0, 0, fmt.Errorf("no matching user found for the given API key")
		}
		return 0, 0, fmt.Errorf("database query failed: %v", err)
	}

	return userID, espID, nil
}

func verifySocketLabsWebhookAndFindUserDynamoDB(client *dynamodb.Client, headers http.Header) (int, string, error) {
	ctx := context.Background()
	logger.Info(ctx, "socketlabs-verification", "Starting DynamoDB verification", nil)

	// SocketLabs typically uses API key authentication in headers
	authHeader := headers.Get("Authorization")
	if authHeader == "" {
		authHeader = headers.Get("X-API-Key")
	}

	logger.Info(ctx, "socketlabs-verification", "Auth Header", map[string]interface{}{
		"auth_header": authHeader,
	})

	if authHeader == "" {
		return 0, "", fmt.Errorf("no authorization header found")
	}

	// Extract API key (remove Bearer prefix if present)
	apiKey := authHeader
	if len(authHeader) > 7 && authHeader[:7] == "Bearer " {
		apiKey = authHeader[7:]
	}

	// Scan the users table directly to find the user with matching SocketLabs credentials
	result, err := client.Scan(context.TODO(), &dynamodb.ScanInput{
		TableName:        aws.String("users"),
		FilterExpression: aws.String("socketlabs_webhook_api_key = :api_key"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":api_key": &types.AttributeValueMemberS{Value: apiKey},
		},
	})
	if err != nil {
		logger.Error(ctx, "socketlabs-verification", "DynamoDB scan failed", err)
		return 0, "", fmt.Errorf("dynamodb scan failed: %v", err)
	}

	if len(result.Items) == 0 {
		return 0, "", fmt.Errorf("no matching user found for the given API key")
	}

	// Get the first matching user
	item := result.Items[0]

	// Get user ID directly from the user record
	var userID string
	if idValue, ok := item["id"].(*types.AttributeValueMemberN); ok {
		userID = idValue.Value
	} else {
		return 0, "", fmt.Errorf("unexpected user_id type")
	}

	// Get email directly from the user record
	var email string
	if emailValue, ok := item["email"].(*types.AttributeValueMemberS); ok {
		email = emailValue.Value
	} else {
		return 0, "", fmt.Errorf("email not found or has unexpected type")
	}

	userIDInt, err := strconv.Atoi(userID)
	if err != nil {
		return 0, "", fmt.Errorf("invalid user ID format: %v", err)
	}

	logger.Info(ctx, "socketlabs-verification", "Found valid DynamoDB user", map[string]interface{}{
		"user_id": userID,
		"email":   email,
	})

	return userIDInt, email, nil
}

func associateSocketLabsEventWithUser(db *sql.DB, messageID string, userID, espID int) error {
	_, err := db.Exec(`
	INSERT INTO message_user_associations (message_id, user_id, esp_id, provider)
	SELECT ?, ?, esp_id, 'socketlabs'
	FROM email_service_providers
	WHERE user_id = ? AND provider_name = 'socketlabs'
	ON DUPLICATE KEY UPDATE id = id
`, messageID, userID, userID)

	if err != nil {
		return fmt.Errorf("failed to insert message association: %v", err)
	}
	return nil
}
