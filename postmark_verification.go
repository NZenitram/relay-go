package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"relay-go/m/logger"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type PostmarkVerification struct {
	httpClient *http.Client
	apiKey     string
}

func NewPostmarkVerification(apiKey string) *PostmarkVerification {
	return &PostmarkVerification{
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
		apiKey: apiKey,
	}
}

func (v *PostmarkVerification) VerifyEmail(ctx context.Context, email string) (bool, error) {
	url := "https://api.postmarkapp.com/email/validate"

	body := map[string]string{
		"email": email,
	}
	jsonBody, err := json.Marshal(body)
	if err != nil {
		logger.Error(ctx, "postmark-verification", "Failed to marshal request body", err)
		return false, fmt.Errorf("failed to marshal request body: %v", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(jsonBody))
	if err != nil {
		logger.Error(ctx, "postmark-verification", "Failed to create request", err)
		return false, fmt.Errorf("failed to create request: %v", err)
	}

	req.Header.Set("X-Postmark-Server-Token", v.apiKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := v.httpClient.Do(req)
	if err != nil {
		logger.Error(ctx, "postmark-verification", "Failed to send request", err)
		return false, fmt.Errorf("failed to send request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		logger.Error(ctx, "postmark-verification", "Received non-200 response", fmt.Errorf("status: %d", resp.StatusCode))
		return false, fmt.Errorf("received non-200 response: %d", resp.StatusCode)
	}

	var result struct {
		Valid bool `json:"valid"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		logger.Error(ctx, "postmark-verification", "Failed to decode response", err)
		return false, fmt.Errorf("failed to decode response: %v", err)
	}

	logger.Info(ctx, "postmark-verification", "Email verification completed", map[string]interface{}{
		"email": email,
		"valid": result.Valid,
	})
	return result.Valid, nil
}

func verifyPostmarkWebhookAndFindUser(db *sql.DB, headers http.Header) (int, int, error) {
	authHeader := headers.Get("Authorization")
	logger.Info(context.Background(), "postmark-verification", "Auth Header", map[string]interface{}{
		"auth_header": authHeader,
	})

	username, password, err := decodeBasicAuth(authHeader)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to decode auth header: %v", err)
	}

	var userID, espID int
	err = db.QueryRow(`
        SELECT user_id, esp_id 
        FROM email_service_providers 
        WHERE provider_name = 'postmark' 
        AND postmark_webhook_user = ? 
        AND postmark_webhook_password = ?
    `, username, password).Scan(&userID, &espID)

	if err != nil {
		if err == sql.ErrNoRows {
			return 0, 0, fmt.Errorf("no matching user found for the given credentials")
		}
		return 0, 0, fmt.Errorf("database query failed: %v", err)
	}

	return userID, espID, nil
}

func associatePostmarkEventWithUser(db *sql.DB, messageID string, userID, espID int) error {
	_, err := db.Exec(`
	INSERT INTO message_user_associations (message_id, user_id, esp_id, provider)
	SELECT ?, ?, esp_id, 'postmark'
	FROM email_service_providers
	WHERE user_id = ? AND provider_name = 'postmark'
	ON DUPLICATE KEY UPDATE id = id
`, messageID, userID, userID)

	if err != nil {
		return fmt.Errorf("failed to insert message association: %v", err)
	}
	return nil
}

func verifyPostmarkWebhookAndFindUserDynamoDB(client *dynamodb.Client, headers http.Header) (int, string, error) {
	ctx := context.Background()
	logger.Info(ctx, "postmark-verification", "Starting DynamoDB verification", nil)

	authHeader := headers.Get("Authorization")
	logger.Info(ctx, "postmark-verification", "Auth Header", map[string]interface{}{
		"auth_header": authHeader,
	})

	username, password, err := decodeBasicAuth(authHeader)
	if err != nil {
		return 0, "", fmt.Errorf("failed to decode auth header: %v", err)
	}

	// Scan the users table directly to find the user with matching Postmark credentials
	result, err := client.Scan(context.TODO(), &dynamodb.ScanInput{
		TableName:        aws.String("users"),
		FilterExpression: aws.String("postmark_webhook_user = :username AND postmark_webhook_password = :password"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":username": &types.AttributeValueMemberS{Value: username},
			":password": &types.AttributeValueMemberS{Value: password},
		},
	})
	if err != nil {
		logger.Error(ctx, "postmark-verification", "DynamoDB scan failed", err)
		return 0, "", fmt.Errorf("dynamodb scan failed: %v", err)
	}

	if len(result.Items) == 0 {
		return 0, "", fmt.Errorf("no matching user found for the given credentials")
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

	logger.Info(ctx, "postmark-verification", "Found valid DynamoDB user", map[string]interface{}{
		"user_id": userID,
		"email":   email,
	})

	return userIDInt, email, nil
}
