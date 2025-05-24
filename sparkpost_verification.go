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

func verifySparkPostWebhookAndFindUser(db *sql.DB, headers http.Header) (int, int, error) {
	authHeader := headers.Get("Authorization")
	logger.Info(context.Background(), "sparkpost-verification", "Auth Header", map[string]interface{}{
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
        WHERE provider_name = 'sparkpost' 
        AND sparkpost_webhook_user = ? 
        AND sparkpost_webhook_password = ?
    `, username, password).Scan(&userID, &espID)

	if err != nil {
		if err == sql.ErrNoRows {
			return 0, 0, fmt.Errorf("no matching user found for the given credentials")
		}
		return 0, 0, fmt.Errorf("database query failed: %v", err)
	}

	return userID, espID, nil
}

func associateSparkPostEventWithUser(db *sql.DB, messageID string, userID, espID int) error {
	_, err := db.Exec(`
	INSERT INTO message_user_associations (message_id, user_id, esp_id, provider)
	SELECT ?, ?, esp_id, 'sparkpost'
	FROM email_service_providers
	WHERE user_id = ? AND provider_name = 'sparkpost'
	ON DUPLICATE KEY UPDATE id = id
`, messageID, userID, userID)

	if err != nil {
		return fmt.Errorf("failed to insert message association: %v", err)
	}
	return nil
}

type SparkPostWebhookHeaders struct {
	AcceptEncoding      []string `json:"Accept-Encoding"`
	Authorization       []string `josn:"Authorization"`
	ContentLength       []string `json:"Content-Length"`
	ContentType         []string `json:"Content-Type"`
	UserAgent           []string `json:"User-Agent"`
	XForwardedFor       []string `json:"X-Forwarded-For"`
	XForwardedHost      []string `json:"X-Forwarded-Host"`
	XForwardedProto     []string `json:"X-Forwarded-Proto"`
	XSparkpostSignature []string `json:"X-Sparkpost-Signature"`
}

type SparkPostVerification struct {
	httpClient *http.Client
	apiKey     string
}

func NewSparkPostVerification(apiKey string) *SparkPostVerification {
	return &SparkPostVerification{
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
		apiKey: apiKey,
	}
}

func (v *SparkPostVerification) VerifyEmail(ctx context.Context, email string) (bool, error) {
	url := "https://api.sparkpost.com/api/v1/recipient-validation/single"

	body := map[string]string{
		"email": email,
	}
	jsonBody, err := json.Marshal(body)
	if err != nil {
		logger.Error(ctx, "sparkpost-verification", "Failed to marshal request body", err)
		return false, fmt.Errorf("failed to marshal request body: %v", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(jsonBody))
	if err != nil {
		logger.Error(ctx, "sparkpost-verification", "Failed to create request", err)
		return false, fmt.Errorf("failed to create request: %v", err)
	}

	req.Header.Set("Authorization", v.apiKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := v.httpClient.Do(req)
	if err != nil {
		logger.Error(ctx, "sparkpost-verification", "Failed to send request", err)
		return false, fmt.Errorf("failed to send request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		logger.Error(ctx, "sparkpost-verification", "Received non-200 response", fmt.Errorf("status: %d", resp.StatusCode))
		return false, fmt.Errorf("received non-200 response: %d", resp.StatusCode)
	}

	var result struct {
		Results struct {
			Valid bool `json:"valid"`
		} `json:"results"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		logger.Error(ctx, "sparkpost-verification", "Failed to decode response", err)
		return false, fmt.Errorf("failed to decode response: %v", err)
	}

	logger.Info(ctx, "sparkpost-verification", "Email verification completed", map[string]interface{}{
		"email": email,
		"valid": result.Results.Valid,
	})
	return result.Results.Valid, nil
}

func verifySparkPostWebhookAndFindUserDynamoDB(client *dynamodb.Client, headers http.Header) (int, string, error) {
	ctx := context.Background()
	logger.Info(ctx, "sparkpost-verification", "Starting DynamoDB verification", nil)

	authHeader := headers.Get("Authorization")
	logger.Info(ctx, "sparkpost-verification", "Auth Header", map[string]interface{}{
		"auth_header": authHeader,
	})

	username, password, err := decodeBasicAuth(authHeader)
	if err != nil {
		return 0, "", fmt.Errorf("failed to decode auth header: %v", err)
	}

	// Scan the users table directly to find the user with matching SparkPost credentials
	result, err := client.Scan(context.TODO(), &dynamodb.ScanInput{
		TableName:        aws.String("users"),
		FilterExpression: aws.String("sparkpost_webhook_user = :username AND sparkpost_webhook_password = :password"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":username": &types.AttributeValueMemberS{Value: username},
			":password": &types.AttributeValueMemberS{Value: password},
		},
	})
	if err != nil {
		logger.Error(ctx, "sparkpost-verification", "DynamoDB scan failed", err)
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

	logger.Info(ctx, "sparkpost-verification", "Found valid DynamoDB user", map[string]interface{}{
		"user_id": userID,
		"email":   email,
	})

	return userIDInt, email, nil
}
