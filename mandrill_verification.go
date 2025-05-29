package main

import (
	"context"
	"crypto/hmac"
	"crypto/sha1"
	"database/sql"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/url"
	"relay-go/m/logger"
	"sort"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// generateMandrillSignature generates a signature for Mandrill webhook verification
// Based on Mailchimp Transactional documentation:
// https://mailchimp.com/developer/transactional/guides/track-respond-activity-webhooks/#authenticating-webhook-requests
func generateMandrillSignature(webhookKey, webhookURL string, params url.Values) string {
	// Step 1: Create a string with the webhook URL
	signedData := webhookURL

	// Step 2: Append each POST variable's key and value to the URL string, with no delimiter
	// Sort keys for consistent signature generation
	keys := make([]string, 0, len(params))
	for key := range params {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	for _, key := range keys {
		signedData += key + params.Get(key)
	}

	// Step 3: Hash the resulting string with HMAC-SHA1
	mac := hmac.New(sha1.New, []byte(webhookKey))
	mac.Write([]byte(signedData))

	// Step 4: Base64 encode the binary signature
	return base64.StdEncoding.EncodeToString(mac.Sum(nil))
}

// verifyMandrillSignature verifies the Mandrill webhook signature
func verifyMandrillSignature(ctx context.Context, webhookKey, webhookURL string, params url.Values, receivedSignature string) bool {
	expectedSignature := generateMandrillSignature(webhookKey, webhookURL, params)

	logger.Info(ctx, "mandrill-verification", "Signature verification", map[string]interface{}{
		"expected_signature": expectedSignature,
		"received_signature": receivedSignature,
		"signatures_match":   expectedSignature == receivedSignature,
	})

	return expectedSignature == receivedSignature
}

func verifyMandrillWebhookAndFindUser(db *sql.DB, webhookURL string, params url.Values, headers http.Header) (int, int, string, error) {
	ctx := context.Background()
	logger.Info(ctx, "mandrill-verification", "Starting MySQL verification", nil)

	signature := headers.Get("X-Mandrill-Signature")
	if signature == "" {
		return 0, 0, "", fmt.Errorf("missing X-Mandrill-Signature header")
	}

	rows, err := db.Query(`
		SELECT esp.user_id, esp.esp_id, esp.mandrill_webhook_key, u.email
		FROM email_service_providers esp
		JOIN users u ON esp.user_id = u.id
		WHERE esp.provider_name = 'mandrill' 
		AND esp.mandrill_webhook_key IS NOT NULL
	`)
	if err != nil {
		logger.Error(ctx, "mandrill-verification", "MySQL query failed", err)
		return 0, 0, "", fmt.Errorf("database query failed: %v", err)
	}
	defer rows.Close()

	var foundUsers int
	for rows.Next() {
		foundUsers++
		var userID, espID int
		var webhookKey, email string
		if err := rows.Scan(&userID, &espID, &webhookKey, &email); err != nil {
			logger.Error(ctx, "mandrill-verification", "Failed to scan MySQL row", err)
			continue
		}

		logger.Info(ctx, "mandrill-verification", "Checking MySQL user", map[string]interface{}{
			"user_id": userID,
			"esp_id":  espID,
			"email":   email,
		})

		if verifyMandrillSignature(ctx, webhookKey, webhookURL, params, signature) {
			logger.Info(ctx, "mandrill-verification", "Found valid MySQL user", map[string]interface{}{
				"user_id": userID,
				"esp_id":  espID,
				"email":   email,
			})
			return userID, espID, email, nil
		}
	}

	logger.Info(ctx, "mandrill-verification", "MySQL verification complete", map[string]interface{}{
		"users_checked": foundUsers,
	})
	return 0, 0, "", fmt.Errorf("no matching user found for the given webhook signature: %v", signature)
}

func verifyMandrillWebhookAndFindUserDynamoDB(client *dynamodb.Client, webhookURL string, params url.Values, headers http.Header) (int, string, error) {
	ctx := context.Background()
	logger.Info(ctx, "mandrill-verification", "Starting DynamoDB verification", nil)

	signature := headers.Get("X-Mandrill-Signature")
	if signature == "" {
		return 0, "", fmt.Errorf("missing X-Mandrill-Signature header")
	}

	// Scan the users table to find users with Mandrill webhook keys
	result, err := client.Scan(context.TODO(), &dynamodb.ScanInput{
		TableName:        aws.String("users"),
		FilterExpression: aws.String("attribute_exists(mandrill_webhook_key)"),
	})
	if err != nil {
		logger.Error(ctx, "mandrill-verification", "DynamoDB scan failed", err)
		return 0, "", fmt.Errorf("dynamodb scan failed: %v", err)
	}

	logger.Info(ctx, "mandrill-verification", "DynamoDB scan results", map[string]interface{}{
		"items_found": len(result.Items),
	})

	for _, item := range result.Items {
		// Get user ID
		var userID string
		switch idValue := item["id"].(type) {
		case *types.AttributeValueMemberN:
			userID = idValue.Value
		case *types.AttributeValueMemberS:
			userID = idValue.Value
		default:
			logger.Error(ctx, "mandrill-verification", "Unexpected ID type for DynamoDB item", nil)
			continue
		}

		webhookKey := item["mandrill_webhook_key"].(*types.AttributeValueMemberS).Value
		email := item["email"].(*types.AttributeValueMemberS).Value

		logger.Info(ctx, "mandrill-verification", "Checking DynamoDB user", map[string]interface{}{
			"user_id": userID,
			"email":   email,
		})

		if verifyMandrillSignature(ctx, webhookKey, webhookURL, params, signature) {
			logger.Info(ctx, "mandrill-verification", "Found valid DynamoDB user", map[string]interface{}{
				"user_id": userID,
				"email":   email,
			})
			userIDInt, err := strconv.Atoi(userID)
			if err != nil {
				logger.Error(ctx, "mandrill-verification", "Invalid user ID format", err)
				return 0, "", fmt.Errorf("invalid user ID format for %s: %v", userID, err)
			}
			return userIDInt, email, nil
		}
	}

	logger.Info(ctx, "mandrill-verification", "DynamoDB verification complete", nil)
	return 0, "", fmt.Errorf("no matching user found for the given webhook signature: %v", signature)
}

func associateMandrillEventWithUser(db *sql.DB, messageID string, userID, espID int) error {
	_, err := db.Exec(`
	INSERT INTO message_user_associations (message_id, user_id, esp_id, provider)
	SELECT ?, ?, esp_id, 'mandrill'
	FROM email_service_providers
	WHERE user_id = ? AND provider_name = 'mandrill'
	ON DUPLICATE KEY UPDATE id = id
`, messageID, userID, userID)

	if err != nil {
		return fmt.Errorf("failed to insert message association: %v", err)
	}
	return nil
}
