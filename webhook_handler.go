package main

import (
	"context"
	"fmt"
	"net/http"
	"relay-go/m/database"
	"relay-go/m/logger"

	"database/sql"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

// WebhookHandler handles webhook processing for different providers
type WebhookHandler struct {
	db       *sql.DB
	dynamoDB *dynamodb.Client
}

// NewWebhookHandler creates a new WebhookHandler instance
func NewWebhookHandler(db *sql.DB, dynamoDB *dynamodb.Client) *WebhookHandler {
	return &WebhookHandler{
		db:       db,
		dynamoDB: dynamoDB,
	}
}

// ProcessWebhook handles the webhook processing for different providers
func (h *WebhookHandler) ProcessWebhook(ctx context.Context, provider string, body []byte, headers http.Header) (int, string, error) {
	var userID int
	var email string
	var verifyErr error

	// Verify webhook and find user based on the provider and data store mode
	switch provider {
	case "sendgrid":
		if database.IsMySQLKafkaMode() {
			userID, email, verifyErr = verifySendgridWebhookAndFindUser(h.db, body, headers)
		} else {
			userID, email, verifyErr = verifySendgridWebhookAndFindUserDynamoDB(h.dynamoDB, body, headers)
		}
	case "sparkpost":
		if database.IsMySQLKafkaMode() {
			userID, _, verifyErr = verifySparkPostWebhookAndFindUser(h.db, headers)
		} else {
			userID, email, verifyErr = verifySparkPostWebhookAndFindUserDynamoDB(h.dynamoDB, headers)
		}
	default:
		return 0, "", fmt.Errorf("unsupported provider: %s", provider)
	}

	if verifyErr != nil {
		logger.Error(ctx, "webhook-handler", "Failed to verify webhook", verifyErr, map[string]interface{}{
			"user_id":  userID,
			"provider": provider,
		})
		return 0, "", verifyErr
	}

	// Add user ID to context
	ctx = logger.WithUserID(ctx, fmt.Sprintf("%d", userID))

	return userID, email, nil
}
