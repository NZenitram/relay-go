package main

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"relay-go/m/logger"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

// TestWebhookHandler handles webhooks in test mode for PARQUET conversion testing
type TestWebhookHandler struct {
	db           *sql.DB
	dynamoClient *dynamodb.Client
}

// NewTestWebhookHandler creates a new test webhook handler
func NewTestWebhookHandler(db *sql.DB, dynamoClient *dynamodb.Client) *TestWebhookHandler {
	return &TestWebhookHandler{
		db:           db,
		dynamoClient: dynamoClient,
	}
}

// IsTestMode checks if the request is in test mode
func IsTestMode(r *http.Request) bool {
	return r.Header.Get("X-Test-Mode") == "true"
}

// ProcessTestWebhook processes a webhook in test mode, bypassing normal verification
func (h *TestWebhookHandler) ProcessTestWebhook(ctx context.Context, provider string, body []byte, headers http.Header) (int, string, error) {
	// Get test user ID from header
	userIDStr := headers.Get("X-Test-User-ID")
	if userIDStr == "" {
		return 0, "", fmt.Errorf("missing X-Test-User-ID header in test mode")
	}

	userID, err := strconv.Atoi(userIDStr)
	if err != nil {
		return 0, "", fmt.Errorf("invalid X-Test-User-ID: %v", err)
	}

	// Use a test email for tracking
	testEmail := fmt.Sprintf("test-user-%d@parquet-test.local", userID)

	logger.Info(ctx, "test-webhook", "Processing test webhook", map[string]interface{}{
		"provider": provider,
		"user_id":  userID,
		"email":    testEmail,
		"mode":     "PARQUET_TEST",
	})

	return userID, testEmail, nil
}