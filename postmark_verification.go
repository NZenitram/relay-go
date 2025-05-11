package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"relay-go/m/logger"
	"time"
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
	log.Printf("Auth Header: %s", authHeader)

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
