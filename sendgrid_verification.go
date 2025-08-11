package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"relay-go/m/database"
	"strconv"
	"time"

	"relay-go/m/logger"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/sendgrid/sendgrid-go/helpers/eventwebhook"
)

type DynamoDBUser struct {
	ID                      int64  `json:"id" dynamodbav:"id"`
	SendGridVerificationKey string `json:"sendgrid_verification_key" dynamodbav:"sendgrid_verification_key"`
	Email                   string `json:"email" dynamodbav:"email"`
	CreatedAt               int64  `json:"created_at" dynamodbav:"created_at"`
	UpdatedAt               int64  `json:"updated_at" dynamodbav:"updated_at"`
}

type SendgridWebhookPayload struct {
	Headers map[string][]string `json:"headers"`
	Body    json.RawMessage     `json:"body"`
}

type SendGridVerification struct {
	httpClient *http.Client
	apiKey     string
}

func NewSendGridVerification(apiKey string) *SendGridVerification {
	return &SendGridVerification{
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
		apiKey: apiKey,
	}
}

// func (v *SendGridVerification) VerifyEmail(ctx context.Context, email string) (bool, error) {
// 	url := fmt.Sprintf("https://api.sendgrid.com/v3/validations/email", email)

// 	body := map[string]string{
// 		"email": email,
// 	}
// 	jsonBody, err := json.Marshal(body)
// 	if err != nil {
// 		logger.Error(ctx, "sendgrid-verification", "Failed to marshal request body", err)
// 		return false, fmt.Errorf("failed to marshal request body: %v", err)
// 	}

// 	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(jsonBody))
// 	if err != nil {
// 		logger.Error(ctx, "sendgrid-verification", "Failed to create request", err)
// 		return false, fmt.Errorf("failed to create request: %v", err)
// 	}

// 	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", v.apiKey))
// 	req.Header.Set("Content-Type", "application/json")

// 	resp, err := v.httpClient.Do(req)
// 	if err != nil {
// 		logger.Error(ctx, "sendgrid-verification", "Failed to send request", err)
// 		return false, fmt.Errorf("failed to send request: %v", err)
// 	}
// 	defer resp.Body.Close()

// 	if resp.StatusCode != http.StatusOK {
// 		logger.Error(ctx, "sendgrid-verification", "Received non-200 response", fmt.Errorf("status: %d", resp.StatusCode))
// 		return false, fmt.Errorf("received non-200 response: %d", resp.StatusCode)
// 	}

// 	var result struct {
// 		Valid bool `json:"valid"`
// 	}
// 	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
// 		logger.Error(ctx, "sendgrid-verification", "Failed to decode response", err)
// 		return false, fmt.Errorf("failed to decode response: %v", err)
// 	}

// 	logger.Info(ctx, "sendgrid-verification", "Email verification completed", map[string]interface{}{
// 		"email": email,
// 		"valid": result.Valid,
// 	})
// 	return result.Valid, nil
// }

// func isMySQLAvailable(db *sql.DB) bool {
// 	err := db.Ping()
// 	return err == nil
// }

type MySQLUser struct {
	ID                      int    `json:"id"`
	SendGridVerificationKey string `json:"sendgrid_verification_key"`
	Email                   string `json:"email"`
	CreatedAt               int64  `json:"created_at"`
	UpdatedAt               int64  `json:"updated_at"`
}

// Common user data structure for caching
type UserData struct {
	ID                      int64  `json:"id"`
	SendGridVerificationKey string `json:"sendgrid_verification_key"`
	Email                   string `json:"email"`
	CreatedAt               int64  `json:"created_at"`
	UpdatedAt               int64  `json:"updated_at"`
}

// verifyWebhookSignature verifies the SendGrid webhook signature
func verifyWebhookSignature(ctx context.Context, verificationKey string, body []byte, signature, timestamp string) (bool, error) {
	ecdaKey, err := eventwebhook.ConvertPublicKeyBase64ToECDSA(verificationKey)
	if err != nil {
		logger.Error(ctx, "sendgrid-verification", "Cannot convert public key", err, map[string]interface{}{
			"verification_key_length": len(verificationKey),
			"verification_key_prefix": verificationKey[:20] + "...",
		})
		return false, fmt.Errorf("cannot convert public key: %v", err)
	}

	valid, err := eventwebhook.VerifySignature(ecdaKey, body, signature, timestamp)
	if err != nil {
		logger.Error(ctx, "sendgrid-verification", "Signature verification failed", err, map[string]interface{}{
			"signature":               signature,
			"timestamp":               timestamp,
			"body_length":             len(body),
			"verification_key_length": len(verificationKey),
		})
		return false, fmt.Errorf("signature verification failed: %v", err)
	}

	return valid, nil
}

// getUserDataFromCache attempts to get user data from cache
func getUserDataFromCache(ctx context.Context, userID string) (*UserData, bool, error) {
	var cachedUser UserData
	cacheHit, err := database.GetCachedUserData(userID, &cachedUser)
	if err != nil {
		logger.Info(ctx, "sendgrid-verification", "Cache error for user", nil, map[string]interface{}{
			"user_id": userID,
			"error":   err.Error(),
		})
		return nil, false, err
	}
	if cacheHit {
		return &cachedUser, true, nil
	}
	return nil, false, nil
}

// cacheUserData stores user data in cache
func cacheUserData(ctx context.Context, userID string, userData UserData) error {
	if err := database.CacheUserData(userID, userData); err != nil {
		log.Printf("Failed to cache user data for ID %s: %v", userID, err)
		return err
	}
	logger.Info(ctx, "sendgrid-verification", "Successfully cached user data", map[string]interface{}{
		"user_id": userID,
	})
	return nil
}

func verifySendgridWebhookAndFindUser(db *sql.DB, body []byte, headers http.Header) (int, string, error) {
	ctx := context.Background()
	logger.Info(ctx, "sendgrid-verification", "Starting MySQL verification", nil)
	signature := headers.Get("X-Twilio-Email-Event-Webhook-Signature")
	timestamp := headers.Get("X-Twilio-Email-Event-Webhook-Timestamp")

	if signature == "" || timestamp == "" {
		return 0, "", fmt.Errorf("missing webhook signature or timestamp")
	}

	rows, err := db.Query(`
		SELECT id, sendgrid_verification_key, email, UNIX_TIMESTAMP(created_at), UNIX_TIMESTAMP(updated_at)
		FROM users
		WHERE sendgrid_verification_key IS NOT NULL
	`)
	if err != nil {
		logger.Error(ctx, "sendgrid-verification", "MySQL query failed", err)
		return 0, "", fmt.Errorf("database query failed: %v", err)
	}
	defer rows.Close()

	var foundUsers int
	for rows.Next() {
		foundUsers++
		var userID int
		var verificationKey string
		var email string
		var createdAt, updatedAt int64
		if err := rows.Scan(&userID, &verificationKey, &email, &createdAt, &updatedAt); err != nil {
			logger.Error(ctx, "sendgrid-verification", "Failed to scan MySQL user row", err)
			continue
		}

		logger.Info(ctx, "sendgrid-verification", "Checking MySQL user", map[string]interface{}{
			"user_id": userID,
			"email":   email,
		})

		// Try to get user data from cache
		cachedUser, cacheHit, err := getUserDataFromCache(ctx, fmt.Sprintf("%d", userID))
		if err == nil && cacheHit {
			verificationKey = cachedUser.SendGridVerificationKey
			logger.Info(ctx, "sendgrid-verification", "Using cached verification key for MySQL user", map[string]interface{}{
				"user_id":   userID,
				"cache_hit": true,
			})
		} else {
			// Cache miss or error, store in cache
			userData := UserData{
				ID:                      int64(userID),
				SendGridVerificationKey: verificationKey,
				Email:                   email,
				CreatedAt:               createdAt,
				UpdatedAt:               updatedAt,
			}
			if err := cacheUserData(ctx, fmt.Sprintf("%d", userID), userData); err != nil {
				logger.Error(ctx, "sendgrid-verification", "Failed to cache MySQL user data", err)
			}
		}

		valid, err := verifyWebhookSignature(ctx, verificationKey, body, signature, timestamp)
		if err != nil {
			logger.Error(ctx, "sendgrid-verification", "MySQL user verification failed", err)
			continue
		}

		if valid {
			logger.Info(ctx, "sendgrid-verification", "Found valid MySQL user", map[string]interface{}{
				"user_id": userID,
				"email":   email,
			})
			return userID, email, nil
		}
	}

	logger.Info(ctx, "sendgrid-verification", "MySQL verification complete", map[string]interface{}{
		"users_checked": foundUsers,
	})
	return 0, "", fmt.Errorf("no matching user found for the given webhook signature: %v", signature)
}

func verifySendgridWebhookAndFindUserDynamoDB(client *dynamodb.Client, body []byte, headers http.Header) (int, string, error) {
	ctx := context.Background()
	signature := headers.Get("X-Twilio-Email-Event-Webhook-Signature")
	timestamp := headers.Get("X-Twilio-Email-Event-Webhook-Timestamp")

	if signature == "" || timestamp == "" {
		return 0, "", fmt.Errorf("missing webhook signature or timestamp")
	}

	result, err := client.Scan(context.TODO(), &dynamodb.ScanInput{
		TableName:        aws.String("users"),
		FilterExpression: aws.String("attribute_exists(sendgrid_verification_key)"),
	})
	if err != nil {
		logger.Error(ctx, "sendgrid-verification", "DynamoDB scan failed", err)
		return 0, "", fmt.Errorf("dynamodb scan failed: %v", err)
	}

	if len(result.Items) == 0 {
		logger.Error(ctx, "sendgrid-verification", "No users found with SendGrid verification keys", nil, map[string]interface{}{
			"total_users_found": 0,
			"signature":         signature,
			"timestamp":         timestamp,
			"body_length":       len(body),
		})
		return 0, "", fmt.Errorf("no users have sendgrid_verification_key configured")
	}

	for i, item := range result.Items {
		// Get user ID
		var userID string
		switch idValue := item["id"].(type) {
		case *types.AttributeValueMemberN:
			userID = idValue.Value
		case *types.AttributeValueMemberS:
			userID = idValue.Value
		default:
			logger.Error(ctx, "sendgrid-verification", "Unexpected ID type for DynamoDB item", nil, map[string]interface{}{
				"user_index": i,
				"id_type":    fmt.Sprintf("%T", item["id"]),
			})
			continue
		}

		verificationKey := item["sendgrid_verification_key"].(*types.AttributeValueMemberS).Value
		email := item["email"].(*types.AttributeValueMemberS).Value

		// Try to get user data from cache
		cachedUser, cacheHit, err := getUserDataFromCache(ctx, userID)
		if err == nil && cacheHit {
			verificationKey = cachedUser.SendGridVerificationKey
		} else {
			// Cache miss or error, store in cache
			userData := UserData{
				ID:                      parseNumericID(userID),
				SendGridVerificationKey: verificationKey,
				Email:                   email,
				CreatedAt:               parseTimestamp(item["created_at"].(*types.AttributeValueMemberN).Value),
				UpdatedAt:               parseTimestamp(item["updated_at"].(*types.AttributeValueMemberN).Value),
			}
			if err := cacheUserData(ctx, userID, userData); err != nil {
				logger.Error(ctx, "sendgrid-verification", "Failed to cache DynamoDB user data", err)
			}
		}

		valid, err := verifyWebhookSignature(ctx, verificationKey, body, signature, timestamp)
		if err != nil {
			continue
		}

		if valid {
			userIDInt, err := strconv.Atoi(userID)
			if err != nil {
				logger.Error(ctx, "sendgrid-verification", "Invalid user ID format", err)
				return 0, "", fmt.Errorf("invalid user ID format for %s: %v", userID, err)
			}
			return userIDInt, email, nil
		}
	}

	logger.Error(ctx, "sendgrid-verification", "No matching user found after checking all users", nil)
	return 0, "", fmt.Errorf("no matching user found for the given webhook signature: %v", signature)
}

// func associateSendgridEventWithUser(db *sql.DB, sgEventBody SendGridEventBody, userID int) error {
// 	// Only attempt MySQL operations if MySQL is available
// 	if !isMySQLAvailable(db) {
// 		return nil
// 	}

// 	// Insert the association into the message_user_associations table
// 	_, err := db.Exec(`
// 		INSERT INTO message_user_associations (message_id, user_id, esp_id, provider)
// 		SELECT ?, ?, esp_id, 'sendgrid'
// 		FROM email_service_providers
// 		WHERE user_id = ? AND provider_name = 'sendgrid'
// 		ON DUPLICATE KEY UPDATE id = id
// `, sgEventBody.SGMessageID, userID, userID)
// 	if err != nil {
// 		return fmt.Errorf("failed to insert message association: %v", err)
// 	}

// 	return nil
// }

type SendGridEventBody struct {
	Email         string   `json:"email"`
	FromAddress   string   `json:"from,omitempty"`
	Event         string   `json:"event"`
	SGMessageID   string   `json:"sg_message_id"`
	SGMachineOpen bool     `json:"sg_machine_open"`
	Timestamp     int64    `json:"timestamp"`
	Category      []string `json:"category"`
	SGEventID     string   `json:"sg_event_id"`
	SMTPID        string   `json:"smtp-id"`
	BounceType    string   `json:"bounce_type,omitempty"`
	Reason        string   `json:"reason,omitempty"`
}

type EventPayload struct {
	Body []json.RawMessage `json:"body"`
}

// Helper function to parse timestamp string to int64
func parseTimestamp(timestampStr string) int64 {
	timestamp, err := strconv.ParseInt(timestampStr, 10, 64)
	if err != nil {
		log.Printf("Failed to parse timestamp %s: %v", timestampStr, err)
		return 0
	}
	return timestamp
}

// Helper function to parse numeric ID from string
func parseNumericID(idStr string) int64 {
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		log.Printf("Failed to parse numeric ID %s: %v", idStr, err)
		return 0
	}
	return id
}
