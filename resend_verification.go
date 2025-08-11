package main

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"net/http"
	"relay-go/m/database"
	"relay-go/m/logger"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// verifyResendWebhookSignature verifies the Resend webhook signature using Svix headers
func verifyResendWebhookSignature(ctx context.Context, webhookSecret string, body []byte, headers http.Header) (bool, error) {
	// Get Svix headers
	svixID := headers.Get("svix-id")
	svixTimestamp := headers.Get("svix-timestamp")
	svixSignature := headers.Get("svix-signature")

	if svixID == "" || svixTimestamp == "" || svixSignature == "" {
		logger.Error(ctx, "resend-verification", "Missing required Svix headers", nil, map[string]interface{}{
			"svix_id_present":        svixID != "",
			"svix_timestamp_present": svixTimestamp != "",
			"svix_signature_present": svixSignature != "",
		})
		return false, fmt.Errorf("missing required Svix headers")
	}

	// Verify timestamp is recent (within 5 minutes)
	timestamp, err := strconv.ParseInt(svixTimestamp, 10, 64)
	if err != nil {
		logger.Error(ctx, "resend-verification", "Invalid timestamp format", err)
		return false, fmt.Errorf("invalid timestamp format: %v", err)
	}

	currentTime := time.Now().Unix()
	if currentTime-timestamp > 300 || timestamp > currentTime+300 {
		logger.Error(ctx, "resend-verification", "Timestamp too old or in future", nil, map[string]interface{}{
			"current_time":      currentTime,
			"webhook_timestamp": timestamp,
			"difference":        currentTime - timestamp,
		})
		return false, fmt.Errorf("timestamp is too old or in the future")
	}

	// Construct the signed content
	signedContent := fmt.Sprintf("%s.%s.%s", svixID, svixTimestamp, string(body))

	// Extract the signature (format: v1,signature1 v1,signature2 ...)
	signatures := strings.Split(svixSignature, " ")
	for i, sig := range signatures {
		parts := strings.Split(sig, ",")
		if len(parts) != 2 || parts[0] != "v1" {
			continue
		}

		// Decode the webhook secret from base64
		// Svix secrets are base64 encoded when stored as whsec_xxx format
		secretKey := webhookSecret
		if strings.HasPrefix(webhookSecret, "whsec_") {
			// Remove the prefix and decode
			encodedSecret := strings.TrimPrefix(webhookSecret, "whsec_")
			decodedSecret, err := base64.StdEncoding.DecodeString(encodedSecret)
			if err == nil {
				secretKey = string(decodedSecret)
			} else {
				logger.Error(ctx, "resend-verification", "Failed to decode webhook secret", err, map[string]interface{}{
					"secret_prefix": webhookSecret[:10] + "...",
				})
			}
		}

		// Compute HMAC-SHA256
		h := hmac.New(sha256.New, []byte(secretKey))
		h.Write([]byte(signedContent))
		expectedMAC := h.Sum(nil)
		expectedSignatureHex := hex.EncodeToString(expectedMAC)
		expectedSignatureBase64 := base64.StdEncoding.EncodeToString(expectedMAC)

		// Log signature comparison for debugging
		logger.Info(ctx, "resend-verification", "Comparing signatures", map[string]interface{}{
			"signature_index": i,
			"received":        parts[1][:20] + "...",
			"expected_hex":    expectedSignatureHex[:20] + "...",
			"expected_b64":    expectedSignatureBase64[:20] + "...",
			"secret_length":   len(secretKey),
			"content_length":  len(signedContent),
		})

		// Compare signatures - try both hex and base64 encoding
		if parts[1] == expectedSignatureHex || parts[1] == expectedSignatureBase64 {
			logger.Info(ctx, "resend-verification", "Signature verified successfully", map[string]interface{}{
				"svix_id": svixID,
			})
			return true, nil
		}
	}

	logger.Error(ctx, "resend-verification", "No matching signature found", nil, map[string]interface{}{
		"svix_id":        svixID,
		"svix_timestamp": svixTimestamp,
		"signatures":     len(signatures),
	})
	return false, fmt.Errorf("no matching signature found")
}

// verifyResendWebhookAndFindUser verifies the webhook and finds the user (MySQL mode)
func verifyResendWebhookAndFindUser(db *sql.DB, body []byte, headers http.Header) (int, string, error) {
	ctx := context.Background()
	logger.Info(ctx, "resend-verification", "Starting MySQL verification", nil)

	rows, err := db.Query(`
		SELECT id, resend_webhook_secret, email, UNIX_TIMESTAMP(created_at), UNIX_TIMESTAMP(updated_at)
		FROM users
		WHERE resend_webhook_secret IS NOT NULL
	`)
	if err != nil {
		logger.Error(ctx, "resend-verification", "MySQL query failed", err)
		return 0, "", fmt.Errorf("database query failed: %v", err)
	}
	defer rows.Close()

	var foundUsers int
	for rows.Next() {
		foundUsers++
		var userID int
		var webhookSecret string
		var email string
		var createdAt, updatedAt int64
		if err := rows.Scan(&userID, &webhookSecret, &email, &createdAt, &updatedAt); err != nil {
			logger.Error(ctx, "resend-verification", "Failed to scan MySQL user row", err)
			continue
		}

		logger.Info(ctx, "resend-verification", "Checking MySQL user", map[string]interface{}{
			"user_id": userID,
			"email":   email,
		})

		// Try to get user data from cache
		cachedUser, cacheHit, err := getUserDataFromCacheResend(ctx, fmt.Sprintf("%d", userID))
		if err == nil && cacheHit {
			webhookSecret = cachedUser.ResendWebhookSecret
			logger.Info(ctx, "resend-verification", "Using cached webhook secret for MySQL user", map[string]interface{}{
				"user_id":   userID,
				"cache_hit": true,
			})
		} else {
			// Cache miss or error, store in cache
			userData := ResendUserData{
				ID:                  int64(userID),
				ResendWebhookSecret: webhookSecret,
				Email:               email,
				CreatedAt:           createdAt,
				UpdatedAt:           updatedAt,
			}
			if err := cacheUserDataResend(ctx, fmt.Sprintf("%d", userID), userData); err != nil {
				logger.Error(ctx, "resend-verification", "Failed to cache MySQL user data", err)
			}
		}

		valid, err := verifyResendWebhookSignature(ctx, webhookSecret, body, headers)
		if err != nil {
			logger.Error(ctx, "resend-verification", "MySQL user verification failed", err)
			continue
		}

		if valid {
			logger.Info(ctx, "resend-verification", "Found valid MySQL user", map[string]interface{}{
				"user_id": userID,
				"email":   email,
			})
			return userID, email, nil
		}
	}

	logger.Info(ctx, "resend-verification", "MySQL verification complete", map[string]interface{}{
		"users_checked": foundUsers,
	})
	return 0, "", fmt.Errorf("no matching user found for the given webhook signature")
}

// verifyResendWebhookAndFindUserDynamoDB verifies the webhook and finds the user (DynamoDB mode)
func verifyResendWebhookAndFindUserDynamoDB(client *dynamodb.Client, body []byte, headers http.Header) (int, string, error) {
	ctx := context.Background()
	logger.Info(ctx, "resend-verification", "Starting DynamoDB verification", nil)

	result, err := client.Scan(context.TODO(), &dynamodb.ScanInput{
		TableName:        aws.String("users"),
		FilterExpression: aws.String("attribute_exists(resend_webhook_secret)"),
	})
	if err != nil {
		logger.Error(ctx, "resend-verification", "DynamoDB scan failed", err)
		return 0, "", fmt.Errorf("dynamodb scan failed: %v", err)
	}

	if len(result.Items) == 0 {
		logger.Error(ctx, "resend-verification", "No users found with Resend webhook secrets", nil)
		return 0, "", fmt.Errorf("no users have resend_webhook_secret configured")
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
			logger.Error(ctx, "resend-verification", "Unexpected ID type for DynamoDB item", nil, map[string]interface{}{
				"user_index": i,
				"id_type":    fmt.Sprintf("%T", item["id"]),
			})
			continue
		}

		webhookSecret := item["resend_webhook_secret"].(*types.AttributeValueMemberS).Value
		email := item["email"].(*types.AttributeValueMemberS).Value

		// Try to get user data from cache
		cachedUser, cacheHit, err := getUserDataFromCacheResend(ctx, userID)
		if err == nil && cacheHit {
			webhookSecret = cachedUser.ResendWebhookSecret
		} else {
			// Cache miss or error, store in cache
			userData := ResendUserData{
				ID:                  parseNumericID(userID),
				ResendWebhookSecret: webhookSecret,
				Email:               email,
				CreatedAt:           parseTimestamp(item["created_at"].(*types.AttributeValueMemberN).Value),
				UpdatedAt:           parseTimestamp(item["updated_at"].(*types.AttributeValueMemberN).Value),
			}
			if err := cacheUserDataResend(ctx, userID, userData); err != nil {
				logger.Error(ctx, "resend-verification", "Failed to cache DynamoDB user data", err)
			}
		}

		valid, err := verifyResendWebhookSignature(ctx, webhookSecret, body, headers)
		if err != nil {
			continue
		}

		if valid {
			userIDInt, err := strconv.Atoi(userID)
			if err != nil {
				logger.Error(ctx, "resend-verification", "Invalid user ID format", err)
				return 0, "", fmt.Errorf("invalid user ID format for %s: %v", userID, err)
			}
			return userIDInt, email, nil
		}
	}

	logger.Error(ctx, "resend-verification", "No matching user found after checking all users", nil)
	return 0, "", fmt.Errorf("no matching user found for the given webhook signature")
}

// ResendUserData represents cached user data for Resend
type ResendUserData struct {
	ID                  int64  `json:"id"`
	ResendWebhookSecret string `json:"resend_webhook_secret"`
	Email               string `json:"email"`
	CreatedAt           int64  `json:"created_at"`
	UpdatedAt           int64  `json:"updated_at"`
}

// getUserDataFromCacheResend attempts to get user data from cache
func getUserDataFromCacheResend(ctx context.Context, userID string) (*ResendUserData, bool, error) {
	var cachedUser ResendUserData
	cacheHit, err := database.GetCachedUserData(userID, &cachedUser)
	if err != nil {
		logger.Info(ctx, "resend-verification", "Cache error for user", nil, map[string]interface{}{
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

// cacheUserDataResend stores user data in cache
func cacheUserDataResend(ctx context.Context, userID string, userData ResendUserData) error {
	if err := database.CacheUserData(userID, userData); err != nil {
		logger.Error(ctx, "resend-verification", "Failed to cache user data", err, map[string]interface{}{
			"user_id": userID,
		})
		return err
	}
	logger.Info(ctx, "resend-verification", "Successfully cached user data", map[string]interface{}{
		"user_id": userID,
	})
	return nil
}