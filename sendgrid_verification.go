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

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/sendgrid/sendgrid-go/helpers/eventwebhook"
)

type DynamoDBUser struct {
	ID                      string `json:"id" dynamodbav:"id"`
	SendGridVerificationKey string `json:"sendgrid_verification_key" dynamodbav:"sendgrid_verification_key"`
	Email                   string `json:"email" dynamodbav:"email"`
	CreatedAt               int64  `json:"created_at" dynamodbav:"created_at"`
	UpdatedAt               int64  `json:"updated_at" dynamodbav:"updated_at"`
}

type SendgridWebhookPayload struct {
	Headers map[string][]string `json:"headers"`
	Body    json.RawMessage     `json:"body"`
}

func isMySQLAvailable(db *sql.DB) bool {
	err := db.Ping()
	return err == nil
}

func verifySendgridWebhookAndFindUser(db *sql.DB, body []byte, headers http.Header) (int, error) {
	signature := headers.Get("X-Twilio-Email-Event-Webhook-Signature")
	timestamp := headers.Get("X-Twilio-Email-Event-Webhook-Timestamp")

	if signature == "" || timestamp == "" {
		return 0, fmt.Errorf("missing webhook signature or timestamp")
	}

	// Query all users with SendGrid verification keys
	rows, err := db.Query(`
		SELECT id, sendgrid_verification_key
		FROM users
		WHERE sendgrid_verification_key IS NOT NULL
	`)
	if err != nil {
		return 0, fmt.Errorf("database query failed: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var userID int
		var verificationKey string
		if err := rows.Scan(&userID, &verificationKey); err != nil {
			log.Printf("Failed to scan user row: %v", err)
			continue
		}

		ecdaKey, err := eventwebhook.ConvertPublicKeyBase64ToECDSA(verificationKey)
		if err != nil {
			log.Printf("Cannot convert public key for user ID %d: %v", userID, err)
			continue
		}

		valid, err := eventwebhook.VerifySignature(ecdaKey, body, signature, timestamp)
		if err != nil {
			log.Printf("Signature verification failed for user ID %d: %v", userID, err)
			continue
		}

		if valid {
			return userID, nil
		}
	}

	return 0, fmt.Errorf("no matching user found for the given webhook signature: %v", signature)
}

func verifySendgridWebhookAndFindUserDynamoDB(client *dynamodb.Client, body []byte, headers http.Header) (int, error) {
	signature := headers.Get("X-Twilio-Email-Event-Webhook-Signature")
	timestamp := headers.Get("X-Twilio-Email-Event-Webhook-Timestamp")

	if signature == "" || timestamp == "" {
		return 0, fmt.Errorf("missing webhook signature or timestamp")
	}

	// Query all users with SendGrid verification keys
	result, err := client.Scan(context.TODO(), &dynamodb.ScanInput{
		TableName:        aws.String("users"),
		FilterExpression: aws.String("attribute_exists(sendgrid_verification_key)"),
	})
	if err != nil {
		return 0, fmt.Errorf("dynamodb scan failed: %v", err)
	}

	for _, item := range result.Items {
		userIDStr := item["id"].(*types.AttributeValueMemberS).Value
		verificationKey := item["sendgrid_verification_key"].(*types.AttributeValueMemberS).Value

		// Try to get user data from cache first
		var cachedUser DynamoDBUser
		cacheHit, err := database.GetCachedUserData(userIDStr, &cachedUser)
		if err != nil {
			log.Printf("Cache error for user ID %s: %v", userIDStr, err)
			// Continue with DynamoDB data if cache fails
		} else if cacheHit {
			log.Printf("Cache hit for user ID %s", userIDStr)
			// Use cached verification key
			verificationKey = cachedUser.SendGridVerificationKey
		} else {
			log.Printf("Cache miss for user ID %s, storing in cache", userIDStr)
			// Cache miss, store user data in cache
			userData := DynamoDBUser{
				ID:                      userIDStr,
				SendGridVerificationKey: verificationKey,
				Email:                   item["email"].(*types.AttributeValueMemberS).Value,
				CreatedAt:               parseTimestamp(item["created_at"].(*types.AttributeValueMemberN).Value),
				UpdatedAt:               parseTimestamp(item["updated_at"].(*types.AttributeValueMemberN).Value),
			}
			if err := database.CacheUserData(userIDStr, userData); err != nil {
				log.Printf("Failed to cache user data for ID %s: %v", userIDStr, err)
				// Continue with DynamoDB data if caching fails
			} else {
				log.Printf("Successfully cached user data for ID %s", userIDStr)
			}
		}

		ecdaKey, err := eventwebhook.ConvertPublicKeyBase64ToECDSA(verificationKey)
		if err != nil {
			log.Printf("Cannot convert public key for user ID %s: %v", userIDStr, err)
			continue
		}

		valid, err := eventwebhook.VerifySignature(ecdaKey, body, signature, timestamp)
		if err != nil {
			log.Printf("Signature verification failed for user ID %s: %v", userIDStr, err)
			continue
		}

		if valid {
			// Convert string ID to int
			var userID int
			_, err = fmt.Sscanf(userIDStr, "%d", &userID)
			if err != nil {
				log.Printf("Invalid user ID format for %s: %v", userIDStr, err)
				continue
			}
			return userID, nil
		}
	}

	return 0, fmt.Errorf("no matching user found for the given webhook signature: %v", signature)
}

func associateSendgridEventWithUser(db *sql.DB, sgEventBody SendGridEventBody, userID int) error {
	// Only attempt MySQL operations if MySQL is available
	if !isMySQLAvailable(db) {
		return nil
	}

	// Insert the association into the message_user_associations table
	_, err := db.Exec(`
		INSERT INTO message_user_associations (message_id, user_id, esp_id, provider)
		SELECT ?, ?, esp_id, 'sendgrid'
		FROM email_service_providers
		WHERE user_id = ? AND provider_name = 'sendgrid'
		ON DUPLICATE KEY UPDATE id = id
`, sgEventBody.SGMessageID, userID, userID)
	if err != nil {
		return fmt.Errorf("failed to insert message association: %v", err)
	}

	return nil
}

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
