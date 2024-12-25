package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/sendgrid/sendgrid-go/helpers/eventwebhook"
)

type SendgridWebhookPayload struct {
	Headers map[string][]string `json:"headers"`
	Body    json.RawMessage     `json:"body"`
}

func verifySendgridWebhookAndFindUser(db *sql.DB, body []byte, headers http.Header) (int, error) {
	// Short-circuit for development purposes if user ID is 1
	if devMode := os.Getenv("DEV_MODE"); devMode == "true" {
		return 1, nil
	}

	signature := headers.Get("X-Twilio-Email-Event-Webhook-Signature")
	timestamp := headers.Get("X-Twilio-Email-Event-Webhook-Timestamp")

	rows, err := db.Query("SELECT user_id, sendgrid_verification_key FROM email_service_providers WHERE provider_name = 'sendgrid' AND sendgrid_verification_key IS NOT NULL")
	if err != nil {
		return 0, fmt.Errorf("failed to query database: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var userID int
		var publicKey string
		err := rows.Scan(&userID, &publicKey)
		if err != nil {
			return 0, fmt.Errorf("failed to scan row: %v", err)
		}

		ecdaKey, err := eventwebhook.ConvertPublicKeyBase64ToECDSA(publicKey)
		if err != nil {
			log.Printf("Cannot convert public kye for user ID %s: %v", err, userID)
			continue
		}

		valid, err := eventwebhook.VerifySignature(ecdaKey, body, signature, timestamp)
		if err != nil {
			log.Printf("Signature verification failed for user ID %s: %v", err, userID)
			continue
		}

		if valid {
			return userID, nil
		}
	}

	return 0, fmt.Errorf("no matching user found for the given webhook signature: %v", signature)
}

func associateSendgridEventWithUser(db *sql.DB, sgEventBody SendGridEventBody, userID int) error {
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
