package main

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
)

type User struct {
	ID     int
	APIKey string
}

func validateAPIKey(db *sql.DB, apiKey string) (*User, error) {
	var user User
	err := db.QueryRow("SELECT id, api_key FROM users WHERE api_key = $1", apiKey).Scan(&user.ID, &user.APIKey)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, errors.New("invalid API key")
		}
		return nil, fmt.Errorf("database query failed: %v", err)
	}

	return &user, nil
}

func storeEmailRequest(db *sql.DB, userID int, messageID string) error {
	_, err := db.Exec(`
        INSERT INTO email_requests (user_id, message_id)
        VALUES ($1, $2)
    `, userID, messageID)

	if err != nil {
		return fmt.Errorf("failed to store email request: %v", err)
	}

	return nil
}

func logInvalidAttempt(apiKey string) {
	log.Printf("WARNING: Invalid API Key attempt: %s", apiKey)
}
