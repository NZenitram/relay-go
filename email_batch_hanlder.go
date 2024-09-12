package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/IBM/sarama"
)

func handleBatchSend(db *sql.DB, userID int, emailMessage EmailPayload) error {
	batchInfo, err := createBatchRecord(db, userID, emailMessage)
	if err != nil {
		return fmt.Errorf("failed to create batch record: %v", err)
	}

	// Queue emails for batch processing
	err = queueEmailsForBatch(*batchInfo, emailMessage)
	if err != nil {
		return fmt.Errorf("failed to queue emails for batch: %v", err)
	}

	return nil
}

func createBatchRecord(db *sql.DB, userID int, emailMessage EmailPayload) (*BatchInfo, error) {

	batchInfo := &BatchInfo{
		UserID:      userID,
		TotalEmails: len(emailMessage.Personalizations),
		CreatedAt:   time.Now().UTC(),
		Status:      "pending",
	}

	dbInsertErr := db.QueryRow(`
        INSERT INTO email_batches (user_id, total_messages, created_at, status)
        VALUES ($1, $2, $3, $4)
        RETURNING id
    `, batchInfo.UserID, batchInfo.TotalEmails, batchInfo.CreatedAt, batchInfo.Status).Scan(&batchInfo.ID)

	if dbInsertErr != nil {
		return nil, fmt.Errorf("failed to insert batch record: %v", dbInsertErr)
	}

	return batchInfo, nil
}

func queueEmailsForBatch(batchInfo BatchInfo, emailMessage EmailPayload) error {
	kafkaBrokers := []string{os.Getenv("KAFKA_BROKERS")}
	if len(kafkaBrokers) == 0 {
		log.Fatal("KAFKA_BROKERS environment variable is not set")
	}
	// Set up the Kafka producer
	producer, err := sarama.NewSyncProducer(kafkaBrokers, nil)
	if err != nil {
		log.Fatalf("Failed to start Kafka producer: %v", err)
	}
	defer producer.Close()

	for _, p := range emailMessage.Personalizations {
		batchEmail := struct {
			BatchInfo       BatchInfo
			Personalization Personalization
			From            EmailAddress
			Content         []Content
			Attachments     []Attachment
			Headers         map[string]string
			Sections        map[string]string
			Categories      []string
		}{
			BatchInfo:       batchInfo,
			Personalization: p,
			From:            emailMessage.From,
			Content:         emailMessage.Content,
			Attachments:     emailMessage.Attachments,
			Headers:         emailMessage.Headers,
			Sections:        emailMessage.Sections,
			Categories:      emailMessage.Categories,
		}

		batchEmailJSON, err := json.Marshal(batchEmail)
		if err != nil {
			return err
		}

		_, _, err = producer.SendMessage(&sarama.ProducerMessage{
			Topic: "batch_emails",
			Value: sarama.StringEncoder(batchEmailJSON),
		})
		if err != nil {
			return err
		}
	}

	return nil
}
