package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/IBM/sarama"
	"github.com/go-redis/redis/v8"
)

type BatchProcessor struct {
	db    *sql.DB
	redis *redis.Client
	kafka sarama.SyncProducer
	ctx   context.Context
}

func NewBatchProcessor(db *sql.DB, redisAddr string, redisPass string, kafkaBrokers []string) (*BatchProcessor, error) {
	ctx := context.Background()

	redisClient := redis.NewClient(&redis.Options{
		Addr:     redisAddr,
		Password: redisPass,
	})

	kafkaConfig := sarama.NewConfig()
	kafkaConfig.Producer.Return.Successes = true
	producer, err := sarama.NewSyncProducer(kafkaBrokers, kafkaConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka producer: %v", err)
	}

	return &BatchProcessor{
		db:    db,
		redis: redisClient,
		kafka: producer,
		ctx:   ctx,
	}, nil
}

func (bp *BatchProcessor) HandleBatchSend(userID int, messagePayload Message) error {

	var emailPayload EmailPayload
	err := json.Unmarshal(messagePayload.Body, &emailPayload)
	if err != nil {
		log.Printf("Failed to Unmarshal MessagePayload.Body to EmailPayload: %v", err)
	}

	batchSize, _ := strconv.Atoi(emailPayload.CustomArgs["BatchSize"])
	batchInterval, _ := strconv.Atoi(emailPayload.CustomArgs["BatchInterval"])
	totalPersonalizations := len(emailPayload.Personalizations)

	schedule := calculateBatchSchedule(totalPersonalizations, batchSize, batchInterval)

	batchID, err := bp.createBatchRecord(userID, emailPayload, len(schedule))
	if err != nil {
		return fmt.Errorf("failed to create batch record: %v", err)
	}

	err = bp.storeEmailsInRedis(batchID, messagePayload, batchSize, len(schedule))
	if err != nil {
		return fmt.Errorf("failed to store emails in Redis: %v", err)
	}

	err = bp.scheduleEmailSending(batchID, schedule, batchInterval)
	if err != nil {
		return fmt.Errorf("failed to schedule email sending: %v", err)
	}

	return nil
}

func (bp *BatchProcessor) createBatchRecord(userID int, emailPayload EmailPayload, totalBatches int) (int, error) {
	batchSize := emailPayload.CustomArgs["BatchSize"]
	intervalSeconds := emailPayload.CustomArgs["BatchInterval"]
	var batchID int
	err := bp.db.QueryRow(`
		INSERT INTO email_batches (user_id, total_messages, batch_size, interval_seconds, total_batches, created_at, status)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		RETURNING batch_id
	`, userID, len(emailPayload.Personalizations), batchSize, intervalSeconds, totalBatches, time.Now().UTC(), "pending").Scan(&batchID)

	if err != nil {
		return 0, fmt.Errorf("failed to insert batch record: %v", err)
	}

	return batchID, nil
}

func (bp *BatchProcessor) storeEmailsInRedis(batchID int, messagePayload Message, batchSize, totalBatches int) error {
	batchKey := fmt.Sprintf("batch:%d", batchID)
	var emailPayload EmailPayload
	err := json.Unmarshal(messagePayload.Body, &emailPayload)
	if err != nil {
		log.Printf("Failed to Unmarshal MessagePayload.Body to EmailPayload: %v", err)
	}

	commonDataJSON, err := json.Marshal(messagePayload.Body)
	if err != nil {
		return fmt.Errorf("failed to marshal common email data: %v", err)
	}

	err = bp.redis.HSet(bp.ctx, batchKey, "common", commonDataJSON).Err()
	if err != nil {
		return fmt.Errorf("failed to store common email data in Redis: %v", err)
	}

	// Message and User ID
	err = bp.redis.HSet(bp.ctx, batchKey, "user_id", messagePayload.UserID, "msg_id", messagePayload.MessageID).Err()
	if err != nil {
		return fmt.Errorf("failed to store Message and User ID in Redis: %v", err)
	}
	// Store batch information
	err = bp.redis.HSet(bp.ctx, batchKey, "batch_size", batchSize, "total_batches", totalBatches).Err()
	if err != nil {
		return fmt.Errorf("failed to store batch information in Redis: %v", err)
	}

	// Store individual personalizations
	for i, p := range emailPayload.Personalizations {
		personalizationKey := fmt.Sprintf("p:%d", i)
		personalizationJSON, err := json.Marshal(p)
		if err != nil {
			return fmt.Errorf("failed to marshal personalization data: %v", err)
		}

		err = bp.redis.HSet(bp.ctx, batchKey, personalizationKey, personalizationJSON).Err()
		if err != nil {
			return fmt.Errorf("failed to store personalization in Redis: %v", err)
		}
	}

	return nil
}

func (bp *BatchProcessor) scheduleEmailSending(batchID int, schedule []int, batchInterval int) error {
	now := time.Now()
	for i, batchSize := range schedule {
		sendTime := now.Add(time.Duration(i*batchInterval) * time.Second)

		// Create a struct to hold the batch ID, size, and index
		batchInfo := struct {
			BatchID    int `json:"batch_id"`
			BatchSize  int `json:"batch_size"`
			BatchIndex int `json:"batch_index"`
		}{
			BatchID:    batchID,
			BatchSize:  batchSize,
			BatchIndex: i,
		}

		// Marshal the struct to JSON
		batchInfoJSON, err := json.Marshal(batchInfo)
		if err != nil {
			return fmt.Errorf("failed to marshal batch info: %v", err)
		}

		// Generate a unique key for each scheduled job
		jobKey := fmt.Sprintf("scheduled_email:%d:%d", batchID, i)

		// Use the JSON string as the member in the sorted set
		err = bp.redis.ZAdd(bp.ctx, "scheduled_emails", &redis.Z{
			Score:  float64(sendTime.Unix()),
			Member: jobKey,
		}).Err()
		if err != nil {
			return fmt.Errorf("failed to schedule email sending: %v", err)
		}

		// Store the batch info in a separate hash
		err = bp.redis.HSet(bp.ctx, jobKey, map[string]interface{}{
			"info": string(batchInfoJSON),
		}).Err()
		if err != nil {
			return fmt.Errorf("failed to store batch info: %v", err)
		}
	}

	return nil
}

func (bp *BatchProcessor) ProcessScheduledEmails() {
	log.Println("Debug: Starting ProcessScheduledEmails")
	for {
		// log.Println("Debug: Checking for due emails")
		// Check for due emails every minute
		time.Sleep(1 * time.Minute)

		now := time.Now().Unix()
		// log.Printf("Debug: Current timestamp: %d", now)

		batchInfos, err := bp.redis.ZRangeByScore(bp.ctx, "scheduled_emails", &redis.ZRangeBy{
			Min: "0",
			Max: fmt.Sprintf("%d", now),
		}).Result()

		if err != nil {
			log.Printf("Error: Failed to fetch due emails: %v", err)
			continue
		}

		// log.Printf("Debug: Found %d due emails", len(batchInfos))

		for i, batchInfoJSON := range batchInfos {
			log.Printf("Debug: Processing batch info %d: %s", i, batchInfoJSON)

			// Attempt to retrieve the actual batch info from the hash
			jobKey := batchInfoJSON // batchInfoJSON is actually the jobKey in this case
			actualBatchInfo, err := bp.redis.HGet(bp.ctx, jobKey, "info").Result()
			if err != nil {
				log.Printf("Error: Failed to retrieve batch info for key %s: %v", jobKey, err)
				continue
			}

			// log.Printf("Debug: Retrieved actual batch info: %s", actualBatchInfo)

			var batchInfo struct {
				BatchID    int `json:"batch_id"`
				BatchSize  int `json:"batch_size"`
				BatchIndex int `json:"batch_index"`
			}

			err = json.Unmarshal([]byte(actualBatchInfo), &batchInfo)
			if err != nil {
				log.Printf("Error: Failed to unmarshal batch info: %v", err)
				log.Printf("Debug: Problematic JSON: %s", actualBatchInfo)
				continue
			}

			// log.Printf("Debug: Unmarshaled batch info: %+v", batchInfo)

			// log.Printf("Debug: Processing batch ID %d, size %d, index %d", batchInfo.BatchID, batchInfo.BatchSize, batchInfo.BatchIndex)
			bp.processBatch(batchInfo.BatchID, batchInfo.BatchSize)

			// log.Printf("Debug: Removing processed batch from scheduled_emails")
			removeResult, err := bp.redis.ZRem(bp.ctx, "scheduled_emails", jobKey).Result()
			if err != nil {
				log.Printf("Error: Failed to remove processed batch from scheduled_emails: %v", err)
			} else {
				log.Printf("Debug: Removed %d entries from scheduled_emails", removeResult)
			}

			// Clean up the hash entry
			deleteResult, err := bp.redis.Del(bp.ctx, jobKey).Result()
			if err != nil {
				log.Printf("Error: Failed to delete hash entry for %s: %v", jobKey, err)
			} else {
				log.Printf("Debug: Deleted %d hash entries for %s", deleteResult, jobKey)
			}
		}
	}
}

func (bp *BatchProcessor) processBatch(batchID int, batchSize int) {
	log.Printf("Debug: Starting to process batch %d with size %d", batchID, batchSize)
	batchKey := fmt.Sprintf("batch:%d", batchID)

	// Fetch common data
	commonDataJSON, err := bp.redis.HGet(bp.ctx, batchKey, "common").Result()
	if err != nil {
		log.Printf("Failed to fetch common email data for batch %d: %v", batchID, err)
		return
	}

	var emailPayload EmailPayload
	err = json.Unmarshal([]byte(commonDataJSON), &emailPayload)
	if err != nil {
		log.Printf("Failed to unmarshal common email data for batch %d: %v", batchID, err)
		return
	}

	// Fetch the current batch index
	currentBatchIndex, err := bp.redis.HGet(bp.ctx, batchKey, "current_batch_index").Int()
	if err != nil && err != redis.Nil {
		log.Printf("Failed to fetch current batch index for batch %d: %v", batchID, err)
		return
	}

	// Fetch Message ID
	messageID, err := bp.redis.HGet(bp.ctx, batchKey, "msg_id").Result()
	if err != nil && err != redis.Nil {
		log.Printf("Failed to fetch MessageID for batch %d: %v", messageID, err)
		return
	}

	// Fetch User ID
	userID, err := bp.redis.HGet(bp.ctx, batchKey, "user_id").Int()
	if err != nil && err != redis.Nil {
		log.Printf("Failed to fetch user ID for batch %d: %v", batchID, err)
		return
	}

	// Fetch all personalization keys
	allKeys, err := bp.redis.HKeys(bp.ctx, batchKey).Result()
	if err != nil {
		log.Printf("Failed to fetch keys for batch %d: %v", batchID, err)
		return
	}

	// Filter and sort personalization keys
	var pKeys []string
	for _, key := range allKeys {
		if strings.HasPrefix(key, "p:") {
			pKeys = append(pKeys, key)
		}
	}
	sort.Strings(pKeys)

	// log.Printf("Debug: Found %d personalization keys for batch %d", len(pKeys), batchID)

	// Calculate start and end indices for this batch
	numLoops := batchSize
	if len(pKeys) < numLoops {
		numLoops = len(pKeys)
	}

	// Process personalizations for this batch
	var emptyPersonalization []Personalization
	emailPayload.Personalizations = emptyPersonalization

	for i := 0; i < numLoops; i++ {
		personalizationKey := pKeys[i]
		// log.Printf("Debug: Processing key %s for batch %d", personalizationKey, batchID)

		personalizationJSON, err := bp.redis.HGet(bp.ctx, batchKey, personalizationKey).Result()
		if err != nil {
			log.Printf("Failed to fetch personalization data for batch %d, key %s: %v", batchID, personalizationKey, err)
			continue
		}

		var personalization Personalization
		err = json.Unmarshal([]byte(personalizationJSON), &personalization)
		if err != nil {
			log.Printf("Failed to unmarshal personalization data for batch %d, key %s: %v", batchID, personalizationKey, err)
			continue
		}

		emailPayload.Personalizations = append(emailPayload.Personalizations, personalization)

		// Remove processed personalization from Redis
		bp.redis.HDel(bp.ctx, batchKey, personalizationKey)
	}

	kafkaMessage := kafkaPayload{
		BatchID:   batchID,
		MessageID: messageID,
		UserID:    userID,
		Body:      emailPayload,
	}

	// Send to Kafka for processing
	emailPayloadJSON, err := json.Marshal(kafkaMessage)
	if err != nil {
		log.Printf("Failed to marshal email payload for batch %d, key %s: %v", batchID, err)
	}

	_, _, err = bp.kafka.SendMessage(&sarama.ProducerMessage{
		Topic: "emails",
		Key:   sarama.StringEncoder(fmt.Sprintf("%d", batchID)),
		Value: sarama.StringEncoder(emailPayloadJSON),
	})
	if err != nil {
		log.Printf("Failed to send message to Kafka for batch %d, key %s: %v", batchID, err)
	}

	newBatchIndex := currentBatchIndex + 1

	// Update the database after processing all due emails in this loop
	_, err = bp.db.ExecContext(bp.ctx, `
        UPDATE email_batches 
        	SET batches_to_kafka = $1, 
            updated_at = CURRENT_TIMESTAMP 
       		WHERE batch_id = $2`, newBatchIndex, batchID)
	if err != nil {
		log.Printf("Failed to update processed_emails for batch %d: %v", batchID, err)
	}
	// Update the current batch index
	err = bp.redis.HSet(bp.ctx, batchKey, "current_batch_index", newBatchIndex).Err()
	if err != nil {
		log.Printf("Failed to update current batch index for batch %d: %v", batchID, err)
	}

	// Check if this was the last batch
	if numLoops == len(pKeys) {
		// Update batch status to completed
		// _, err = bp.db.ExecContext(bp.ctx, `
		// UPDATE email_batches
		// 	SET status = 'completed',
		// 	updated_at = CURRENT_TIMESTAMP
		// 	WHERE batch_id = $1`, batchID)
		// if err != nil {
		// 	log.Printf("Failed to update batch status to completed for batch %d: %v", batchID, err)
		// }
		// This was the last batch, clean up Redis
		bp.redis.Del(bp.ctx, batchKey)
		log.Printf("Batch %d completed and cleaned up", batchID)
	}

	// log.Printf("Debug: Finished processing batch %d", batchID)
}

func calculateBatchSchedule(totalPersonalizations, batchSize, batchIntervalSeconds int) []int {
	if batchSize <= 0 || batchIntervalSeconds <= 0 {
		return nil
	}

	totalBatches := (totalPersonalizations + batchSize - 1) / batchSize
	schedule := make([]int, totalBatches)

	for i := 0; i < totalBatches; i++ {
		if i == totalBatches-1 {
			schedule[i] = totalPersonalizations - (i * batchSize)
		} else {
			schedule[i] = batchSize
		}
	}

	return schedule
}
