package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"relay-go/m/logger"
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

	result, err := bp.db.Exec(`
        INSERT INTO email_batches (user_id, total_messages, batch_size, interval_seconds, total_batches, created_at, status)
        VALUES (?, ?, ?, ?, ?, NOW(), ?)
    `, userID, len(emailPayload.Personalizations), batchSize, intervalSeconds, totalBatches, "pending")

	if err != nil {
		return 0, fmt.Errorf("failed to insert batch record: %v", err)
	}

	batchID, err := result.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("failed to get last insert ID: %v", err)
	}

	return int(batchID), nil
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
	ctx := context.Background()
	logger.Info(ctx, "batch-processor", "Starting ProcessScheduledEmails")
	for {
		time.Sleep(1 * time.Minute)

		now := time.Now().Unix()

		batchInfos, err := bp.redis.ZRangeByScore(bp.ctx, "scheduled_emails", &redis.ZRangeBy{
			Min: "0",
			Max: fmt.Sprintf("%d", now),
		}).Result()

		if err != nil {
			logger.Error(ctx, "batch-processor", "Failed to fetch due emails", err)
			continue
		}

		for i, batchInfoJSON := range batchInfos {
			logger.Info(ctx, "batch-processor", "Processing batch info", map[string]interface{}{
				"batch_index": i,
				"batch_info":  batchInfoJSON,
			})

			jobKey := batchInfoJSON
			actualBatchInfo, err := bp.redis.HGet(bp.ctx, jobKey, "info").Result()
			if err != nil {
				logger.Error(ctx, "batch-processor", "Failed to retrieve batch info", err, map[string]interface{}{
					"job_key": jobKey,
				})
				continue
			}

			var batchInfo struct {
				BatchID    int `json:"batch_id"`
				BatchSize  int `json:"batch_size"`
				BatchIndex int `json:"batch_index"`
			}

			err = json.Unmarshal([]byte(actualBatchInfo), &batchInfo)
			if err != nil {
				logger.Error(ctx, "batch-processor", "Failed to unmarshal batch info", err, map[string]interface{}{
					"batch_info_json": actualBatchInfo,
				})
				continue
			}

			bp.processBatch(batchInfo.BatchID, batchInfo.BatchSize)

			removeResult, err := bp.redis.ZRem(bp.ctx, "scheduled_emails", jobKey).Result()
			if err != nil {
				logger.Error(ctx, "batch-processor", "Failed to remove processed batch from scheduled_emails", err)
			} else {
				logger.Info(ctx, "batch-processor", "Removed entries from scheduled_emails", map[string]interface{}{
					"removed_count": removeResult,
				})
			}

			deleteResult, err := bp.redis.Del(bp.ctx, jobKey).Result()
			if err != nil {
				logger.Error(ctx, "batch-processor", "Failed to delete hash entry", err, map[string]interface{}{
					"job_key": jobKey,
				})
			} else {
				logger.Info(ctx, "batch-processor", "Deleted hash entries", map[string]interface{}{
					"deleted_count": deleteResult,
					"job_key":       jobKey,
				})
			}
		}
	}
}

func (bp *BatchProcessor) processBatch(batchID int, batchSize int) {
	ctx := context.Background()
	logger.Info(ctx, "batch-processor", "Starting to process batch", map[string]interface{}{
		"batch_id":   batchID,
		"batch_size": batchSize,
	})

	batchKey := fmt.Sprintf("batch:%d", batchID)

	commonDataJSON, err := bp.redis.HGet(bp.ctx, batchKey, "common").Result()
	if err != nil {
		logger.Error(ctx, "batch-processor", "Failed to fetch common email data", err, map[string]interface{}{
			"batch_id": batchID,
		})
		return
	}

	var emailPayload EmailPayload
	err = json.Unmarshal([]byte(commonDataJSON), &emailPayload)
	if err != nil {
		logger.Error(ctx, "batch-processor", "Failed to unmarshal common email data", err, map[string]interface{}{
			"batch_id": batchID,
		})
		return
	}

	currentBatchIndex, err := bp.redis.HGet(bp.ctx, batchKey, "current_batch_index").Int()
	if err != nil && err != redis.Nil {
		logger.Error(ctx, "batch-processor", "Failed to fetch current batch index", err, map[string]interface{}{
			"batch_id": batchID,
		})
		return
	}

	messageID, err := bp.redis.HGet(bp.ctx, batchKey, "msg_id").Result()
	if err != nil && err != redis.Nil {
		logger.Error(ctx, "batch-processor", "Failed to fetch MessageID", err, map[string]interface{}{
			"batch_id": batchID,
		})
		return
	}

	userID, err := bp.redis.HGet(bp.ctx, batchKey, "user_id").Int()
	if err != nil && err != redis.Nil {
		logger.Error(ctx, "batch-processor", "Failed to fetch user ID", err, map[string]interface{}{
			"batch_id": batchID,
		})
		return
	}

	allKeys, err := bp.redis.HKeys(bp.ctx, batchKey).Result()
	if err != nil {
		logger.Error(ctx, "batch-processor", "Failed to fetch keys", err, map[string]interface{}{
			"batch_id": batchID,
		})
		return
	}

	var pKeys []string
	for _, key := range allKeys {
		if strings.HasPrefix(key, "p:") {
			pKeys = append(pKeys, key)
		}
	}
	sort.Strings(pKeys)

	numLoops := batchSize
	if len(pKeys) < numLoops {
		numLoops = len(pKeys)
	}

	var emptyPersonalization []Personalization
	emailPayload.Personalizations = emptyPersonalization

	for i := 0; i < numLoops; i++ {
		personalizationKey := pKeys[i]

		personalizationJSON, err := bp.redis.HGet(bp.ctx, batchKey, personalizationKey).Result()
		if err != nil {
			logger.Error(ctx, "batch-processor", "Failed to fetch personalization data", err, map[string]interface{}{
				"batch_id": batchID,
				"key":      personalizationKey,
			})
			continue
		}

		var personalization Personalization
		err = json.Unmarshal([]byte(personalizationJSON), &personalization)
		if err != nil {
			logger.Error(ctx, "batch-processor", "Failed to unmarshal personalization data", err, map[string]interface{}{
				"batch_id": batchID,
				"key":      personalizationKey,
			})
			continue
		}

		emailPayload.Personalizations = append(emailPayload.Personalizations, personalization)
		bp.redis.HDel(bp.ctx, batchKey, personalizationKey)
	}

	kafkaMessage := kafkaPayload{
		BatchID:   batchID,
		MessageID: messageID,
		UserID:    userID,
		Body:      emailPayload,
	}

	emailPayloadJSON, err := json.Marshal(kafkaMessage)
	if err != nil {
		logger.Error(ctx, "batch-processor", "Failed to marshal email payload", err, map[string]interface{}{
			"batch_id": batchID,
		})
	}

	_, _, err = bp.kafka.SendMessage(&sarama.ProducerMessage{
		Topic: "emails",
		Key:   sarama.StringEncoder(fmt.Sprintf("%d", batchID)),
		Value: sarama.StringEncoder(emailPayloadJSON),
	})
	if err != nil {
		logger.Error(ctx, "batch-processor", "Failed to send message to Kafka", err, map[string]interface{}{
			"batch_id": batchID,
		})
	}

	newBatchIndex := currentBatchIndex + 1

	_, err = bp.db.ExecContext(bp.ctx, `
    UPDATE email_batches 
    SET batches_to_kafka = ?, 
        updated_at = CURRENT_TIMESTAMP 
    WHERE batch_id = ?`, newBatchIndex, batchID)
	if err != nil {
		logger.Error(ctx, "batch-processor", "Failed to update processed_emails", err, map[string]interface{}{
			"batch_id": batchID,
		})
	}

	err = bp.redis.HSet(bp.ctx, batchKey, "current_batch_index", newBatchIndex).Err()
	if err != nil {
		logger.Error(ctx, "batch-processor", "Failed to update current batch index", err, map[string]interface{}{
			"batch_id": batchID,
		})
	}

	if numLoops == len(pKeys) {
		bp.redis.Del(bp.ctx, batchKey)
		logger.Info(ctx, "batch-processor", "Batch completed and cleaned up", map[string]interface{}{
			"batch_id": batchID,
		})
	}
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
