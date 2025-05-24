package database

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"relay-go/m/logger"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/redis/go-redis/v9"
)

// BatchConfig holds configuration for event batching
type BatchConfig struct {
	MaxSize         int           // Maximum number of events per batch
	MaxBytes        int           // Maximum bytes per batch
	FlushInterval   time.Duration // How often to flush regardless of size
	Provider        string        // Email provider (sendgrid, sparkpost, etc.)
	MemoryThreshold int           // Percentage of Redis memory usage that triggers a forced commit
}

// BatchMetadata contains information about a batch of events
type BatchMetadata struct {
	BatchID    string    `json:"batch_id"`
	Provider   string    `json:"provider"`
	Timestamp  time.Time `json:"timestamp"`
	EventCount int       `json:"event_count"`
	FileSize   int64     `json:"file_size"`
	Checksum   string    `json:"checksum"`
	Status     string    `json:"status"`
}

// EventBatch represents a batch of events for S3 storage
type EventBatch struct {
	BatchID   string            `json:"batch_id"`
	Provider  string            `json:"provider"`
	Timestamp time.Time         `json:"timestamp"`
	Events    []json.RawMessage `json:"events"` // Changed to store raw JSON
	Metadata  BatchMetadata     `json:"metadata"`
}

// WebhookEvent represents a single webhook event
type WebhookEvent struct {
	// Core fields
	EventID   string    `json:"event_id"`  // Unique identifier for the event
	EventType string    `json:"event"`     // Type of event (delivered, opened, clicked, etc.)
	Timestamp time.Time `json:"timestamp"` // When the event occurred
	Provider  string    `json:"provider"`  // Email provider (sendgrid, sparkpost, postmark)

	// User and recipient information
	UserID   int64  `json:"user_id"`     // ID of the user who sent the email
	Email    string `json:"email"`       // Recipient email address
	Username string `json:"sh_username"` // Username of the sender

	// Message tracking
	MessageID string `json:"message_id"` // Provider's message ID
	SMTPID    string `json:"smtp_id"`    // SMTP ID if available

	// Event details
	Categories           []string `json:"categories"`            // Categories/tags associated with the event
	Reason               string   `json:"reason"`                // Reason for bounces/failures
	ErrorCode            string   `json:"error_code"`            // Error code if applicable
	BounceType           string   `json:"bounce_type"`           // Type of bounce (hard, soft, etc.)
	BounceClassification string   `json:"bounce_classification"` // Detailed bounce classification

	// Location and device info
	IPAddress string `json:"ip_address"` // IP address of the event
	UserAgent string `json:"user_agent"` // User agent if available
	GeoIP     *GeoIP `json:"geo_ip"`     // Geographic information
}

// GeoIP represents geographic information
type GeoIP struct {
	Country    string  `json:"country"`
	Region     string  `json:"region"`
	City       string  `json:"city"`
	Latitude   float64 `json:"latitude"`
	Longitude  float64 `json:"longitude"`
	PostalCode string  `json:"postal_code"`
}

// EventBatcher handles batching of webhook events for S3 storage
type EventBatcher struct {
	redisClient *redis.Client
	s3Client    *s3.Client
	config      BatchConfig
	bucketName  string
}

var (
	eventBatcher     *EventBatcher
	eventBatcherOnce sync.Once
)

// GetEventBatcher returns the singleton instance of EventBatcher
func GetEventBatcher() *EventBatcher {
	eventBatcherOnce.Do(func() {
		redisClient := redis.NewClient(&redis.Options{
			Addr: os.Getenv("REDIS_HOST"),
		})

		var awsCfg aws.Config
		var err error
		if os.Getenv("DEV_MODE") == "true" {
			awsCfg, err = config.LoadDefaultConfig(context.Background(),
				config.WithRegion(os.Getenv("AWS_REGION")),
				config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
					os.Getenv("AWS_ACCESS_KEY_ID"),
					os.Getenv("AWS_SECRET_ACCESS_KEY"),
					os.Getenv("AWS_SESSION_TOKEN"),
				)),
			)
		} else {
			awsCfg, err = config.LoadDefaultConfig(context.Background())
		}
		if err != nil {
			logger.Fatal(nil, "init", "Failed to load AWS config", err)
		}

		// Create S3 client with custom endpoint resolver
		s3Client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
			o.UsePathStyle = true // Use path-style addressing
		})

		// Default to production settings
		batcherConfig := BatchConfig{
			MaxSize:         1000,
			MaxBytes:        5 * 1024 * 1024, // 5MB
			FlushInterval:   5 * time.Minute,
			Provider:        "sendgrid",
			MemoryThreshold: 80,
		}

		// Override with development settings if DEV_MODE is set
		if os.Getenv("DEV_MODE") == "true" {
			batcherConfig = BatchConfig{
				MaxSize:         10,               // Smaller batches for faster testing
				MaxBytes:        1024 * 1024,      // 1MB
				FlushInterval:   30 * time.Second, // Flush every 30 seconds
				Provider:        "sendgrid",
				MemoryThreshold: 50, // Lower memory threshold for development
			}
			logger.Info(nil, "event-batcher", "Using development batch configuration", map[string]interface{}{
				"max_size":         batcherConfig.MaxSize,
				"max_bytes":        batcherConfig.MaxBytes,
				"flush_interval":   batcherConfig.FlushInterval,
				"memory_threshold": batcherConfig.MemoryThreshold,
			})
		}

		eventBatcher = NewEventBatcher(redisClient, s3Client, batcherConfig, os.Getenv("S3_BUCKET_NAME"))

		// Start the batch worker
		go eventBatcher.StartBatchWorker(context.Background())
	})
	return eventBatcher
}

// GetEventBatcherForProvider returns an EventBatcher instance configured for a specific provider
func GetEventBatcherForProvider(provider string) *EventBatcher {
	batcher := GetEventBatcher()
	batcher.config.Provider = provider
	return batcher
}

// NewEventBatcher creates a new EventBatcher instance
func NewEventBatcher(redisClient *redis.Client, s3Client *s3.Client, config BatchConfig, bucketName string) *EventBatcher {
	if config.MemoryThreshold == 0 {
		config.MemoryThreshold = 80 // Default to 80% if not specified
	}
	return &EventBatcher{
		redisClient: redisClient,
		s3Client:    s3Client,
		config:      config,
		bucketName:  bucketName,
	}
}

// AddEvent adds an event to the batch
func (b *EventBatcher) AddEvent(ctx context.Context, event json.RawMessage, provider string, userID int64, email string) error {
	logger.Info(ctx, "event-batcher", "Adding event to Redis", map[string]interface{}{
		"provider": provider,
		"user_id":  userID,
		"email":    email,
	})

	// Inject sh_username into the raw JSON
	var eventMap map[string]interface{}
	if err := json.Unmarshal(event, &eventMap); err != nil {
		logger.Error(ctx, "event-batcher", "Failed to unmarshal event", err)
		return fmt.Errorf("failed to unmarshal event: %w", err)
	}

	// Add sh_username to the event
	eventMap["sh_username"] = email

	// Marshal back to JSON
	modifiedEvent, err := json.Marshal(eventMap)
	if err != nil {
		logger.Error(ctx, "event-batcher", "Failed to marshal modified event", err)
		return fmt.Errorf("failed to marshal modified event: %w", err)
	}

	// Check Redis memory usage
	info, err := b.redisClient.Info(ctx, "memory").Result()
	if err != nil {
		logger.Error(ctx, "event-batcher", "Failed to get Redis memory info", err)
		return fmt.Errorf("failed to get Redis memory info: %w", err)
	}

	// Parse memory usage
	var usedMemory int64
	fmt.Sscanf(info, "used_memory:%d", &usedMemory)
	logger.Info(ctx, "event-batcher", "Redis memory usage", map[string]interface{}{
		"used_memory": usedMemory,
		"threshold":   b.config.MemoryThreshold,
	})

	if usedMemory > int64(b.config.MemoryThreshold) {
		logger.Info(ctx, "event-batcher", "Memory threshold exceeded, forcing batch commit", map[string]interface{}{
			"used_memory": usedMemory,
			"threshold":   b.config.MemoryThreshold,
		})
		// Force commit if memory threshold is exceeded
		if err := b.ProcessBatch(ctx, provider, userID); err != nil {
			logger.Error(ctx, "event-batcher", "Failed to process batch due to memory threshold", err)
			return fmt.Errorf("failed to process batch due to memory threshold: %w", err)
		}
	}

	// Store event in Redis hash with userID in the key
	eventKey := fmt.Sprintf("provider:%s:user:%d:events", provider, userID)

	// Get the next sequence number for this provider/user combination
	now := time.Now()
	seqKey := fmt.Sprintf("provider:%s:user:%d:sequence:%s", provider, userID, now.Format("20060102"))
	seq, err := b.redisClient.Incr(ctx, seqKey).Result()
	if err != nil {
		logger.Error(ctx, "event-batcher", "Failed to increment sequence", err)
		return fmt.Errorf("failed to increment sequence: %w", err)
	}

	// Set expiry on the sequence key (24 hours)
	err = b.redisClient.Expire(ctx, seqKey, 24*time.Hour).Err()
	if err != nil {
		logger.Error(ctx, "event-batcher", "Failed to set expiry on sequence key", err)
	}

	// Use timestamp-based sequence as hash field
	fieldKey := fmt.Sprintf("%d_%d", now.Unix(), seq)

	// Store both the event and its userID
	eventData := struct {
		Event  json.RawMessage `json:"event"`
		UserID int64           `json:"user_id"`
	}{
		Event:  modifiedEvent,
		UserID: userID,
	}

	eventDataJSON, err := json.Marshal(eventData)
	if err != nil {
		logger.Error(ctx, "event-batcher", "Failed to marshal event data", err)
		return fmt.Errorf("failed to marshal event data: %w", err)
	}

	err = b.redisClient.HSet(ctx, eventKey, fieldKey, string(eventDataJSON)).Err()
	if err != nil {
		logger.Error(ctx, "event-batcher", "Failed to store event in Redis", err)
		return fmt.Errorf("failed to store event in Redis: %w", err)
	}

	// Update metadata
	metadataKey := fmt.Sprintf("provider:%s:user:%d:metadata", provider, userID)
	pipe := b.redisClient.Pipeline()
	pipe.HIncrBy(ctx, metadataKey, "event_count", 1)
	pipe.HSet(ctx, metadataKey, "batch_start_time", now.Unix())
	_, err = pipe.Exec(ctx)
	if err != nil {
		logger.Error(ctx, "event-batcher", "Failed to update metadata", err)
		return fmt.Errorf("failed to update metadata: %w", err)
	}

	// Clean up old sequence keys (older than 24 hours)
	oldSeqKey := fmt.Sprintf("provider:%s:user:%d:sequence:%s", provider, userID, now.Add(-24*time.Hour).Format("20060102"))
	b.redisClient.Del(ctx, oldSeqKey)

	logger.Info(ctx, "event-batcher", "Successfully added event to Redis", map[string]interface{}{
		"provider":  provider,
		"user_id":   userID,
		"email":     email,
		"event_key": eventKey,
		"field_key": fieldKey,
	})

	return nil
}

// ProcessBatch processes and uploads a batch of events to S3
func (b *EventBatcher) ProcessBatch(ctx context.Context, provider string, userID int64) error {
	// Get all events from Redis for this provider/user combination
	eventKey := fmt.Sprintf("provider:%s:user:%d:events", provider, userID)
	events, err := b.redisClient.HGetAll(ctx, eventKey).Result()
	if err != nil {
		logger.Error(ctx, "event-batcher", "Failed to get events from Redis", err, map[string]interface{}{
			"provider": provider,
			"user_id":  userID,
			"key":      eventKey,
		})
		return fmt.Errorf("failed to get events from Redis: %w", err)
	}

	logger.Info(ctx, "event-batcher", "Retrieved events from Redis", map[string]interface{}{
		"provider": provider,
		"user_id":  userID,
		"count":    len(events),
	})

	if len(events) == 0 {
		logger.Info(ctx, "event-batcher", "No events to commit", map[string]interface{}{
			"provider": provider,
			"user_id":  userID,
		})
		return nil
	}

	// Create batch ID
	batchID := fmt.Sprintf("%s_%s_batch%d", provider, time.Now().Format("20060102150405"), time.Now().UnixNano())

	// Convert events to raw JSON
	var rawEvents []json.RawMessage
	for _, eventJSON := range events {
		var eventData struct {
			Event  json.RawMessage `json:"event"`
			UserID int64           `json:"user_id"`
		}
		if err := json.Unmarshal([]byte(eventJSON), &eventData); err != nil {
			logger.Error(ctx, "event-batcher", "Failed to unmarshal event data", err, map[string]interface{}{
				"provider": provider,
				"user_id":  userID,
				"event":    eventJSON,
			})
			continue
		}
		rawEvents = append(rawEvents, eventData.Event)
	}

	logger.Info(ctx, "event-batcher", "Processed events for batch", map[string]interface{}{
		"provider":    provider,
		"user_id":     userID,
		"batch_id":    batchID,
		"event_count": len(rawEvents),
	})

	// Create batch metadata
	metadata := BatchMetadata{
		BatchID:    batchID,
		Provider:   provider,
		Timestamp:  time.Now(),
		EventCount: len(rawEvents),
		Status:     "processing",
	}

	// Create batch
	batch := EventBatch{
		BatchID:   batchID,
		Provider:  provider,
		Timestamp: time.Now(),
		Events:    rawEvents,
		Metadata:  metadata,
	}

	// Marshal batch to JSON
	batchJSON, err := json.Marshal(batch)
	if err != nil {
		logger.Error(ctx, "event-batcher", "Failed to marshal batch", err, map[string]interface{}{
			"provider": provider,
			"user_id":  userID,
			"batch_id": batchID,
		})
		return fmt.Errorf("failed to marshal batch: %w", err)
	}

	// Calculate checksum
	hash := sha256.Sum256(batchJSON)
	checksum := hex.EncodeToString(hash[:])

	// Generate S3 key with provider/date/hour structure
	now := time.Now()
	s3Key := fmt.Sprintf("events/user_%d/%s/%04d/%02d/%02d/%02d/events_%s_%s.json",
		userID,
		provider,
		now.Year(), now.Month(), now.Day(), now.Hour(),
		now.Format("20060102150405"),
		batchID,
	)

	logger.Info(ctx, "event-batcher", "Preparing to upload batch to S3", map[string]interface{}{
		"provider":  provider,
		"user_id":   userID,
		"batch_id":  batchID,
		"s3_key":    s3Key,
		"file_size": len(batchJSON),
		"checksum":  checksum,
		"dev_mode":  os.Getenv("DEV_MODE"),
	})

	// In DEV_MODE, just log the events instead of uploading to S3
	if os.Getenv("DEV_MODE") == "true" {
		logger.Info(ctx, "event-batcher", "DEV_MODE: Would upload batch to S3", map[string]interface{}{
			"provider":  provider,
			"user_id":   userID,
			"batch_id":  batchID,
			"events":    len(rawEvents),
			"s3_key":    s3Key,
			"file_size": len(batchJSON),
			"checksum":  checksum,
		})
	} else {
		// Upload to S3
		_, err = b.s3Client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:      aws.String(b.bucketName),
			Key:         aws.String(s3Key),
			Body:        bytes.NewReader(batchJSON),
			ContentType: aws.String("application/json"),
		})
		if err != nil {
			metadata.Status = "failed"
			logger.Error(ctx, "event-batcher", "Failed to upload batch to S3", err, map[string]interface{}{
				"provider": provider,
				"user_id":  userID,
				"batch_id": batchID,
				"s3_key":   s3Key,
				"bucket":   b.bucketName,
			})
			return fmt.Errorf("failed to upload batch to S3: %w", err)
		}

		logger.Info(ctx, "event-batcher", "Successfully uploaded batch to S3", map[string]interface{}{
			"provider":  provider,
			"user_id":   userID,
			"batch_id":  batchID,
			"s3_key":    s3Key,
			"bucket":    b.bucketName,
			"file_size": len(batchJSON),
		})
	}

	// Update metadata with success status and file size
	metadata.Status = "completed"
	metadata.FileSize = int64(len(batchJSON))
	metadata.Checksum = checksum

	// Clear the batch from Redis
	err = b.redisClient.Del(ctx, eventKey).Err()
	if err != nil {
		logger.Error(ctx, "event-batcher", "Failed to clear events from Redis", err, map[string]interface{}{
			"provider": provider,
			"user_id":  userID,
			"batch_id": batchID,
			"key":      eventKey,
		})
	} else {
		logger.Info(ctx, "event-batcher", "Cleared events from Redis", map[string]interface{}{
			"provider": provider,
			"user_id":  userID,
			"batch_id": batchID,
			"key":      eventKey,
		})
	}

	// Clear metadata
	metadataKey := fmt.Sprintf("provider:%s:user:%d:metadata", provider, userID)
	err = b.redisClient.Del(ctx, metadataKey).Err()
	if err != nil {
		logger.Error(ctx, "event-batcher", "Failed to clear metadata from Redis", err, map[string]interface{}{
			"provider": provider,
			"user_id":  userID,
			"batch_id": batchID,
			"key":      metadataKey,
		})
	} else {
		logger.Info(ctx, "event-batcher", "Cleared metadata from Redis", map[string]interface{}{
			"provider": provider,
			"user_id":  userID,
			"batch_id": batchID,
			"key":      metadataKey,
		})
	}

	logger.Info(ctx, "event-batcher", "Successfully committed batch", map[string]interface{}{
		"provider":  provider,
		"user_id":   userID,
		"batch_id":  batchID,
		"events":    len(rawEvents),
		"s3_key":    s3Key,
		"file_size": len(batchJSON),
		"checksum":  checksum,
		"status":    metadata.Status,
	})

	return nil
}

// StartBatchWorker starts the background worker for batch processing
func (b *EventBatcher) StartBatchWorker(ctx context.Context) {
	logger.Info(ctx, "event-batcher", "Starting batch worker", map[string]interface{}{
		"flush_interval": b.config.FlushInterval,
		"max_size":       b.config.MaxSize,
		"max_bytes":      b.config.MaxBytes,
		"provider":       b.config.Provider,
	})

	ticker := time.NewTicker(b.config.FlushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logger.Info(ctx, "event-batcher", "Batch worker shutting down")
			return
		case <-ticker.C:
			logger.Info(ctx, "event-batcher", "Batch worker tick", map[string]interface{}{
				"timestamp": time.Now().Format(time.RFC3339),
			})

			// Get all provider/user combinations from Redis
			keys, err := b.redisClient.Keys(ctx, "provider:*:user:*:events").Result()
			if err != nil {
				logger.Error(ctx, "event-batcher", "Failed to get provider/user keys", err)
				continue
			}

			logger.Info(ctx, "event-batcher", "Found provider/user combinations", map[string]interface{}{
				"count": len(keys),
				"keys":  keys,
			})

			// Process each provider/user combination
			for _, key := range keys {
				// Extract provider and userID from key
				// Key format: provider:sendgrid:user:2:events
				parts := strings.Split(key, ":")
				if len(parts) != 5 {
					logger.Error(ctx, "event-batcher", "Invalid key format", nil, map[string]interface{}{
						"key":   key,
						"parts": parts,
					})
					continue
				}
				provider := parts[1]
				userID, err := strconv.ParseInt(parts[3], 10, 64)
				if err != nil {
					logger.Error(ctx, "event-batcher", "Failed to parse userID", err, map[string]interface{}{
						"key":         key,
						"user_id_str": parts[3],
					})
					continue
				}

				logger.Info(ctx, "event-batcher", "Processing batch", map[string]interface{}{
					"provider": provider,
					"user_id":  userID,
					"key":      key,
				})

				// Process batch for this provider/user combination
				if err := b.ProcessBatch(ctx, provider, userID); err != nil {
					logger.Error(ctx, "event-batcher", "Failed to process batch", err, map[string]interface{}{
						"provider": provider,
						"user_id":  userID,
					})
				}
			}
		}
	}
}

// EventBatcherProcessor implements the webhook.EventProcessor interface
type EventBatcherProcessor struct {
	batcher *EventBatcher
}

// NewEventBatcherProcessor creates a new EventBatcherProcessor
func NewEventBatcherProcessor() *EventBatcherProcessor {
	return &EventBatcherProcessor{
		batcher: GetEventBatcher(),
	}
}

// Process all SparkPost events and use the provider name from the Event object
func processSparkPostEvent(ctx context.Context, event json.RawMessage, userID int64, email string, batcher *EventBatcher) error {
	// Add the raw event directly to the batch
	return GetEventBatcherForProvider("sparkpost").AddEvent(ctx, event, "sparkpost", userID, email)
}

// ProcessEvent implements the webhook.EventProcessor interface
func (p *EventBatcherProcessor) ProcessEvent(ctx context.Context, event json.RawMessage, userID int64, email string) error {
	// Try to get the provider from the context
	provider, ok := ctx.Value("provider").(string)
	if !ok {
		provider = "unknown"
	}

	// Add the raw event directly to the batch
	return GetEventBatcherForProvider(provider).AddEvent(ctx, event, provider, userID, email)
}
