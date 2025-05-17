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
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
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
	BatchID   string         `json:"batch_id"`
	Provider  string         `json:"provider"`
	Timestamp time.Time      `json:"timestamp"`
	Events    []WebhookEvent `json:"events"`
	Metadata  BatchMetadata  `json:"metadata"`
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
	Categories []string `json:"categories"` // Categories/tags associated with the event
	Reason     string   `json:"reason"`     // Reason for bounces/failures
	ErrorCode  string   `json:"error_code"` // Error code if applicable

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
func (b *EventBatcher) AddEvent(ctx context.Context, event WebhookEvent) error {
	// Check Redis memory usage
	info, err := b.redisClient.Info(ctx, "memory").Result()
	if err != nil {
		return fmt.Errorf("failed to get Redis memory info: %w", err)
	}

	// Parse memory usage
	var usedMemory int64
	fmt.Sscanf(info, "used_memory:%d", &usedMemory)
	if usedMemory > int64(b.config.MemoryThreshold) {
		// Force commit if memory threshold is exceeded
		if err := b.ProcessBatch(ctx, b.config.Provider); err != nil {
			return fmt.Errorf("failed to process batch due to memory threshold: %w", err)
		}
	}

	// Store event in Redis hash
	eventKey := fmt.Sprintf("provider:%s:events", b.config.Provider)
	eventJSON, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	// Get the next sequence number for this provider using a timestamp-based sequence
	now := time.Now()
	seqKey := fmt.Sprintf("provider:%s:sequence:%s", b.config.Provider, now.Format("20060102"))
	seq, err := b.redisClient.Incr(ctx, seqKey).Result()
	if err != nil {
		return fmt.Errorf("failed to increment sequence: %w", err)
	}

	// Set expiry on the sequence key (24 hours)
	err = b.redisClient.Expire(ctx, seqKey, 24*time.Hour).Err()
	if err != nil {
		logger.Error(ctx, "event-batcher", "Failed to set expiry on sequence key", err)
	}

	// Use timestamp-based sequence as hash field
	fieldKey := fmt.Sprintf("%d_%d", now.Unix(), seq)
	err = b.redisClient.HSet(ctx, eventKey, fieldKey, eventJSON).Err()
	if err != nil {
		return fmt.Errorf("failed to store event in Redis: %w", err)
	}

	// Update metadata
	metadataKey := fmt.Sprintf("provider:%s:metadata", b.config.Provider)
	pipe := b.redisClient.Pipeline()
	pipe.HIncrBy(ctx, metadataKey, "event_count", 1)
	pipe.HSet(ctx, metadataKey, "batch_start_time", now.Unix())
	_, err = pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed to update metadata: %w", err)
	}

	// Clean up old sequence keys (older than 24 hours)
	oldSeqKey := fmt.Sprintf("provider:%s:sequence:%s", b.config.Provider, now.Add(-24*time.Hour).Format("20060102"))
	b.redisClient.Del(ctx, oldSeqKey)

	return nil
}

// ProcessBatch processes and uploads a batch of events to S3
func (b *EventBatcher) ProcessBatch(ctx context.Context, provider string) error {
	// Get all events from Redis
	eventKey := fmt.Sprintf("provider:%s:events", provider)
	events, err := b.redisClient.HGetAll(ctx, eventKey).Result()
	if err != nil {
		return fmt.Errorf("failed to get events from Redis: %w", err)
	}

	if len(events) == 0 {
		logger.Info(ctx, "event-batcher", "No events to commit", map[string]interface{}{
			"provider": provider,
		})
		return nil
	}

	// Create batch ID
	batchID := fmt.Sprintf("%s_%s_batch%d", provider, time.Now().Format("20060102150405"), time.Now().UnixNano())

	// Convert events to WebhookEvent structs
	var webhookEvents []WebhookEvent
	for _, eventJSON := range events {
		var event WebhookEvent
		if err := json.Unmarshal([]byte(eventJSON), &event); err != nil {
			logger.Error(ctx, "event-batcher", "Failed to unmarshal event", err, map[string]interface{}{
				"provider": provider,
				"batch_id": batchID,
			})
			continue
		}
		webhookEvents = append(webhookEvents, event)
	}

	// Create batch metadata
	metadata := BatchMetadata{
		BatchID:    batchID,
		Provider:   provider,
		Timestamp:  time.Now(),
		EventCount: len(webhookEvents),
		Status:     "processing",
	}

	// Create batch
	batch := EventBatch{
		BatchID:   batchID,
		Provider:  provider,
		Timestamp: time.Now(),
		Events:    webhookEvents,
		Metadata:  metadata,
	}

	// Marshal batch to JSON
	batchJSON, err := json.Marshal(batch)
	if err != nil {
		return fmt.Errorf("failed to marshal batch: %w", err)
	}

	// Calculate checksum
	hash := sha256.Sum256(batchJSON)
	checksum := hex.EncodeToString(hash[:])

	// Generate S3 key with provider/date/hour structure
	now := time.Now()
	s3Key := fmt.Sprintf("events/%s/%04d/%02d/%02d/%02d/events_%s_%s.json",
		provider,
		now.Year(), now.Month(), now.Day(), now.Hour(),
		now.Format("20060102150405"),
		batchID,
	)

	// In DEV_MODE, just log the events instead of uploading to S3
	if os.Getenv("DEV_MODE") == "true" {
		logger.Info(ctx, "event-batcher", "DEV_MODE: Would upload batch to S3", map[string]interface{}{
			"provider":  provider,
			"batch_id":  batchID,
			"events":    len(webhookEvents),
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
				"batch_id": batchID,
				"s3_key":   s3Key,
			})
			return fmt.Errorf("failed to upload batch to S3: %w", err)
		}
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
			"batch_id": batchID,
		})
	}

	// Clear metadata
	metadataKey := fmt.Sprintf("provider:%s:metadata", provider)
	err = b.redisClient.Del(ctx, metadataKey).Err()
	if err != nil {
		logger.Error(ctx, "event-batcher", "Failed to clear metadata from Redis", err, map[string]interface{}{
			"provider": provider,
			"batch_id": batchID,
		})
	}

	logger.Info(ctx, "event-batcher", "Successfully committed batch", map[string]interface{}{
		"provider":  provider,
		"batch_id":  batchID,
		"events":    len(webhookEvents),
		"s3_key":    s3Key,
		"file_size": len(batchJSON),
		"checksum":  checksum,
	})

	return nil
}

// StartBatchWorker starts the background worker for batch processing
func (b *EventBatcher) StartBatchWorker(ctx context.Context) {
	ticker := time.NewTicker(b.config.FlushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Process all batches for the current provider
			if err := b.ProcessBatch(ctx, b.config.Provider); err != nil {
				logger.Error(ctx, "event-batcher", "Failed to process batch", err)
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

// ProcessEvent implements the webhook.EventProcessor interface
func (p *EventBatcherProcessor) ProcessEvent(ctx context.Context, event json.RawMessage, userID int64, email string) error {
	// Try to parse as SendGrid event first
	var sgEvent struct {
		Event       string   `json:"event"`
		Email       string   `json:"email"`
		Timestamp   int64    `json:"timestamp"`
		SGMessageID string   `json:"sg_message_id"`
		Category    []string `json:"category"`
		SGEventID   string   `json:"sg_event_id"`
		SMTPID      string   `json:"smtp-id"`
		BounceType  string   `json:"bounce_type,omitempty"`
		Reason      string   `json:"reason,omitempty"`
		IP          string   `json:"ip,omitempty"`
		UserAgent   string   `json:"useragent,omitempty"`
	}

	if err := json.Unmarshal(event, &sgEvent); err == nil && sgEvent.Event != "" {
		// It's a SendGrid event
		webhookEvent := WebhookEvent{
			EventID:    fmt.Sprintf("sg_%s", sgEvent.SGEventID),
			EventType:  sgEvent.Event,
			Timestamp:  time.Unix(sgEvent.Timestamp, 0),
			Provider:   "sendgrid",
			UserID:     userID,
			Email:      sgEvent.Email,
			Username:   email,
			MessageID:  sgEvent.SGMessageID,
			SMTPID:     sgEvent.SMTPID,
			Categories: sgEvent.Category,
			Reason:     sgEvent.Reason,
			IPAddress:  sgEvent.IP,
			UserAgent:  sgEvent.UserAgent,
		}
		return GetEventBatcherForProvider("sendgrid").AddEvent(ctx, webhookEvent)
	}

	// Try to parse as SparkPost event
	var spEvent struct {
		Msys struct {
			MessageEvent *struct {
				Type        string `json:"type"`
				Timestamp   string `json:"timestamp"`
				MessageID   string `json:"message_id"`
				RcptTo      string `json:"rcpt_to"`
				BounceClass string `json:"bounce_class,omitempty"`
				ErrorCode   string `json:"error_code,omitempty"`
				Reason      string `json:"reason,omitempty"`
				IPAddress   string `json:"ip_address,omitempty"`
				UserAgent   string `json:"user_agent,omitempty"`
				GeoIP       *GeoIP `json:"geo_ip,omitempty"`
			} `json:"message_event,omitempty"`
			TrackEvent *struct {
				Type      string `json:"type"`
				Timestamp string `json:"timestamp"`
				MessageID string `json:"message_id"`
				RcptTo    string `json:"rcpt_to"`
				IPAddress string `json:"ip_address,omitempty"`
				UserAgent string `json:"user_agent,omitempty"`
				GeoIP     *GeoIP `json:"geo_ip,omitempty"`
			} `json:"track_event,omitempty"`
		} `json:"msys"`
	}

	if err := json.Unmarshal(event, &spEvent); err == nil {
		var eventType string
		var timestamp time.Time
		var messageID string
		var recipientEmail string
		var errorCode string
		var reason string
		var ipAddress string
		var userAgent string
		var geoIP *GeoIP

		if spEvent.Msys.MessageEvent != nil {
			eventType = spEvent.Msys.MessageEvent.Type
			timestamp, _ = time.Parse(time.RFC3339, spEvent.Msys.MessageEvent.Timestamp)
			messageID = spEvent.Msys.MessageEvent.MessageID
			recipientEmail = spEvent.Msys.MessageEvent.RcptTo
			errorCode = spEvent.Msys.MessageEvent.ErrorCode
			reason = spEvent.Msys.MessageEvent.Reason
			ipAddress = spEvent.Msys.MessageEvent.IPAddress
			userAgent = spEvent.Msys.MessageEvent.UserAgent
			geoIP = spEvent.Msys.MessageEvent.GeoIP
		} else if spEvent.Msys.TrackEvent != nil {
			eventType = spEvent.Msys.TrackEvent.Type
			timestamp, _ = time.Parse(time.RFC3339, spEvent.Msys.TrackEvent.Timestamp)
			messageID = spEvent.Msys.TrackEvent.MessageID
			recipientEmail = spEvent.Msys.TrackEvent.RcptTo
			ipAddress = spEvent.Msys.TrackEvent.IPAddress
			userAgent = spEvent.Msys.TrackEvent.UserAgent
			geoIP = spEvent.Msys.TrackEvent.GeoIP
		}

		if eventType != "" {
			webhookEvent := WebhookEvent{
				EventID:   fmt.Sprintf("sp_%s", messageID),
				EventType: eventType,
				Timestamp: timestamp,
				Provider:  "sparkpost",
				UserID:    userID,
				Email:     recipientEmail,
				Username:  email,
				MessageID: messageID,
				Reason:    reason,
				ErrorCode: errorCode,
				IPAddress: ipAddress,
				UserAgent: userAgent,
				GeoIP:     geoIP,
			}
			return GetEventBatcherForProvider("sparkpost").AddEvent(ctx, webhookEvent)
		}
	}

	// Try to parse as Postmark event
	var pmEvent struct {
		MessageID string `json:"MessageID"`
		Type      string `json:"Type"`
		Timestamp string `json:"DeliveredAt"`
		Recipient string `json:"Recipient"`
		Details   string `json:"Details,omitempty"`
		ErrorCode string `json:"ErrorCode,omitempty"`
		IPAddress string `json:"ServerIP,omitempty"`
		UserAgent string `json:"UserAgent,omitempty"`
	}

	if err := json.Unmarshal(event, &pmEvent); err == nil && pmEvent.MessageID != "" {
		timestamp, _ := time.Parse(time.RFC3339, pmEvent.Timestamp)
		webhookEvent := WebhookEvent{
			EventID:   fmt.Sprintf("pm_%s", pmEvent.MessageID),
			EventType: pmEvent.Type,
			Timestamp: timestamp,
			Provider:  "postmark",
			UserID:    userID,
			Email:     pmEvent.Recipient,
			Username:  email,
			MessageID: pmEvent.MessageID,
			Reason:    pmEvent.Details,
			ErrorCode: pmEvent.ErrorCode,
			IPAddress: pmEvent.IPAddress,
			UserAgent: pmEvent.UserAgent,
		}
		return GetEventBatcherForProvider("postmark").AddEvent(ctx, webhookEvent)
	}

	// If we can't determine the event type, create a generic event
	webhookEvent := WebhookEvent{
		EventID:   fmt.Sprintf("gen_%s", uuid.New().String()),
		EventType: "unknown",
		Timestamp: time.Now(),
		Provider:  "unknown",
		UserID:    userID,
		Email:     email,
		Username:  email,
	}
	return GetEventBatcherForProvider("unknown").AddEvent(ctx, webhookEvent)
}
