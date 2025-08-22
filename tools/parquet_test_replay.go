package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type Config struct {
	MinioEndpoint   string
	MinioAccessKey  string
	MinioSecretKey  string
	BucketName      string
	RelayGoEndpoint string
	SourceUserID    string
	TestUserID      int // New user ID for test data
	Provider        string
	BatchSize       int
	MaxFiles        int
}

type EventBatch struct {
	BatchID   string          `json:"batch_id"`
	Provider  string          `json:"provider"`
	Timestamp interface{}     `json:"timestamp"`
	Events    []json.RawMessage `json:"events"`
}

func main() {
	var cfg Config
	
	// Parse command line flags
	flag.StringVar(&cfg.MinioEndpoint, "minio", "http://localhost:9010", "MinIO endpoint URL")
	flag.StringVar(&cfg.MinioAccessKey, "access-key", "minioadmin", "MinIO access key")
	flag.StringVar(&cfg.MinioSecretKey, "secret-key", "minioadmin", "MinIO secret key")
	flag.StringVar(&cfg.BucketName, "bucket", "production-relay-go-events-998623545110", "S3 bucket name")
	flag.StringVar(&cfg.RelayGoEndpoint, "relay-go", "http://localhost:8080", "Relay-Go webhook endpoint")
	flag.StringVar(&cfg.SourceUserID, "source-user", "4", "Source user ID to copy events from")
	flag.IntVar(&cfg.TestUserID, "test-user", 99999, "Test user ID for PARQUET output")
	flag.StringVar(&cfg.Provider, "provider", "sendgrid", "Provider (sendgrid, mandrill, etc)")
	flag.IntVar(&cfg.BatchSize, "batch-size", 100, "Events per webhook batch")
	flag.IntVar(&cfg.MaxFiles, "max-files", 10, "Maximum number of files to process (0 = all)")
	flag.Parse()

	log.Printf("Starting PARQUET test replay tool")
	log.Printf("Configuration:")
	log.Printf("  MinIO: %s", cfg.MinioEndpoint)
	log.Printf("  Source User: %s", cfg.SourceUserID)
	log.Printf("  Test User: %d", cfg.TestUserID)
	log.Printf("  Provider: %s", cfg.Provider)
	log.Printf("  Batch Size: %d", cfg.BatchSize)
	log.Printf("  Max Files: %d", cfg.MaxFiles)

	// Create S3 client for MinIO
	s3Client, err := createMinioClient(cfg)
	if err != nil {
		log.Fatalf("Failed to create MinIO client: %v", err)
	}

	// List JSON files for the source user
	files, err := listEventFiles(s3Client, cfg)
	if err != nil {
		log.Fatalf("Failed to list event files: %v", err)
	}

	log.Printf("Found %d JSON files to process", len(files))

	// Process each file
	totalEvents := 0
	successCount := 0
	failCount := 0

	for i, file := range files {
		if cfg.MaxFiles > 0 && i >= cfg.MaxFiles {
			log.Printf("Reached max files limit (%d), stopping", cfg.MaxFiles)
			break
		}

		log.Printf("\n[%d/%d] Processing file: %s", i+1, len(files), file)
		
		events, err := readEventsFromFile(s3Client, cfg, file)
		if err != nil {
			log.Printf("ERROR: Failed to read events from %s: %v", file, err)
			continue
		}

		log.Printf("  Found %d events in file", len(events))
		totalEvents += len(events)

		// Send events in batches to relay-go
		for j := 0; j < len(events); j += cfg.BatchSize {
			end := j + cfg.BatchSize
			if end > len(events) {
				end = len(events)
			}
			
			batch := events[j:end]
			log.Printf("  Sending batch %d-%d (%d events)", j, end, len(batch))
			
			if err := sendEventsToRelayGo(cfg, batch); err != nil {
				log.Printf("  ERROR: Failed to send batch: %v", err)
				failCount += len(batch)
			} else {
				successCount += len(batch)
				log.Printf("  ✓ Batch sent successfully")
			}

			// Small delay between batches to avoid overwhelming the system
			time.Sleep(100 * time.Millisecond)
		}
	}

	log.Printf("\n========================================")
	log.Printf("PARQUET Test Replay Complete!")
	log.Printf("========================================")
	log.Printf("Total events processed: %d", totalEvents)
	log.Printf("Successfully sent: %d", successCount)
	log.Printf("Failed: %d", failCount)
	log.Printf("\nCheck MinIO for both JSON and PARQUET output:")
	log.Printf("  JSON:    events/user_%d/%s/*/events_*.json", cfg.TestUserID, cfg.Provider)
	log.Printf("  PARQUET: events/user_%d/%s/*/events_*.parquet", cfg.TestUserID, cfg.Provider)
}

func createMinioClient(cfg Config) (*s3.Client, error) {
	// Create AWS config for MinIO
	awsCfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			cfg.MinioAccessKey,
			cfg.MinioSecretKey,
			"",
		)),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	// Create S3 client with MinIO endpoint
	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.UsePathStyle = true
		o.EndpointResolver = s3.EndpointResolverFunc(func(region string, options s3.EndpointResolverOptions) (aws.Endpoint, error) {
			return aws.Endpoint{
				URL:               cfg.MinioEndpoint,
				HostnameImmutable: true,
				SigningRegion:     region,
			}, nil
		})
	})

	return client, nil
}

func listEventFiles(client *s3.Client, cfg Config) ([]string, error) {
	prefix := fmt.Sprintf("events/user_%s/%s/", cfg.SourceUserID, cfg.Provider)
	
	var files []string
	paginator := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{
		Bucket: aws.String(cfg.BucketName),
		Prefix: aws.String(prefix),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(context.Background())
		if err != nil {
			return nil, fmt.Errorf("failed to list objects: %w", err)
		}

		for _, obj := range page.Contents {
			if strings.HasSuffix(*obj.Key, ".json") {
				files = append(files, *obj.Key)
			}
		}
	}

	return files, nil
}

func readEventsFromFile(client *s3.Client, cfg Config, key string) ([]json.RawMessage, error) {
	// Download the file from MinIO
	result, err := client.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String(cfg.BucketName),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get object: %w", err)
	}
	defer result.Body.Close()

	// Read the file content
	data, err := io.ReadAll(result.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read object body: %w", err)
	}

	// Parse the event batch
	var batch EventBatch
	if err := json.Unmarshal(data, &batch); err != nil {
		return nil, fmt.Errorf("failed to parse event batch: %w", err)
	}

	// Update each event with the test user ID
	var modifiedEvents []json.RawMessage
	for _, event := range batch.Events {
		var eventMap map[string]interface{}
		if err := json.Unmarshal(event, &eventMap); err != nil {
			log.Printf("Warning: Failed to parse event, skipping: %v", err)
			continue
		}

		// Update the user_id in the event (if present)
		if _, ok := eventMap["user_id"]; ok {
			eventMap["user_id"] = cfg.TestUserID
		}

		// Re-marshal the modified event
		modifiedEvent, err := json.Marshal(eventMap)
		if err != nil {
			log.Printf("Warning: Failed to marshal modified event, skipping: %v", err)
			continue
		}

		modifiedEvents = append(modifiedEvents, modifiedEvent)
	}

	return modifiedEvents, nil
}

func sendEventsToRelayGo(cfg Config, events []json.RawMessage) error {
	// Construct webhook endpoint URL
	endpoint := fmt.Sprintf("%s/webhook-events/%s", cfg.RelayGoEndpoint, cfg.Provider)

	// Create the webhook payload based on provider format
	var payload []byte
	var err error

	switch cfg.Provider {
	case "mandrill":
		// Mandrill uses form-encoded with mandrill_events parameter
		eventsJSON, _ := json.Marshal(events)
		formData := fmt.Sprintf("mandrill_events=%s", eventsJSON)
		payload = []byte(formData)
	default:
		// Most providers (SendGrid, SparkPost, etc.) send JSON array directly
		payload, err = json.Marshal(events)
		if err != nil {
			return fmt.Errorf("failed to marshal events: %w", err)
		}
	}

	// Create HTTP request
	var req *http.Request
	if cfg.Provider == "mandrill" {
		req, err = http.NewRequest("POST", endpoint, bytes.NewBuffer(payload))
		if err != nil {
			return fmt.Errorf("failed to create request: %w", err)
		}
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	} else {
		req, err = http.NewRequest("POST", endpoint, bytes.NewBuffer(payload))
		if err != nil {
			return fmt.Errorf("failed to create request: %w", err)
		}
		req.Header.Set("Content-Type", "application/json")
	}

	// Add test webhook secret header (relay-go will need to verify this)
	// For testing, we'll use a special header to indicate test mode
	req.Header.Set("X-Test-Mode", "true")
	req.Header.Set("X-Test-User-ID", fmt.Sprintf("%d", cfg.TestUserID))
	
	// Provider-specific headers for webhook verification
	switch cfg.Provider {
	case "sendgrid":
		// SendGrid uses signature verification - for testing we'll skip
		req.Header.Set("X-Twilio-Email-Event-Webhook-Signature", "test-signature")
		req.Header.Set("X-Twilio-Email-Event-Webhook-Timestamp", fmt.Sprintf("%d", time.Now().Unix()))
	case "sparkpost":
		req.Header.Set("X-MessageSystems-Webhook-Token", "test-token")
	case "postmark":
		req.Header.Set("X-Postmark-Token", "test-token")
	case "mandrill":
		req.Header.Set("X-Mandrill-Signature", "test-signature")
	}

	// Send the request
	client := &http.Client{
		Timeout: 30 * time.Second,
	}
	
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	// Check response status
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("webhook returned status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}