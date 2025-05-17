package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"relay-go/m/database"
	"relay-go/m/logger"
	"relay-go/m/webhook"
	"strings"

	"github.com/IBM/sarama"
	"github.com/google/uuid"
	"github.com/joho/godotenv"
)

// Config holds all configuration values
type Config struct {
	HTTPPort      string
	KafkaBrokers  []string
	EmailTopic    string
	WebhookTopics map[string]string
	SplunkHost    string
	SplunkToken   string
	RedisHost     string
	LogLevel      string
}

// loadConfig loads all configuration from environment variables
func loadConfig() (*Config, error) {
	envErr := godotenv.Load()
	if envErr != nil {
		logger.Info(nil, "init", "No .env file found, using system environment variables")
	}

	config := &Config{
		HTTPPort:     os.Getenv("HTTP_SERVER_PORT"),
		KafkaBrokers: []string{os.Getenv("KAFKA_BROKERS")},
		EmailTopic:   os.Getenv("EMAIL_TOPIC"),
		WebhookTopics: map[string]string{
			"sendgrid":   os.Getenv("WEBHOOK_TOPIC_SENDGRID"),
			"sparkpost":  os.Getenv("WEBHOOK_TOPIC_SPARKPOST"),
			"postmark":   os.Getenv("WEBHOOK_TOPIC_POSTMARK"),
			"socketlabs": os.Getenv("WEBHOOK_TOPIC_SOCKETLABS"),
		},
		SplunkHost:  os.Getenv("SPLUNK_HOST"),
		SplunkToken: os.Getenv("SPLUNK_KEY"),
		RedisHost:   os.Getenv("REDIS_HOST"),
		LogLevel:    os.Getenv("LOG_LEVEL"),
	}

	// Validate required configuration
	if config.HTTPPort == "" {
		return nil, fmt.Errorf("HTTP_SERVER_PORT environment variable is not set")
	}
	if len(config.KafkaBrokers) == 0 || config.KafkaBrokers[0] == "" {
		return nil, fmt.Errorf("KAFKA_BROKERS environment variable is not set")
	}
	if config.EmailTopic == "" {
		return nil, fmt.Errorf("EMAIL_TOPIC environment variable is not set")
	}
	if config.SplunkHost == "" {
		return nil, fmt.Errorf("SPLUNK_HOST environment variable is not set")
	}
	if config.SplunkToken == "" {
		return nil, fmt.Errorf("SPLUNK_KEY environment variable is not set")
	}
	if config.RedisHost == "" {
		return nil, fmt.Errorf("REDIS_HOST environment variable is not set")
	}

	return config, nil
}

var splunkClient *webhook.SplunkClient
var webhookHandler *WebhookHandler
var config *Config

func init() {
	var err error
	config, err = loadConfig()
	if err != nil {
		logger.Fatal(nil, "init", "Failed to load configuration", err)
	}

	// Set logger level based on environment
	if config.LogLevel != "" {
		switch config.LogLevel {
		case "DEBUG":
			logger.SetLevel(logger.DEBUG)
		case "INFO":
			logger.SetLevel(logger.INFO)
		case "WARNING":
			logger.SetLevel(logger.WARNING)
		case "ERROR":
			logger.SetLevel(logger.ERROR)
		case "FATAL":
			logger.SetLevel(logger.FATAL)
		default:
			logger.SetLevel(logger.INFO)
		}
	}

	// Initialize data stores
	if err := database.InitDataStores(); err != nil {
		logger.Fatal(nil, "init", "Failed to initialize data stores", err)
	}

	splunkClient = webhook.NewSplunkClient(config.SplunkHost, config.SplunkToken)

	// Initialize webhook handler
	webhookHandler = NewWebhookHandler(database.GetDB(), database.GetDynamoClient())
}

func main() {
	var producer sarama.SyncProducer
	var err error

	// Only initialize Kafka if we're in MySQL+Kafka mode
	if database.IsMySQLKafkaMode() {
		// Set up the Kafka producer
		producer, err = sarama.NewSyncProducer(config.KafkaBrokers, nil)
		if err != nil {
			logger.Fatal(nil, "main", "Failed to start Kafka producer", err)
		}
		defer producer.Close()
	}

	// Set up the HTTP server for emails
	http.HandleFunc("/emails", func(w http.ResponseWriter, r *http.Request) {
		authHeader := r.Header.Get("Authorization")
		apiKey := extractAPIKey(authHeader)

		user, validateAPIKeyError := validateAPIKey(database.GetDB(), apiKey)
		if validateAPIKeyError != nil {
			logInvalidAttempt(apiKey)
			http.Error(w, "Invalid API key", http.StatusUnauthorized)
			return
		}

		body, httpReqReadErr := io.ReadAll(r.Body)
		if httpReqReadErr != nil {
			http.Error(w, "Failed to read request body", http.StatusInternalServerError)
			return
		}
		defer r.Body.Close()

		var emailPayload EmailPayload
		if emailPayloadJSONerr := json.Unmarshal(body, &emailPayload); emailPayloadJSONerr != nil {
			http.Error(w, fmt.Sprintf("Invalid JSON: %v", emailPayloadJSONerr), http.StatusBadRequest)
			return
		}

		if err := validateEmailPayload(emailPayload); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		messageID := uuid.New().String()

		// Store the email request
		err := storeEmailRequest(database.GetDB(), user.ID, messageID)
		if err != nil {
			http.Error(w, "Failed to store email request", http.StatusInternalServerError)
			return
		}
	})

	// Set up the HTTP server for webhooks
	http.HandleFunc("/webhook-events/sendgrid", func(w http.ResponseWriter, r *http.Request) {
		// Create request context with ID
		ctx := logger.WithRequestID(r.Context(), uuid.New().String())

		body, err := io.ReadAll(r.Body)
		if err != nil {
			logger.Error(ctx, "sendgrid-webhook", "Failed to read request body", err)
			http.Error(w, "Failed to read request body", http.StatusInternalServerError)
			return
		}
		defer r.Body.Close()

		userID, email, err := webhookHandler.ProcessWebhook(ctx, "sendgrid", body, r.Header)
		if err != nil {
			http.Error(w, "Failed to verify webhook", http.StatusUnauthorized)
			return
		}

		// Add user ID to context
		ctx = logger.WithUserID(ctx, fmt.Sprintf("%d", userID))

		// Unmarshal the events
		var events []json.RawMessage
		if err := json.Unmarshal(body, &events); err != nil {
			logger.Error(ctx, "sendgrid-webhook", "Failed to unmarshal message body", err)
			http.Error(w, "Failed to process event payload", http.StatusBadRequest)
			return
		}

		// Create appropriate processor based on mode
		var processor webhook.EventProcessor
		if database.IsMySQLKafkaMode() {
			// Full mode: Process with MySQL and Kafka
			processor = webhook.NewKafkaEventProcessor(producer, config.WebhookTopics["sendgrid"])
		} else {
			// Light mode: Send to both Splunk and S3
			processor = webhook.NewCompositeProcessor(
				webhook.NewSplunkEventProcessor(splunkClient),
				database.NewEventBatcherProcessor(),
			)
		}

		// Process all events
		if err := webhook.ProcessWebhookEvents(ctx, events, int64(userID), email, processor); err != nil {
			logger.Error(ctx, "sendgrid-webhook", "Failed to process events", err)
			http.Error(w, "Failed to process webhook", http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)
	})

	http.HandleFunc("/webhook-events/sparkpost", func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "Failed to read request body", http.StatusInternalServerError)
			return
		}
		defer r.Body.Close()

		// Verify the webhook and find the associated user

		userID, espID, err := verifySparkPostWebhookAndFindUser(database.GetDB(), r.Header)
		if err != nil {
			log.Printf("Sparkpost - Failed to verify webhook: %v", err)
			http.Error(w, "Failed to verify webhook", http.StatusUnauthorized)
			return
		}

		log.Printf("UserID: %v, ESPID: %v", userID, espID)

		var sparkPostPayload SparkPostPayload
		err = json.Unmarshal(body, &sparkPostPayload)
		if err != nil {
			log.Printf("Failed to unmarshal SparkPost events: %v", err)
			http.Error(w, "Failed to process event payload", http.StatusBadRequest)
			return
		}

		for _, event := range sparkPostPayload {
			err = associateSparkPostEventWithUser(database.GetDB(), event.Msys.MessageEvent.MessageID, userID, espID)
			if err != nil {
				log.Printf("Failed to associate event with user: %v", err)
				// Decide whether to continue or return based on your error handling strategy
			}
		}

		message := Message{
			Headers: r.Header,
			Body:    body,
		}

		handleRequest(w, producer, config.WebhookTopics["sparkpost"], message)
	})

	http.HandleFunc("/webhook-events/postmark", func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "Failed to read request body", http.StatusInternalServerError)
			return
		}
		defer r.Body.Close()

		userID, espID, err := verifyPostmarkWebhookAndFindUser(database.GetDB(), r.Header)
		if err != nil {
			log.Printf("Postmark - Failed to verify webhook: %v", err)
			http.Error(w, "Failed to verify webhook", http.StatusUnauthorized)
			return
		}

		var postmarkEvent struct {
			MessageID string `json:"MessageID"`
		}
		err = json.Unmarshal(body, &postmarkEvent)
		if err != nil {
			log.Printf("Failed to unmarshal Postmark event: %v", err)
			http.Error(w, "Failed to process event payload", http.StatusBadRequest)
			return
		}

		err = associatePostmarkEventWithUser(database.GetDB(), postmarkEvent.MessageID, userID, espID)
		if err != nil {
			log.Printf("Failed to associate event with user: %v", err)
			http.Error(w, "Failed to process event", http.StatusInternalServerError)
			return
		}

		message := Message{
			Headers: r.Header,
			Body:    body,
		}

		handleRequest(w, producer, config.WebhookTopics["postmark"], message)
	})

	http.HandleFunc("/webhook-events/socketlabs", func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "Failed to read request body", http.StatusInternalServerError)
			return
		}
		defer r.Body.Close()

		message := Message{
			Headers: r.Header,
			Body:    body,
		}

		handleRequest(w, producer, config.WebhookTopics["socketlabs"], message)
	})

	http.HandleFunc("/healthcheck", func(w http.ResponseWriter, r *http.Request) {
		ctx := logger.WithRequestID(r.Context(), uuid.New().String())
		logger.Info(ctx, "healthcheck", "Health check request received", nil)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status": "healthy"}`))
	})

	// Listen on the specified port
	logger.Info(nil, "main", fmt.Sprintf("Listening on port %s...", config.HTTPPort))
	if err := http.ListenAndServe(":"+config.HTTPPort, nil); err != nil {
		logger.Fatal(nil, "main", "Failed to start HTTP server", err)
	}
}

func handleRequest(w http.ResponseWriter, producer sarama.SyncProducer, topic string, message Message) {
	ctx := logger.WithRequestID(context.Background(), uuid.New().String())
	if message.UserID != 0 {
		ctx = logger.WithUserID(ctx, fmt.Sprintf("%d", message.UserID))
	}

	// Serialize the message to JSON
	messageBytes, err := json.Marshal(message)
	if err != nil {
		logger.Error(ctx, "kafka-producer", "Failed to serialize message", err)
		http.Error(w, "Failed to serialize message", http.StatusInternalServerError)
		return
	}

	// Produce the message to Kafka
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(messageBytes),
	}
	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		logger.Error(ctx, "kafka-producer", "Failed to send message to Kafka", err)
		http.Error(w, "Failed to send message to Kafka", http.StatusInternalServerError)
		return
	}

	// Log success
	logger.Info(ctx, "kafka-producer", "Message sent to Kafka", map[string]interface{}{
		"topic":     topic,
		"partition": partition,
		"offset":    offset,
	})

	// Respond to the client
	response := fmt.Sprintf("Message sent to Kafka topic %s [partition: %d, offset: %d]", topic, partition, offset)
	w.Write([]byte(response))
}

func validateEmailPayload(emailPayload EmailPayload) error {
	// Check required fields
	if emailPayload.From.Email == "" {
		return errors.New("missing required field: from.email")
	}
	if len(emailPayload.Personalizations) == 0 {
		return errors.New("missing required field: personalizations")
	}
	for i, p := range emailPayload.Personalizations {
		if p.To.Email == "" {
			return fmt.Errorf("missing required field: personalizations[%d].to.email", i)
		}
	}
	if len(emailPayload.Content) == 0 {
		return errors.New("missing required field: content")
	}
	hasTextContent := false
	hasHtmlContent := false
	for _, c := range emailPayload.Content {
		if c.Type == "text/plain" {
			hasTextContent = true
		}
		if c.Type == "text/html" {
			hasHtmlContent = true
		}
	}
	if !hasTextContent && !hasHtmlContent {
		return errors.New("either text/plain or text/html content must be provided")
	}

	return nil
}

type Message struct {
	MessageID string
	Email     string
	UserID    int
	Headers   map[string][]string `json:"headers"`
	Body      json.RawMessage     `json:"body"`
}

func decodeBasicAuth(authHeader string) (username, password string, err error) {
	if !strings.HasPrefix(authHeader, "Basic ") {
		return "", "", fmt.Errorf("invalid authorization header")
	}
	payload, err := base64.StdEncoding.DecodeString(authHeader[6:])
	if err != nil {
		return "", "", fmt.Errorf("invalid base64 in authorization header")
	}
	pair := strings.SplitN(string(payload), ":", 2)
	if len(pair) != 2 {
		return "", "", fmt.Errorf("invalid authorization header format")
	}
	return pair[0], pair[1], nil
}

func extractAPIKey(authHeader string) string {
	return strings.TrimPrefix(authHeader, "Bearer ")
}
