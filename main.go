package main

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"relay-go/m/database"
	"strings"

	"github.com/IBM/sarama"
	"github.com/google/uuid"
	"github.com/joho/godotenv"
)

// var batchProcessor *BatchProcessor

func init() {
	envErr := godotenv.Load()
	if envErr != nil {
		log.Println("No .env file found, using system environment variables")
	}

	redisAddr := os.Getenv("REDIS_HOST")
	if redisAddr == "" {
		log.Fatal("REDIS_HOST environment variable is not set")
	}

	// redisPass := os.Getenv("REDIS_PASSWORD")
	// if redisAddr == "" {
	// 	log.Fatal("REDIS_HOST environment variable is not set")
	// }

	kafkaBrokers := []string{os.Getenv("KAFKA_BROKERS")}
	if len(kafkaBrokers) == 0 {
		log.Fatal("KAFKA_BROKERS environment variable is not set")
	}

	// database.InitDB()
	// db := database.GetDB()
	// Initialize your database connection, Redis address, and Kafka brokers

	// var err error
	// batchProcessor, err = NewBatchProcessor(db, redisAddr, redisPass, kafkaBrokers)
	// if err != nil {
	// 	log.Fatalf("Failed to create batch processor: %v", err)
	// }

	// // Start processing scheduled emails in a separate goroutine
	// go batchProcessor.ProcessScheduledEmails()
}

func main() {
	port := os.Getenv("HTTP_SERVER_PORT")
	if port == "" {
		log.Fatal("HTTP_SERVER_PORT environment variable is not set")
	}

	kafkaBrokers := []string{os.Getenv("KAFKA_BROKERS")}
	if len(kafkaBrokers) == 0 {
		log.Fatal("KAFKA_BROKERS environment variable is not set")
	}

	emailTopic := os.Getenv("EMAIL_TOPIC")
	if emailTopic == "" {
		log.Fatal("EMAIL_TOPIC environment variable is not set")
	}

	webhookTopicSendGrid := os.Getenv("WEBHOOK_TOPIC_SENDGRID")
	if webhookTopicSendGrid == "" {
		log.Fatal("WEBHOOK_TOPIC_SENDGRID environment variable is not set")
	}

	webhookTopicSparkpost := os.Getenv("WEBHOOK_TOPIC_SPARKPOST")
	if webhookTopicSparkpost == "" {
		log.Fatal("WEBHOOK_TOPIC_SPARKPOST environment variable is not set")
	}

	webhookTopicPostmark := os.Getenv("WEBHOOK_TOPIC_POSTMARK")
	if webhookTopicPostmark == "" {
		log.Fatal("WEBHOOK_TOPIC_POSTMARK environment variable is not set")
	}

	webhookTopicSocketlabs := os.Getenv("WEBHOOK_TOPIC_SOCKETLABS")
	if webhookTopicSocketlabs == "" {
		log.Fatal("WEBHOOK_TOPIC_SOCKETLABS environment variable is not set")
	}

	var producer sarama.SyncProducer
	var err error

	// Initialize data stores (MySQL+Kafka or DynamoDB)
	database.InitDataStores()

	// Only initialize Kafka if we're in MySQL+Kafka mode
	if database.IsMySQLKafkaMode() {
		// Set up the Kafka producer
		producer, err = sarama.NewSyncProducer(kafkaBrokers, nil)
		if err != nil {
			log.Fatalf("Failed to start Kafka producer: %v", err)
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

		// message := Message{
		// 	MessageID: messageID,
		// 	UserID:    user.ID,
		// 	Body:      body,
		// }

		// if emailPayload.CustomArgs["IsBatch"] == "true" {
		// 	err := batchProcessor.HandleBatchSend(user.ID, message)
		// 	if err != nil {
		// 		http.Error(w, fmt.Sprintf("Failed to handle batch send: %v", err), http.StatusInternalServerError)
		// 		return
		// 	}
		// 	w.WriteHeader(http.StatusAccepted)
		// 	w.Write([]byte(fmt.Sprintf(`{"message": "Batch email processing started", "batch_id": %v}`, messageID)))
		// } else {
		// 	handleRequest(w, producer, emailTopic, message)
		// }
	})

	// Set up the HTTP server for webhooks
	http.HandleFunc("/webhook-events/sendgrid", func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "Failed to read request body", http.StatusInternalServerError)
			return
		}
		defer r.Body.Close()

		var userID int
		var verifyErr error

		// Check which data store is available and use appropriate verification
		if database.IsMySQLKafkaMode() {
			userID, verifyErr = verifySendgridWebhookAndFindUser(database.GetDB(), body, r.Header)
		} else {
			userID, verifyErr = verifySendgridWebhookAndFindUserDynamoDB(database.GetDynamoClient(), body, r.Header)
		}

		if verifyErr != nil {
			log.Printf("Sendgrid - Failed to verify webhook: %v", verifyErr)
			http.Error(w, "Failed to verify webhook", http.StatusUnauthorized)
			return
		}

		message := Message{
			Headers: r.Header,
			Body:    body,
			UserID:  userID,
		}

		// Process events based on available data store
		if database.IsMySQLKafkaMode() {
			// Full mode: Process with MySQL and Kafka
			var events []json.RawMessage
			if err := json.Unmarshal(message.Body, &events); err != nil {
				log.Printf("Failed to unmarshal message body: %v", err)
				http.Error(w, "Failed to process event payload", http.StatusBadRequest)
				return
			}

			for _, eventData := range events {
				var sgEventBody SendGridEventBody
				if err := json.Unmarshal(eventData, &sgEventBody); err != nil {
					log.Printf("Failed to unmarshal SendGridEventBody: %v", err)
					continue
				}
				if err := associateSendgridEventWithUser(database.GetDB(), sgEventBody, userID); err != nil {
					log.Printf("Failed to associate event with user: %v", err)
					// Continue processing other events
				}
			}

			// Send to Kafka
			handleRequest(w, producer, os.Getenv("WEBHOOK_TOPIC_SENDGRID"), message)
		} else {
			// print userID
			log.Printf("UserID: %v", userID)
			// Light mode: Send directly to Splunk
			if err := sendToSplunkHEC(body, userID); err != nil {
				log.Printf("Failed to send to Splunk: %v", err)
				http.Error(w, "Failed to process webhook", http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusOK)
		}
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

		handleRequest(w, producer, webhookTopicSparkpost, message)
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

		handleRequest(w, producer, webhookTopicPostmark, message)
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

		handleRequest(w, producer, webhookTopicSocketlabs, message)
	})

	http.HandleFunc("/healthcheck", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status": "healthy"}`))
	})

	// Listen on the specified port
	log.Printf("Listening on port %s...", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatalf("Failed to start HTTP server: %v", err)
	}
}

func handleRequest(w http.ResponseWriter, producer sarama.SyncProducer, topic string, message Message) {
	// Serialize the message to JSON
	messageBytes, err := json.Marshal(message)
	if err != nil {
		http.Error(w, "Failed to serialize message", http.StatusInternalServerError)
		log.Printf("Failed to serialize message: %v", err)
		return
	}

	// Produce the message to Kafka
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(messageBytes),
	}
	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		http.Error(w, "Failed to send message to Kafka", http.StatusInternalServerError)
		log.Printf("Failed to send message to Kafka: %v", err)
		return
	}

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
