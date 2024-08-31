package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"

	"github.com/IBM/sarama"
	"github.com/joho/godotenv"
)

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Println("No .env file found, using system environment variables")
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

	port := os.Getenv("HTTP_SERVER_PORT")
	if port == "" {
		log.Fatal("HTTP_SERVER_PORT environment variable is not set")
	}

	// Set up the Kafka producer
	producer, err := sarama.NewSyncProducer(kafkaBrokers, nil)
	if err != nil {
		log.Fatalf("Failed to start Kafka producer: %v", err)
	}
	defer producer.Close()

	// Set up the HTTP server for emails
	http.HandleFunc("/emails", func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "Failed to read request body", http.StatusInternalServerError)
			return
		}
		defer r.Body.Close()

		// Validate the email payload
		if err := validateEmailPayload(body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		message := Message{
			Headers: r.Header,
			Body:    body,
		}

		handleRequest(w, producer, emailTopic, message)
	})

	// Set up the HTTP server for webhooks
	http.HandleFunc("/webhook-events/sendgrid", func(w http.ResponseWriter, r *http.Request) {
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

		handleRequest(w, producer, webhookTopicSendGrid, message)
	})

	http.HandleFunc("/webhook-events/sparkpost", func(w http.ResponseWriter, r *http.Request) {
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

		handleRequest(w, producer, webhookTopicSparkpost, message)
	})

	http.HandleFunc("/webhook-events/postmark", func(w http.ResponseWriter, r *http.Request) {
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

	// Listen on the specified port
	log.Printf("Listening on port %s...", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatalf("Failed to start HTTP server: %v", err)
	}

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

func validateEmailPayload(payload []byte) error {
	var emailPayload EmailPayload
	if err := json.Unmarshal(payload, &emailPayload); err != nil {
		return fmt.Errorf("invalid JSON: %v", err)
	}

	// Check required fields
	if emailPayload.From == "" {
		return errors.New("missing required field: from")
	}
	if len(emailPayload.To) == 0 {
		return errors.New("missing required field: to")
	}
	if emailPayload.Subject == "" {
		return errors.New("missing required field: subject")
	}
	if emailPayload.TextBody == "" && emailPayload.HtmlBody == "" {
		return errors.New("either textbody or htmlbody must be provided")
	}
	if emailPayload.Credentials == nil {
		return errors.New("missing required field: credentials")
	}

	// Check provider-specific credentials
	if emailPayload.Credentials["SocketLabsServerID"] == "" && emailPayload.Credentials["SocketLabsAPIkey"] != "" {
		return errors.New("unexpected SocketLabs API key without 'SocketLabsServerID'. Please provide both or none")
	}
	if emailPayload.Credentials["SocketLabsServerID"] != "" && emailPayload.Credentials["SocketLabsAPIkey"] == "" {
		return errors.New("unexpected SocketLabs Server ID without 'SocketLabsAPIkey'. Please provide both or none")
	}

	if emailPayload.Credentials["PostmarkServerToken"] == "" && emailPayload.Credentials["PostmarkAPIURL"] != "" {
		return errors.New("unexpected Postmark API URL without 'PostmarkServerToken'. Please provide both or none")
	}
	if emailPayload.Credentials["PostmarkServerToken"] != "" && emailPayload.Credentials["PostmarkAPIURL"] == "" {
		return errors.New("unexpected Postmark Server Token without 'PostmarkAPIURL'. Please provide both or none")
	}

	if emailPayload.Credentials["SendGridAPIKey"] != "" {
		return errors.New("unexpected SendGrid credentials. 'SendGridAPIKey' should not be provided")
	}
	return nil
}

type EmailPayload struct {
	From        string                 `json:"from"`
	To          []string               `json:"to"`
	Cc          []string               `json:"cc"`
	Bcc         []string               `json:"bcc"`
	Subject     string                 `json:"subject"`
	TextBody    string                 `json:"textbody"`
	HtmlBody    string                 `json:"htmlbody"`
	Attachments []Attachment           `json:"attachments"`
	Headers     map[string]string      `json:"headers"`
	Data        map[string]interface{} `json:"data"`
	Credentials map[string]string      `json:"credentials"`
}

type Attachment struct {
	Name        string `json:"name"`
	ContentType string `json:"contenttype"`
	Content     string `json:"content"`
}

type Message struct {
	Headers map[string][]string `json:"headers"`
	Body    json.RawMessage     `json:"body"`
}
