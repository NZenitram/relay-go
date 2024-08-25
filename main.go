package main

import (
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

	kafkaTopic := os.Getenv("KAFKA_TOPIC")
	if kafkaTopic == "" {
		log.Fatal("KAFKA_TOPIC environment variable is not set")
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
	// maxAttempts := 10
	// attempt := 0
	// var producer sarama.SyncProducer
	// var kafka_conn_err error

	// for attempt < maxAttempts {
	// 	attempt++
	// 	producer, kafka_conn_err = sarama.NewSyncProducer(kafkaBrokers, nil)
	// 	if kafka_conn_err == nil {
	// 		log.Println("Kafka producer started successfully")
	// 	}
	// 	log.Printf("Attempt %d: Failed to start Kafka producer: %v", attempt, err)
	// 	time.Sleep(2 * time.Second) // Wait before retrying
	// }

	// if err != nil {
	// 	log.Fatalf("Failed to start Kafka producer after %d attempts: %v", maxAttempts, err)
	// }

	// defer producer.Close()

	// Set up the HTTP server
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		// Read the request body
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "Failed to read request body", http.StatusInternalServerError)
			return
		}
		defer r.Body.Close()

		// Produce the message to Kafka
		msg := &sarama.ProducerMessage{
			Topic: kafkaTopic,
			Value: sarama.StringEncoder(body),
		}
		partition, offset, err := producer.SendMessage(msg)
		if err != nil {
			http.Error(w, "Failed to send message to Kafka", http.StatusInternalServerError)
			log.Printf("Failed to send message to Kafka: %v", err)
			return
		}

		// Respond to the client
		response := fmt.Sprintf("Message sent to Kafka topic %s [partition: %d, offset: %d]", kafkaTopic, partition, offset)
		w.Write([]byte(response))
	})

	// Listen on port 8888
	log.Println("Listening on port 8888...")
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatalf("Failed to start HTTP server: %v", err)
	}
}
