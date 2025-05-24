package webhook

import (
	"context"
	"encoding/json"
	"relay-go/m/logger"

	"github.com/IBM/sarama"
)

// EventProcessor defines the interface for processing webhook events
type EventProcessor interface {
	ProcessEvent(ctx context.Context, event json.RawMessage, userID int64, email string) error
}

// KafkaEventProcessor handles events using Kafka
type KafkaEventProcessor struct {
	producer sarama.SyncProducer
	topic    string
}

// NewKafkaEventProcessor creates a new Kafka event processor
func NewKafkaEventProcessor(producer sarama.SyncProducer, topic string) *KafkaEventProcessor {
	return &KafkaEventProcessor{
		producer: producer,
		topic:    topic,
	}
}

// ProcessEvent implements EventProcessor for Kafka
func (p *KafkaEventProcessor) ProcessEvent(ctx context.Context, event json.RawMessage, userID int64, email string) error {
	message := Message{
		UserID: int(userID),
		Email:  email,
		Body:   event,
	}

	// Serialize the message to JSON
	messageBytes, err := json.Marshal(message)
	if err != nil {
		return err
	}

	// Produce the message to Kafka
	msg := &sarama.ProducerMessage{
		Topic: p.topic,
		Value: sarama.StringEncoder(messageBytes),
	}
	_, _, err = p.producer.SendMessage(msg)
	return err
}

// SplunkEventProcessor handles events using Splunk
type SplunkEventProcessor struct {
	splunkClient *SplunkClient
	provider     string
}

// NewSplunkEventProcessor creates a new Splunk event processor
func NewSplunkEventProcessor(splunkClient *SplunkClient, provider string) *SplunkEventProcessor {
	return &SplunkEventProcessor{
		splunkClient: splunkClient,
		provider:     provider,
	}
}

// ProcessEvent implements EventProcessor for Splunk
func (p *SplunkEventProcessor) ProcessEvent(ctx context.Context, event json.RawMessage, userID int64, email string) error {
	return p.splunkClient.SendEvent(ctx, event, int(userID), email, p.provider)
}

// Message represents a webhook event message
type Message struct {
	UserID int             `json:"user_id"`
	Email  string          `json:"email"`
	Body   json.RawMessage `json:"body"`
}

// ProcessWebhookEvents processes a batch of webhook events
func ProcessWebhookEvents(ctx context.Context, events []json.RawMessage, userID int64, email string, processor EventProcessor) error {
	for _, event := range events {
		if err := processor.ProcessEvent(ctx, event, userID, email); err != nil {
			logger.Error(ctx, "webhook-processor", "Failed to process event", err, map[string]interface{}{
				"user_id": userID,
				"email":   email,
			})
			// Continue processing other events
		}
	}
	return nil
}

// CompositeProcessor implements EventProcessor by sending events to multiple processors
type CompositeProcessor struct {
	processors []EventProcessor
}

// NewCompositeProcessor creates a new CompositeProcessor
func NewCompositeProcessor(processors ...EventProcessor) *CompositeProcessor {
	return &CompositeProcessor{
		processors: processors,
	}
}

// ProcessEvent implements the EventProcessor interface by sending the event to all processors
func (p *CompositeProcessor) ProcessEvent(ctx context.Context, event json.RawMessage, userID int64, email string) error {
	var lastErr error
	for _, processor := range p.processors {
		if err := processor.ProcessEvent(ctx, event, userID, email); err != nil {
			lastErr = err
			// Continue processing with other processors even if one fails
		}
	}
	return lastErr
}
