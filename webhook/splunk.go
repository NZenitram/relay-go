package webhook

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"relay-go/m/logger"
)

// SplunkClient handles sending events to Splunk
type SplunkClient struct {
	host       string
	token      string
	httpClient *http.Client
}

// NewSplunkClient creates a new Splunk client
func NewSplunkClient(host, token string) *SplunkClient {
	// Create a custom transport with TLS configuration
	transport := &http.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true, // Skip certificate verification for Splunk
		},
	}

	return &SplunkClient{
		host:       host,
		token:      token,
		httpClient: &http.Client{Transport: transport},
	}
}

// SendEvent sends an event to Splunk
func (c *SplunkClient) SendEvent(ctx context.Context, data []byte, userID int, email string) error {
	// Try to unmarshal as a single event first
	var singleEvent map[string]interface{}
	if err := json.Unmarshal(data, &singleEvent); err == nil {
		// It's a single event, wrap it in a slice
		return c.sendEventsToSplunk(ctx, []map[string]interface{}{singleEvent}, userID, email)
	}

	// Try to unmarshal as an array of events
	var events []map[string]interface{}
	if err := json.Unmarshal(data, &events); err != nil {
		logger.Error(ctx, "splunk", "Failed to unmarshal data", err)
		return fmt.Errorf("failed to unmarshal data: %v", err)
	}

	return c.sendEventsToSplunk(ctx, events, userID, email)
}

// sendEventsToSplunk sends a slice of events to Splunk
func (c *SplunkClient) sendEventsToSplunk(ctx context.Context, events []map[string]interface{}, userID int, email string) error {
	// Construct the Splunk URL
	splunkURL := fmt.Sprintf("https://%s.splunkcloud.com:8088/services/collector/event", c.host)

	// Send each event separately
	for _, event := range events {
		// Add userID to the event
		event["user_id"] = userID
		event["sh_username"] = email

		// Create the request body
		requestBody := map[string]interface{}{
			"event": event,
		}
		jsonBody, err := json.Marshal(requestBody)
		if err != nil {
			logger.Error(ctx, "splunk", "Failed to marshal request body", err)
			return fmt.Errorf("failed to marshal request body: %v", err)
		}

		// Create the HTTP request
		req, err := http.NewRequestWithContext(ctx, "POST", splunkURL, bytes.NewBuffer(jsonBody))
		if err != nil {
			logger.Error(ctx, "splunk", "Failed to create request", err)
			return fmt.Errorf("failed to create request: %v", err)
		}

		// Set headers
		req.Header.Set("Authorization", "Splunk "+c.token)
		req.Header.Set("Content-Type", "application/json")

		// Send the request
		resp, err := c.httpClient.Do(req)
		if err != nil {
			logger.Error(ctx, "splunk", "Failed to send request", err)
			return fmt.Errorf("failed to send request: %v", err)
		}
		defer resp.Body.Close()

		// Check the response
		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			logger.Error(ctx, "splunk", "Received non-200 response", fmt.Errorf("status: %d, body: %s", resp.StatusCode, string(body)))
			return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
		}
	}

	logger.Info(ctx, "splunk", "Successfully sent all events to Splunk", nil)
	return nil
}
