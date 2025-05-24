package main

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

type SplunkEvent struct {
	Time       int64                  `json:"time"`
	Host       string                 `json:"host"`
	Source     string                 `json:"source"`
	Sourcetype string                 `json:"sourcetype"`
	Event      map[string]interface{} `json:"event"`
}

type SplunkClient struct {
	httpClient *http.Client
	host       string
	token      string
}

func NewSplunkClient(host, token string) *SplunkClient {
	// Create a custom HTTP client that ignores TLS verification
	client := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
	}

	return &SplunkClient{
		httpClient: client,
		host:       host,
		token:      token,
	}
}

func (c *SplunkClient) SendEvent(ctx context.Context, data []byte, userID int, email string, provider string) error {
	// Unmarshal the data into a slice of maps
	var events []map[string]interface{}
	if err := json.Unmarshal(data, &events); err != nil {
		logger.Error(ctx, "splunk", "Failed to unmarshal data", err)
		return fmt.Errorf("failed to unmarshal data: %v", err)
	}

	// Construct the Splunk URL
	splunkURL := fmt.Sprintf("https://%s.splunkcloud.com:8088/services/collector/event", c.host)

	// Send each event separately
	for _, event := range events {
		// Add userID to the event
		event["user_id"] = userID
		event["sh_username"] = email
		event["provider"] = provider

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
