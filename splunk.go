package main

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
)

func sendToSplunkHEC(data []byte, userID int) error {
	// Retrieve environment variables
	splunkHost := os.Getenv("SPLUNK_HOST")
	if splunkHost == "" {
		log.Fatal("SPLUNK_HOST environment variable is not set")
	}
	splunkToken := os.Getenv("SPLUNK_KEY")
	if splunkToken == "" {
		log.Fatal("SPLUNK_KEY environment variable is not set")
	}

	// Construct the Splunk URL using the environment variable
	splunkURL := fmt.Sprintf("https://%s.splunkcloud.com:8088/services/collector/event", splunkHost)

	// Unmarshal the data into a slice of maps
	var events []map[string]interface{}
	if err := json.Unmarshal(data, &events); err != nil {
		return fmt.Errorf("failed to unmarshal data: %v", err)
	}

	// Create a custom HTTP client that ignores TLS verification
	client := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
	}

	// Send each event separately
	for _, event := range events {
		// Add userID to the event
		event["user_id"] = userID

		// Create the request body
		requestBody := map[string]interface{}{
			"event": event,
		}
		jsonBody, err := json.Marshal(requestBody)
		if err != nil {
			return fmt.Errorf("failed to marshal request body: %v", err)
		}

		// Create the HTTP request
		req, err := http.NewRequest("POST", splunkURL, bytes.NewBuffer(jsonBody))
		if err != nil {
			return fmt.Errorf("failed to create request: %v", err)
		}

		// Set headers
		req.Header.Set("Authorization", "Splunk "+splunkToken)
		req.Header.Set("Content-Type", "application/json")

		// Send the request
		resp, err := client.Do(req)
		if err != nil {
			return fmt.Errorf("failed to send request: %v", err)
		}
		defer resp.Body.Close()

		// Check the response
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
		}
	}

	return nil
}
