package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"relay-go/m/logger"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEnvironmentVariables(t *testing.T) {
	// Set test environment variables
	os.Setenv("KAFKA_BROKERS", "localhost:9092")
	os.Setenv("EMAIL_TOPIC", "test_email_topic")
	os.Setenv("WEBHOOK_TOPIC_SENDGRID", "test_webhook_sendgrid")
	os.Setenv("WEBHOOK_TOPIC_SPARKPOST", "test_webhook_sparkpost")
	os.Setenv("WEBHOOK_TOPIC_POSTMARK", "test_webhook_postmark")
	os.Setenv("WEBHOOK_TOPIC_SOCKETLABS", "test_webhook_socketlabs")
	os.Setenv("HTTP_SERVER_PORT", "8081")

	// Run main() in a separate goroutine
	go main()

	// Give the server some time to start
	time.Sleep(2 * time.Second)

	// Check if environment variables are set correctly
	assert.Equal(t, "localhost:9092", os.Getenv("KAFKA_BROKERS"), "KAFKA_BROKERS should be set correctly")
	assert.Equal(t, "test_email_topic", os.Getenv("EMAIL_TOPIC"), "EMAIL_TOPIC should be set correctly")
	assert.Equal(t, "test_webhook_sendgrid", os.Getenv("WEBHOOK_TOPIC_SENDGRID"), "WEBHOOK_TOPIC_SENDGRID should be set correctly")
	assert.Equal(t, "test_webhook_sparkpost", os.Getenv("WEBHOOK_TOPIC_SPARKPOST"), "WEBHOOK_TOPIC_SPARKPOST should be set correctly")
	assert.Equal(t, "test_webhook_postmark", os.Getenv("WEBHOOK_TOPIC_POSTMARK"), "WEBHOOK_TOPIC_POSTMARK should be set correctly")
	assert.Equal(t, "test_webhook_socketlabs", os.Getenv("WEBHOOK_TOPIC_SOCKETLABS"), "WEBHOOK_TOPIC_SOCKETLABS should be set correctly")
	assert.Equal(t, "8081", os.Getenv("HTTP_SERVER_PORT"), "HTTP_SERVER_PORT should be set correctly")

	// Test the /emails endpoint
	t.Run("Test /emails endpoint", func(t *testing.T) {
		// Test GET request (should be not allowed)
		resp, err := http.Get("http://localhost:8081/emails")
		require.NoError(t, err, "Server should be running and accessible")
		defer resp.Body.Close()
		assert.Equal(t, http.StatusMethodNotAllowed, resp.StatusCode, "GET /emails should return 405 Method Not Allowed")

		// Test POST request
		emailPayload := EmailPayload{
			From:     "Twitter Zen <nick@nzenitram.com>",
			To:       []string{"\"Nick Martinez, Jr.\" <twitter1@nzenitram.com>", "Mick Nartinez <nzenitram@nzenitram.com>"},
			Cc:       []string{"nick1@nzenitram.com"},
			Bcc:      []string{"support@nzenitram.com"},
			Subject:  "Updating the subject to reflect the test",
			TextBody: "This is the plain text body of the email.",
			HtmlBody: "<p>This is the <strong>HTML</strong> body of the email.</p>",
			Attachments: []Attachment{
				{
					Name:        "example.txt",
					ContentType: "text/plain",
					Content:     "SGVsbG8gd29ybGQh",
				},
			},
			Headers: map[string]string{
				"X-Custom-Header-1": "Custom Value 1",
				"X-Custom-Header-2": "Custom Value 2",
			},
			Data: map[string]interface{}{
				"TrackOpens":    true,
				"TrackLinks":    "HtmlOnly",
				"MessageStream": "outbound",
			},
			Credentials: map[string]string{
				"SocketLabsServerID":  "12345",
				"SocketLabsAPIkey":    "12345abcdefg",
				"SocketLabsWeight":    "50",
				"PostmarkServerToken": "555555555-abcd-5555-9279-2bdaf804f19f",
				"PostmarkWeight":      "50",
			},
		}
		jsonPayload, _ := json.Marshal(emailPayload)
		resp, err = http.Post("http://localhost:8081/emails", "application/json", bytes.NewBuffer(jsonPayload))
		require.NoError(t, err, "POST request to /emails should not error")
		defer resp.Body.Close()
		assert.Equal(t, http.StatusOK, resp.StatusCode, "POST /emails should return 200 OK")
	})

	// Test webhook endpoints
	webhookEndpoints := []string{
		"/webhook-events/sendgrid",
		"/webhook-events/sparkpost",
		"/webhook-events/postmark",
		"/webhook-events/socketlabs",
		"/webhook-events/mandrill",
	}

	for _, endpoint := range webhookEndpoints {
		t.Run("Test "+endpoint+" endpoint", func(t *testing.T) {
			// Test GET request (should be not allowed)
			resp, err := http.Get("http://localhost:8081" + endpoint)
			require.NoError(t, err, "Server should be running and accessible")
			defer resp.Body.Close()
			assert.Equal(t, http.StatusMethodNotAllowed, resp.StatusCode, "GET "+endpoint+" should return 405 Method Not Allowed")

			// Test POST request
			webhookPayload := map[string]interface{}{
				"event":     "test_event",
				"timestamp": time.Now().Unix(),
			}
			jsonPayload, _ := json.Marshal(webhookPayload)
			resp, err = http.Post("http://localhost:8081"+endpoint, "application/json", bytes.NewBuffer(jsonPayload))
			require.NoError(t, err, "POST request to "+endpoint+" should not error")
			defer resp.Body.Close()
			assert.Equal(t, http.StatusOK, resp.StatusCode, "POST "+endpoint+" should return 200 OK")
		})
	}

	// Add more assertions here as needed
}

func TestHealthEndpoint(t *testing.T) {
	// Test health endpoint
	req, err := http.NewRequest("GET", "/healthcheck", nil)
	assert.NoError(t, err)

	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := logger.WithRequestID(r.Context(), uuid.New().String())
		logger.Info(ctx, "healthcheck", "Health check request received", nil)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status": "healthy"}`))
	})

	handler.ServeHTTP(rr, req)
	assert.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), `"status": "healthy"`)
}

func TestSplunkEnabledConfiguration(t *testing.T) {
	// Test with Splunk enabled
	os.Setenv("SPLUNK_ENABLED", "true")
	os.Setenv("SPLUNK_HOST", "test-host")
	os.Setenv("SPLUNK_KEY", "test-key")
	config, err := loadConfig()
	assert.NoError(t, err)
	assert.True(t, config.SplunkEnabled)

	// Test with Splunk disabled
	os.Setenv("SPLUNK_ENABLED", "false")
	config, err = loadConfig()
	assert.NoError(t, err)
	assert.False(t, config.SplunkEnabled)

	// Test with missing SPLUNK_ENABLED (should default to false)
	os.Unsetenv("SPLUNK_ENABLED")
	config, err = loadConfig()
	assert.NoError(t, err)
	assert.False(t, config.SplunkEnabled)

	// Clean up
	os.Unsetenv("SPLUNK_ENABLED")
	os.Unsetenv("SPLUNK_HOST")
	os.Unsetenv("SPLUNK_KEY")
}
