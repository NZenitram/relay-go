package webhook

import (
	"context"
	"encoding/json"
	"relay-go/m/logger"
)

// SparkPostEventExtractor extracts individual events from SparkPost payload
type SparkPostEventExtractor struct{}

// NewSparkPostEventExtractor creates a new SparkPost event extractor
func NewSparkPostEventExtractor() *SparkPostEventExtractor {
	return &SparkPostEventExtractor{}
}

// ExtractEvents extracts individual events from SparkPost payload and returns them as json.RawMessage slice
func (e *SparkPostEventExtractor) ExtractEvents(ctx context.Context, payload []byte) ([]json.RawMessage, error) {
	// Parse only the structure we need to navigate the nested format
	var sparkPostPayload []struct {
		Msys json.RawMessage `json:"msys"`
	}

	if err := json.Unmarshal(payload, &sparkPostPayload); err != nil {
		return nil, err
	}

	var events []json.RawMessage

	for i, envelope := range sparkPostPayload {
		// Parse the msys object to find which event type exists
		var msys map[string]json.RawMessage
		if err := json.Unmarshal(envelope.Msys, &msys); err != nil {
			logger.Error(ctx, "sparkpost-processor", "Failed to parse msys object", err, map[string]interface{}{
				"index": i,
			})
			continue
		}

		// Find the first non-null event type and use its raw JSON
		var eventFound bool
		for _, eventData := range msys {
			// Check if this field contains actual event data (not null)
			if len(eventData) > 4 { // More than just "null"
				events = append(events, eventData)
				eventFound = true
				break
			}
		}

		if !eventFound {
			logger.Warning(ctx, "sparkpost-processor", "Event has no recognized event type", nil, map[string]interface{}{
				"index": i,
			})
		}
	}

	return events, nil
}
