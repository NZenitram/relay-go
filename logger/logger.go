package logger

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"runtime"
	"time"
)

// LogLevel represents the severity level of a log message
type LogLevel string

const (
	DEBUG   LogLevel = "DEBUG"
	INFO    LogLevel = "INFO"
	WARNING LogLevel = "WARNING"
	ERROR   LogLevel = "ERROR"
	FATAL   LogLevel = "FATAL"
)

// LogEntry represents a structured log entry
type LogEntry struct {
	Timestamp   string                 `json:"timestamp"`
	Level       LogLevel               `json:"level"`
	Message     string                 `json:"message"`
	RequestID   string                 `json:"request_id,omitempty"`
	UserID      string                 `json:"user_id,omitempty"`
	Service     string                 `json:"service"`
	Component   string                 `json:"component"`
	File        string                 `json:"file"`
	Line        int                    `json:"line"`
	Error       string                 `json:"error,omitempty"`
	ExtraFields map[string]interface{} `json:"extra_fields,omitempty"`
}

var (
	serviceName = "relay-go"
	// Global logger level
	currentLevel LogLevel = INFO
)

// SetServiceName sets the service name for all log entries
func SetServiceName(name string) {
	serviceName = name
}

// SetLevel sets the global logger level
func SetLevel(level LogLevel) {
	currentLevel = level
}

// GetLevel returns the current logger level
func GetLevel() LogLevel {
	return currentLevel
}

// writeLog writes a structured log entry
func writeLog(ctx context.Context, level LogLevel, component string, message string, err error, requestID string, userID string, extraFields map[string]interface{}) {
	// Skip if the message level is lower than the current level
	if level < currentLevel {
		return
	}

	// Get caller information
	_, file, line, _ := runtime.Caller(2)

	entry := LogEntry{
		Timestamp:   time.Now().UTC().Format(time.RFC3339),
		Level:       level,
		Message:     message,
		RequestID:   requestID,
		UserID:      userID,
		Service:     serviceName,
		Component:   component,
		File:        file,
		Line:        line,
		ExtraFields: extraFields,
	}

	if err != nil {
		entry.Error = err.Error()
	}

	// Convert to JSON
	jsonBytes, err := json.Marshal(entry)
	if err != nil {
		// Fallback to standard logging if JSON marshaling fails
		log.Printf("Failed to marshal log entry: %v", err)
		return
	}

	// Write to stdout
	fmt.Println(string(jsonBytes))
}

// Debug logs a debug message
func Debug(ctx context.Context, component string, message string, extraFields ...map[string]interface{}) {
	requestID := getRequestIDFromContext(ctx)
	userID := getUserIDFromContext(ctx)
	fields := mergeExtraFields(extraFields...)
	writeLog(ctx, DEBUG, component, message, nil, requestID, userID, fields)
}

// Info logs an info message
func Info(ctx context.Context, component string, message string, extraFields ...map[string]interface{}) {
	requestID := getRequestIDFromContext(ctx)
	userID := getUserIDFromContext(ctx)
	fields := mergeExtraFields(extraFields...)
	writeLog(ctx, INFO, component, message, nil, requestID, userID, fields)
}

// Warning logs a warning message
func Warning(ctx context.Context, component string, message string, err error, extraFields ...map[string]interface{}) {
	requestID := getRequestIDFromContext(ctx)
	userID := getUserIDFromContext(ctx)
	fields := mergeExtraFields(extraFields...)
	writeLog(ctx, WARNING, component, message, err, requestID, userID, fields)
}

// Error logs an error message
func Error(ctx context.Context, component string, message string, err error, extraFields ...map[string]interface{}) {
	requestID := getRequestIDFromContext(ctx)
	userID := getUserIDFromContext(ctx)
	fields := mergeExtraFields(extraFields...)
	writeLog(ctx, ERROR, component, message, err, requestID, userID, fields)
}

// Fatal logs a fatal message and exits the program
func Fatal(ctx context.Context, component string, message string, err error, extraFields ...map[string]interface{}) {
	requestID := getRequestIDFromContext(ctx)
	userID := getUserIDFromContext(ctx)
	fields := mergeExtraFields(extraFields...)
	writeLog(ctx, FATAL, component, message, err, requestID, userID, fields)
	os.Exit(1)
}

// Context keys
type contextKey string

const (
	requestIDKey contextKey = "request_id"
	userIDKey    contextKey = "user_id"
)

// WithRequestID adds a request ID to the context
func WithRequestID(ctx context.Context, requestID string) context.Context {
	return context.WithValue(ctx, requestIDKey, requestID)
}

// WithUserID adds a user ID to the context
func WithUserID(ctx context.Context, userID string) context.Context {
	return context.WithValue(ctx, userIDKey, userID)
}

// getRequestIDFromContext retrieves the request ID from the context
func getRequestIDFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	if requestID, ok := ctx.Value(requestIDKey).(string); ok {
		return requestID
	}
	return ""
}

// getUserIDFromContext retrieves the user ID from the context
func getUserIDFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	if userID, ok := ctx.Value(userIDKey).(string); ok {
		return userID
	}
	return ""
}

// mergeExtraFields merges multiple extra fields maps
func mergeExtraFields(fields ...map[string]interface{}) map[string]interface{} {
	if len(fields) == 0 {
		return nil
	}

	result := make(map[string]interface{})
	for _, field := range fields {
		for k, v := range field {
			result[k] = v
		}
	}
	return result
}
