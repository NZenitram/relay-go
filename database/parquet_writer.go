package database

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
	
	"relay-go/m/logger"
)

// EnhancedParquetEvent captures ALL fields from JSON events to prevent data loss
// Based on analysis of real SendGrid data showing 41+ unique fields
type EnhancedParquetEvent struct {
	// ============ CORE FIELDS (100% occurrence) ============
	EventID      string `parquet:"name=event_id, type=BYTE_ARRAY, convertedtype=UTF8"`
	EventType    string `parquet:"name=event_type, type=BYTE_ARRAY, convertedtype=UTF8"`
	Timestamp    int64  `parquet:"name=timestamp, type=INT64"`
	Provider     string `parquet:"name=provider, type=BYTE_ARRAY, convertedtype=UTF8"`
	Email        string `parquet:"name=email, type=BYTE_ARRAY, convertedtype=UTF8"`
	
	// ============ USER/ACCOUNT INFO ============
	UserID       int64  `parquet:"name=user_id, type=INT64"`
	Username     string `parquet:"name=sh_username, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=OPTIONAL"`
	AccountID    int64  `parquet:"name=account_id, type=INT64, repetitiontype=OPTIONAL"`
	ContactID    int64  `parquet:"name=contact_id, type=INT64, repetitiontype=OPTIONAL"`
	
	// ============ SENDGRID SPECIFIC (100% in SG events) ============
	SGEventID    string `parquet:"name=sg_event_id, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=OPTIONAL"`
	SGMessageID  string `parquet:"name=sg_message_id, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=OPTIONAL"`
	
	// ============ EMAIL METADATA ============
	FromEmail    string `parquet:"name=from_email, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=OPTIONAL"`
	MessageID    string `parquet:"name=message_id, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=OPTIONAL"`
	SMTPID       string `parquet:"name=smtp_id, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=OPTIONAL"`
	Subject      string `parquet:"name=subject, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=OPTIONAL"`
	EmailID      int64  `parquet:"name=email_id, type=INT64, repetitiontype=OPTIONAL"`
	
	// ============ CATEGORIES & TEMPLATES ============
	Categories   string `parquet:"name=categories, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=OPTIONAL"`
	TemplateID   int64  `parquet:"name=template_id, type=INT64, repetitiontype=OPTIONAL"`
	EmailTemplateID int64 `parquet:"name=email_template_id, type=INT64, repetitiontype=OPTIONAL"`
	SGTemplateID string `parquet:"name=sg_template_id, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=OPTIONAL"`
	SGTemplateName string `parquet:"name=sg_template_name, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=OPTIONAL"`
	
	// ============ AUTOMATION/ACTION PLAN ============
	AutomationID     int64  `parquet:"name=automation_id, type=INT64, repetitiontype=OPTIONAL"`
	AutomationStepID int64  `parquet:"name=automation_step_id, type=INT64, repetitiontype=OPTIONAL"`
	ActionPlanID     int64  `parquet:"name=action_plan_id, type=INT64, repetitiontype=OPTIONAL"`
	ActionPlanStepID int64  `parquet:"name=action_plan_step_id, type=INT64, repetitiontype=OPTIONAL"`
	ActionPlanStepContactIDs string `parquet:"name=action_plan_step_contact_ids, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=OPTIONAL"`
	AutomationContactLogIDs string `parquet:"name=automation_contact_log_ids, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=OPTIONAL"`
	
	// ============ DELIVERY & ERROR INFO ============
	Response     string `parquet:"name=response, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=OPTIONAL"`
	Reason       string `parquet:"name=reason, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=OPTIONAL"`
	ErrorCode    string `parquet:"name=error_code, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=OPTIONAL"`
	BounceType   string `parquet:"name=bounce_type, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=OPTIONAL"`
	Status       string `parquet:"name=status, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=OPTIONAL"`
	Attempt      int32  `parquet:"name=attempt, type=INT32, repetitiontype=OPTIONAL"`
	TLS          int32  `parquet:"name=tls, type=INT32, repetitiontype=OPTIONAL"`
	CertErr      bool   `parquet:"name=cert_err, type=BOOLEAN, repetitiontype=OPTIONAL"`
	
	// ============ CLICK/OPEN TRACKING ============
	URL          string `parquet:"name=url, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=OPTIONAL"`
	URLOffset    string `parquet:"name=url_offset, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=OPTIONAL"`
	SGContentType string `parquet:"name=sg_content_type, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=OPTIONAL"`
	SGMachineOpen bool  `parquet:"name=sg_machine_open, type=BOOLEAN, repetitiontype=OPTIONAL"`
	
	// ============ DEVICE/LOCATION ============
	IPAddress    string `parquet:"name=ip_address, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=OPTIONAL"`
	UserAgent    string `parquet:"name=user_agent, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=OPTIONAL"`
	Country      string `parquet:"name=country, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=OPTIONAL"`
	City         string `parquet:"name=city, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=OPTIONAL"`
	Domain       string `parquet:"name=domain, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=OPTIONAL"`
	
	// ============ UNSUBSCRIBE/GROUP MANAGEMENT ============
	ASMGroupID   int64  `parquet:"name=asm_group_id, type=INT64, repetitiontype=OPTIONAL"`
	Pool         string `parquet:"name=pool, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=OPTIONAL"`
	
	// ============ MARKETING/CAMPAIGN ============
	MarketingCampaignID   string `parquet:"name=marketing_campaign_id, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=OPTIONAL"`
	MarketingCampaignName string `parquet:"name=marketing_campaign_name, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=OPTIONAL"`
	MCPodID      int64  `parquet:"name=mc_pod_id, type=INT64, repetitiontype=OPTIONAL"`
	MCStats      string `parquet:"name=mc_stats, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=OPTIONAL"`
	PhaseID      string `parquet:"name=phase_id, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=OPTIONAL"`
	
	// ============ SCHEDULING & MISC ============
	SendAt       int64  `parquet:"name=send_at, type=INT64, repetitiontype=OPTIONAL"`
	Newsletter   string `parquet:"name=newsletter, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=OPTIONAL"`
	UniqueID     string `parquet:"name=unique_id, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=OPTIONAL"`
	UniqueArgs   string `parquet:"name=unique_args, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=OPTIONAL"`
	
	// ============ COMPUTED FIELDS FOR ANALYTICS ============
	DayOfWeek    int32  `parquet:"name=day_of_week, type=INT32, repetitiontype=OPTIONAL"`
	HourOfDay    int32  `parquet:"name=hour_of_day, type=INT32, repetitiontype=OPTIONAL"`
	
	// ============ FALLBACK FOR UNMAPPED FIELDS ============
	ExtraFields  string `parquet:"name=extra_fields, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=OPTIONAL"`
}

// ParquetBatchWriter handles writing events to PARQUET format
type ParquetBatchWriter struct {
	s3Client   *s3.Client
	bucketName string
	enabled    bool
}

// NewParquetBatchWriter creates a new PARQUET writer
func NewParquetBatchWriter(s3Client *s3.Client, bucketName string) *ParquetBatchWriter {
	enabled := os.Getenv("ENABLE_PARQUET_OUTPUT") == "true"
	
	if enabled {
		logger.Info(nil, "parquet-writer", "PARQUET output enabled with ENHANCED schema", map[string]interface{}{
			"bucket": bucketName,
			"fields": "50+ fields with full coverage",
		})
	}
	
	return &ParquetBatchWriter{
		s3Client:   s3Client,
		bucketName: bucketName,
		enabled:    enabled,
	}
}

// WriteParquetBatch writes a batch of events to S3 in PARQUET format
func (w *ParquetBatchWriter) WriteParquetBatch(ctx context.Context, events []json.RawMessage, provider string, userID int64, username string) error {
	return w.WriteParquetBatchWithTimestamp(ctx, events, provider, userID, username, time.Time{})
}

// WriteParquetBatchWithTimestamp writes a batch of events to S3 in PARQUET format with optional timestamp preservation
func (w *ParquetBatchWriter) WriteParquetBatchWithTimestamp(ctx context.Context, events []json.RawMessage, provider string, userID int64, username string, preserveTimestamp time.Time) error {
	if !w.enabled {
		return nil // Skip if PARQUET output is disabled
	}
	
	logger.Info(ctx, "parquet-writer", "Starting ENHANCED PARQUET batch write", map[string]interface{}{
		"provider":    provider,
		"user_id":     userID,
		"event_count": len(events),
	})
	
	// Convert raw events to PARQUET events
	parquetEvents := make([]EnhancedParquetEvent, 0, len(events))
	
	for _, rawEvent := range events {
		pe, err := w.convertToEnhancedParquetEvent(rawEvent, provider, userID, username)
		if err != nil {
			logger.Error(ctx, "parquet-writer", "Failed to convert event", err, map[string]interface{}{
				"provider": provider,
				"user_id":  userID,
			})
			continue
		}
		parquetEvents = append(parquetEvents, pe)
	}
	
	if len(parquetEvents) == 0 {
		logger.Warning(ctx, "parquet-writer", "No events to write after conversion", nil, map[string]interface{}{
			"provider": provider,
			"user_id":  userID,
		})
		return nil
	}
	
	// Create temporary file for PARQUET data
	tmpFile := fmt.Sprintf("/tmp/parquet_%d_%s_%d.tmp", userID, provider, time.Now().UnixNano())
	fw, err := local.NewLocalFileWriter(tmpFile)
	if err != nil {
		return fmt.Errorf("failed to create temp file writer: %w", err)
	}
	defer func() {
		fw.Close()
		os.Remove(tmpFile) // Clean up temp file
	}()
	
	// Create PARQUET writer
	pw, err := writer.NewParquetWriter(fw, new(EnhancedParquetEvent), 4)
	if err != nil {
		return fmt.Errorf("failed to create parquet writer: %w", err)
	}
	
	// Configure writer settings
	pw.RowGroupSize = 128 * 1024 * 1024        // 128MB row groups
	pw.PageSize = 8 * 1024                     // 8KB pages
	pw.CompressionType = parquet.CompressionCodec_SNAPPY
	
	// Write events
	for _, event := range parquetEvents {
		if err := pw.Write(event); err != nil {
			logger.Error(ctx, "parquet-writer", "Failed to write event", err)
			continue
		}
	}
	
	// Close writer to flush data
	if err := pw.WriteStop(); err != nil {
		return fmt.Errorf("failed to close parquet writer: %w", err)
	}
	fw.Close()
	
	// Read the file for upload
	fileData, err := os.ReadFile(tmpFile)
	if err != nil {
		return fmt.Errorf("failed to read parquet file: %w", err)
	}
	
	// Generate S3 key with v2 marker for enhanced schema
	var s3Key string
	if !preserveTimestamp.IsZero() {
		// Preserve original timestamp in path with v2 marker
		s3Key = fmt.Sprintf("events/user_%d/%s/%s/v2/events_%s_%d.parquet",
			userID,
			provider,
			preserveTimestamp.Format("2006/01/02/15"),
			preserveTimestamp.Format("20060102150405"),
			time.Now().UnixNano())
	} else {
		// Use current time with v2 marker
		now := time.Now()
		s3Key = fmt.Sprintf("events/user_%d/%s/%s/v2/events_%s_%d.parquet",
			userID,
			provider,
			now.Format("2006/01/02/15"),
			now.Format("20060102150405"),
			now.UnixNano())
	}
	
	// Upload to S3
	_, err = w.s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      &w.bucketName,
		Key:         &s3Key,
		Body:        strings.NewReader(string(fileData)),
		ContentType: stringPtr("application/octet-stream"),
	})
	
	if err != nil {
		return fmt.Errorf("failed to upload parquet to S3: %w", err)
	}
	
	logger.Info(ctx, "parquet-writer", "Successfully wrote ENHANCED PARQUET batch", map[string]interface{}{
		"provider":    provider,
		"user_id":     userID,
		"s3_key":      s3Key,
		"event_count": len(parquetEvents),
		"file_size":   len(fileData),
	})
	
	return nil
}

// convertToEnhancedParquetEvent converts a raw JSON event to EnhancedParquetEvent
func (w *ParquetBatchWriter) convertToEnhancedParquetEvent(rawEvent json.RawMessage, provider string, userID int64, username string) (EnhancedParquetEvent, error) {
	var eventMap map[string]interface{}
	if err := json.Unmarshal(rawEvent, &eventMap); err != nil {
		return EnhancedParquetEvent{}, err
	}
	
	pe := EnhancedParquetEvent{
		Provider: provider,
		UserID:   userID,
		Username: username,
	}
	
	// Track which fields we've processed
	processedFields := make(map[string]bool)
	
	// ============ CORE FIELDS ============
	// Event ID - try multiple field names
	if v := getString(eventMap, "sg_event_id"); v != "" {
		pe.EventID = v
		pe.SGEventID = v
		processedFields["sg_event_id"] = true
	} else if v := getString(eventMap, "event_id", "_id"); v != "" {
		pe.EventID = v
		markProcessed(processedFields, "event_id", "_id")
	}
	
	// Event type
	if v := getString(eventMap, "event"); v != "" {
		pe.EventType = v
		processedFields["event"] = true
	}
	
	// Timestamp
	if v := getFloat(eventMap, "timestamp", "ts"); v > 0 {
		pe.Timestamp = int64(v * 1000) // Convert to milliseconds
		markProcessed(processedFields, "timestamp", "ts")
	} else {
		pe.Timestamp = time.Now().UnixMilli()
	}
	
	// Email
	if v := getString(eventMap, "email"); v != "" {
		pe.Email = v
		pe.Domain = extractDomain(v)
		processedFields["email"] = true
	}
	
	// ============ SENDGRID SPECIFIC ============
	if v := getString(eventMap, "sg_message_id"); v != "" {
		pe.SGMessageID = v
		processedFields["sg_message_id"] = true
	}
	
	if v := getString(eventMap, "sh_username"); v != "" {
		pe.Username = v
		processedFields["sh_username"] = true
	}
	
	// ============ USER/ACCOUNT INFO ============
	if v := getInt(eventMap, "account_id"); v > 0 {
		pe.AccountID = v
		processedFields["account_id"] = true
	}
	
	if v := getInt(eventMap, "contact_id"); v > 0 {
		pe.ContactID = v
		processedFields["contact_id"] = true
	}
	
	// ============ EMAIL METADATA ============
	if v := getString(eventMap, "from"); v != "" {
		pe.FromEmail = v
		processedFields["from"] = true
	}
	
	if v := getString(eventMap, "message_id"); v != "" {
		pe.MessageID = v
		processedFields["message_id"] = true
	}
	
	if v := getString(eventMap, "smtp-id"); v != "" {
		pe.SMTPID = v
		processedFields["smtp-id"] = true
	}
	
	if v := getString(eventMap, "subject"); v != "" {
		pe.Subject = v
		processedFields["subject"] = true
	}
	
	if v := getInt(eventMap, "email_id"); v > 0 {
		pe.EmailID = v
		processedFields["email_id"] = true
	}
	
	// ============ CATEGORIES & TEMPLATES ============
	if cats := eventMap["category"]; cats != nil {
		if catArray, ok := cats.([]interface{}); ok {
			catJSON, _ := json.Marshal(catArray)
			pe.Categories = string(catJSON)
		} else if catStr, ok := cats.(string); ok {
			pe.Categories = catStr
		}
		processedFields["category"] = true
	}
	
	if v := getInt(eventMap, "template_id"); v > 0 {
		pe.TemplateID = v
		processedFields["template_id"] = true
	}
	
	if v := getInt(eventMap, "email_template_id"); v > 0 {
		pe.EmailTemplateID = v
		processedFields["email_template_id"] = true
	}
	
	if v := getString(eventMap, "sg_template_id"); v != "" {
		pe.SGTemplateID = v
		processedFields["sg_template_id"] = true
	}
	
	if v := getString(eventMap, "sg_template_name"); v != "" {
		pe.SGTemplateName = v
		processedFields["sg_template_name"] = true
	}
	
	// ============ AUTOMATION/ACTION PLAN ============
	if v := getInt(eventMap, "automation_id"); v > 0 {
		pe.AutomationID = v
		processedFields["automation_id"] = true
	}
	
	if v := getInt(eventMap, "automation_step_id"); v > 0 {
		pe.AutomationStepID = v
		processedFields["automation_step_id"] = true
	}
	
	if v := getInt(eventMap, "action_plan_id"); v > 0 {
		pe.ActionPlanID = v
		processedFields["action_plan_id"] = true
	}
	
	if v := getInt(eventMap, "action_plan_step_id"); v > 0 {
		pe.ActionPlanStepID = v
		processedFields["action_plan_step_id"] = true
	}
	
	// Handle arrays as JSON strings
	if v := eventMap["action_plan_step_contact_ids"]; v != nil {
		if data, err := json.Marshal(v); err == nil {
			pe.ActionPlanStepContactIDs = string(data)
		}
		processedFields["action_plan_step_contact_ids"] = true
	}
	
	if v := eventMap["automation_contact_log_ids"]; v != nil {
		if data, err := json.Marshal(v); err == nil {
			pe.AutomationContactLogIDs = string(data)
		}
		processedFields["automation_contact_log_ids"] = true
	}
	
	// ============ DELIVERY & ERROR INFO ============
	if v := getString(eventMap, "response"); v != "" {
		pe.Response = v
		processedFields["response"] = true
	}
	
	if v := getString(eventMap, "reason"); v != "" {
		pe.Reason = v
		processedFields["reason"] = true
	}
	
	if v := getString(eventMap, "error_code"); v != "" {
		pe.ErrorCode = v
		processedFields["error_code"] = true
	}
	
	if v := getString(eventMap, "bounce_type", "type"); v != "" {
		pe.BounceType = v
		markProcessed(processedFields, "bounce_type", "type")
	}
	
	if v := getString(eventMap, "status"); v != "" {
		pe.Status = v
		processedFields["status"] = true
	}
	
	if v := getInt32(eventMap, "attempt"); v > 0 {
		pe.Attempt = v
		processedFields["attempt"] = true
	}
	
	if v := getInt32(eventMap, "tls"); v > 0 {
		pe.TLS = v
		processedFields["tls"] = true
	}
	
	if v := getBool(eventMap, "cert_err"); v {
		pe.CertErr = v
		processedFields["cert_err"] = true
	}
	
	// ============ CLICK/OPEN TRACKING ============
	if v := getString(eventMap, "url"); v != "" {
		pe.URL = v
		processedFields["url"] = true
	}
	
	if urlOffset := eventMap["url_offset"]; urlOffset != nil {
		if data, err := json.Marshal(urlOffset); err == nil {
			pe.URLOffset = string(data)
		}
		processedFields["url_offset"] = true
	}
	
	if v := getString(eventMap, "sg_content_type"); v != "" {
		pe.SGContentType = v
		processedFields["sg_content_type"] = true
	}
	
	if v := getBool(eventMap, "sg_machine_open"); v {
		pe.SGMachineOpen = v
		processedFields["sg_machine_open"] = true
	}
	
	// ============ DEVICE/LOCATION ============
	if v := getString(eventMap, "ip"); v != "" {
		pe.IPAddress = v
		processedFields["ip"] = true
	}
	
	if v := getString(eventMap, "useragent"); v != "" {
		pe.UserAgent = v
		processedFields["useragent"] = true
	}
	
	if v := getString(eventMap, "country"); v != "" {
		pe.Country = v
		processedFields["country"] = true
	}
	
	if v := getString(eventMap, "city"); v != "" {
		pe.City = v
		processedFields["city"] = true
	}
	
	if v := getString(eventMap, "domain"); v != "" {
		pe.Domain = v
		processedFields["domain"] = true
	}
	
	// ============ UNSUBSCRIBE/GROUP MANAGEMENT ============
	if v := getInt(eventMap, "asm_group_id"); v > 0 {
		pe.ASMGroupID = v
		processedFields["asm_group_id"] = true
	}
	
	if v := getString(eventMap, "pool"); v != "" {
		pe.Pool = v
		processedFields["pool"] = true
	}
	
	// ============ MARKETING/CAMPAIGN ============
	if v := getString(eventMap, "marketing_campaign_id"); v != "" {
		pe.MarketingCampaignID = v
		processedFields["marketing_campaign_id"] = true
	}
	
	if v := getString(eventMap, "marketing_campaign_name"); v != "" {
		pe.MarketingCampaignName = v
		processedFields["marketing_campaign_name"] = true
	}
	
	if v := getInt(eventMap, "mc_pod_id"); v > 0 {
		pe.MCPodID = v
		processedFields["mc_pod_id"] = true
	}
	
	if v := getString(eventMap, "mc_stats"); v != "" {
		pe.MCStats = v
		processedFields["mc_stats"] = true
	}
	
	if v := getString(eventMap, "phase_id"); v != "" {
		pe.PhaseID = v
		processedFields["phase_id"] = true
	}
	
	// ============ SCHEDULING & MISC ============
	if v := getInt(eventMap, "send_at"); v > 0 {
		pe.SendAt = v
		processedFields["send_at"] = true
	}
	
	if v := getString(eventMap, "newsletter"); v != "" {
		pe.Newsletter = v
		processedFields["newsletter"] = true
	}
	
	if v := getString(eventMap, "unique_id"); v != "" {
		pe.UniqueID = v
		processedFields["unique_id"] = true
	}
	
	if uniqueArgs := eventMap["unique_args"]; uniqueArgs != nil {
		if data, err := json.Marshal(uniqueArgs); err == nil {
			pe.UniqueArgs = string(data)
		}
		processedFields["unique_args"] = true
	}
	
	// ============ COMPUTED FIELDS ============
	t := time.Unix(pe.Timestamp/1000, 0)
	pe.DayOfWeek = int32(t.Weekday())
	pe.HourOfDay = int32(t.Hour())
	
	// ============ CAPTURE UNMAPPED FIELDS ============
	// Skip internal fields
	processedFields["user_id"] = true // Already handled
	processedFields["provider"] = true // Already handled
	
	extraFields := make(map[string]interface{})
	for key, value := range eventMap {
		if !processedFields[key] {
			extraFields[key] = value
		}
	}
	
	if len(extraFields) > 0 {
		if data, err := json.Marshal(extraFields); err == nil {
			pe.ExtraFields = string(data)
		}
	}
	
	return pe, nil
}

// Helper functions
func getString(m map[string]interface{}, keys ...string) string {
	for _, key := range keys {
		if v, ok := m[key].(string); ok && v != "" {
			return v
		}
	}
	return ""
}

func getInt(m map[string]interface{}, keys ...string) int64 {
	for _, key := range keys {
		switch v := m[key].(type) {
		case float64:
			return int64(v)
		case int64:
			return v
		case int:
			return int64(v)
		case string:
			// Handle string "1" as user_id
			if key == "user_id" && v != "" {
				var id int64
				fmt.Sscanf(v, "%d", &id)
				return id
			}
		}
	}
	return 0
}

func getInt32(m map[string]interface{}, keys ...string) int32 {
	for _, key := range keys {
		switch v := m[key].(type) {
		case float64:
			return int32(v)
		case int:
			return int32(v)
		}
	}
	return 0
}

func getFloat(m map[string]interface{}, keys ...string) float64 {
	for _, key := range keys {
		switch v := m[key].(type) {
		case float64:
			return v
		case int:
			return float64(v)
		case int64:
			return float64(v)
		}
	}
	return 0
}

func getBool(m map[string]interface{}, keys ...string) bool {
	for _, key := range keys {
		if v, ok := m[key].(bool); ok {
			return v
		}
	}
	return false
}

func extractDomain(email string) string {
	parts := strings.Split(email, "@")
	if len(parts) == 2 {
		return strings.ToLower(parts[1])
	}
	return ""
}

func markProcessed(processed map[string]bool, keys ...string) {
	for _, key := range keys {
		processed[key] = true
	}
}

func stringPtr(s string) *string {
	return &s
}