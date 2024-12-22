package main

import (
	"database/sql"
	"fmt"
	"log"
	"net/http"
)

func verifySparkPostWebhookAndFindUser(db *sql.DB, headers http.Header) (int, int, error) {
	authHeader := headers.Get("Authorization")
	log.Printf("Auth Header: %s", authHeader) // Add this line

	username, password, err := decodeBasicAuth(authHeader)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to decode auth header: %v", err)
	}

	var userID, espID int
	err = db.QueryRow(`
        SELECT user_id, esp_id 
        FROM email_service_providers 
        WHERE provider_name = 'sparkpost' 
        AND sparkpost_webhook_user = $1 
        AND sparkpost_webhook_password = $2
    `, username, password).Scan(&userID, &espID)

	if err != nil {
		if err == sql.ErrNoRows {
			return 0, 0, fmt.Errorf("no matching user found for the given credentials")
		}
		return 0, 0, fmt.Errorf("database query failed: %v", err)
	}

	return userID, espID, nil
}

func associateSparkPostEventWithUser(db *sql.DB, messageID string, userID, espID int) error {
	_, err := db.Exec(`
	INSERT INTO message_user_associations (message_id, user_id, esp_id, provider)
	SELECT ?, ?, esp_id, 'sparkpost'
	FROM email_service_providers
	WHERE user_id = ? AND provider_name = 'sparkpost'
	ON DUPLICATE KEY UPDATE id = id
`, messageID, userID, userID)

	if err != nil {
		return fmt.Errorf("failed to insert message association: %v", err)
	}
	return nil
}

type GeoIP struct {
	Country    string  `json:"country"`
	Region     string  `json:"region"`
	City       string  `json:"city"`
	Latitude   float64 `json:"latitude"`
	Longitude  float64 `json:"longitude"`
	Zip        int     `json:"zip"`
	PostalCode string  `json:"postal_code"`
}

type UserAgentParsed struct {
	AgentFamily  string `json:"agent_family"`
	DeviceBrand  string `json:"device_brand"`
	DeviceFamily string `json:"device_family"`
	OSFamily     string `json:"os_family"`
	OSVersion    string `json:"os_version"`
	IsMobile     bool   `json:"is_mobile"`
	IsProxy      bool   `json:"is_proxy"`
	IsPrefetched bool   `json:"is_prefetched"`
}

type CommonEventFields struct {
	ABTestID              string `json:"ab_test_id"`
	ABTestVersion         string `json:"ab_test_version"`
	AmpEnabled            bool   `json:"amp_enabled"`
	CampaignID            string `json:"campaign_id"`
	ClickTracking         bool   `json:"click_tracking"`
	CustomerID            string `json:"customer_id"`
	DelvMethod            string `json:"delv_method"`
	EventID               string `json:"event_id"`
	FriendlyFrom          string `json:"friendly_from"`
	InitialPixel          bool   `json:"initial_pixel"`
	InjectionTime         string `json:"injection_time"`
	IPAddress             string `json:"ip_address"`
	IPPool                string `json:"ip_pool"`
	MailboxProvider       string `json:"mailbox_provider"`
	MailboxProviderRegion string `json:"mailbox_provider_region"`
	MessageID             string `json:"message_id"`
	MsgFrom               string `json:"msg_from"`
	MsgSize               string `json:"msg_size"`
	NumRetries            string `json:"num_retries"`
	OpenTracking          bool   `json:"open_tracking"`
	QueueTime             string `json:"queue_time"`
	RcptMeta              struct {
		CustomKey string `json:"customKey"`
	} `json:"rcpt_meta"`
	RcptTags        []string `json:"rcpt_tags"`
	RcptTo          string   `json:"rcpt_to"`
	RcptHash        string   `json:"rcpt_hash"`
	RawRcptTo       string   `json:"raw_rcpt_to"`
	RcptType        string   `json:"rcpt_type"`
	RecipientDomain string   `json:"recipient_domain"`
	RoutingDomain   string   `json:"routing_domain"`
	ScheduledTime   string   `json:"scheduled_time"`
	SendingIP       string   `json:"sending_ip"`
	SubaccountID    string   `json:"subaccount_id"`
	Subject         string   `json:"subject"`
	TemplateID      string   `json:"template_id"`
	TemplateVersion string   `json:"template_version"`
	Timestamp       string   `json:"timestamp"`
	Transactional   string   `json:"transactional"`
	TransmissionID  string   `json:"transmission_id"`
	Type            string   `json:"type"`
}

type MessageEvent struct {
	CommonEventFields
	BounceClass  string   `json:"bounce_class,omitempty"`
	ErrorCode    string   `json:"error_code,omitempty"`
	RawReason    string   `json:"raw_reason,omitempty"`
	Reason       string   `json:"reason,omitempty"`
	SMSCoding    string   `json:"sms_coding"`
	SMSDst       string   `json:"sms_dst"`
	SMSDstNpi    string   `json:"sms_dst_npi"`
	SMSDstTon    string   `json:"sms_dst_ton"`
	SMSRemoteids []string `json:"sms_remoteids,omitempty"`
	SMSSegments  int      `json:"sms_segments,omitempty"`
	SMSSrc       string   `json:"sms_src"`
	SMSSrcNpi    string   `json:"sms_src_npi"`
	SMSSrcTon    string   `json:"sms_src_ton"`
	OutboundTLS  string   `json:"outbound_tls"`
	RecvMethod   string   `json:"recv_method"`
}

type TrackEvent struct {
	CommonEventFields
	GeoIP           GeoIP           `json:"geo_ip"`
	TargetLinkName  string          `json:"target_link_name"`
	TargetLinkURL   string          `json:"target_link_url"`
	UserAgent       string          `json:"user_agent"`
	UserAgentParsed UserAgentParsed `json:"user_agent_parsed"`
}

type SparkPostWebhookHeaders struct {
	AcceptEncoding      []string `json:"Accept-Encoding"`
	Authorization       []string `josn:"Authorization"`
	ContentLength       []string `json:"Content-Length"`
	ContentType         []string `json:"Content-Type"`
	UserAgent           []string `json:"User-Agent"`
	XForwardedFor       []string `json:"X-Forwarded-For"`
	XForwardedHost      []string `json:"X-Forwarded-Host"`
	XForwardedProto     []string `json:"X-Forwarded-Proto"`
	XSparkpostSignature []string `json:"X-Sparkpost-Signature"`
}

type SparkPostPayload []struct {
	Msys struct {
		MessageEvent *MessageEvent `json:"message_event,omitempty"`
		TrackEvent   *TrackEvent   `json:"track_event,omitempty"`
	} `json:"msys"`
}
