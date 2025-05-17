package main

import "time"

type Attachment struct {
	Content     string `json:"content"`
	ContentID   string `json:"content_id"`
	Disposition string `json:"disposition"`
	Filename    string `json:"filename"`
	Name        string `json:"name"`
	Type        string `json:"type"`
	ContentType string `json:"ContentType,omitempty"`
}

type EmailMessage struct {
	From             EmailAddress
	To               []EmailAddress
	Cc               []string
	Bcc              []string
	Subject          string
	TextBody         string
	HtmlBody         string
	Content          []Content
	Attachments      []Attachment
	Headers          map[string]string
	CustomArgs       map[string]interface{} `json:"custom_args"`
	Credentials      Credentials
	Personalizations []Personalization
	Sections         map[string]string
	Categories       []string
}

type Content struct {
	Type  string `json:"type"`
	Value string `json:"value"`
}

type Personalization struct {
	To            EmailAddress
	Subject       string
	Substitutions map[string]string
}

type EmailAddress struct {
	Name  string
	Email string
}

type Credentials struct {
	SocketLabsServerID  string `json:"SocketLabsServerID"`
	SocketLabsAPIKey    string `json:"SocketLabsAPIkey"`
	SocketLabsWeight    string `json:"SocketLabsWeight"`
	PostmarkServerToken string `json:"PostmarkServerToken"`
	PostmarkWeight      string `json:"PostmarkWeight"`
	SendgridAPIKey      string `json:"SendgridAPIKey"`
	SendgridWeight      string `json:"SendgridWeight"`
	SparkpostAPIKey     string `json:"SparkpostAPIKey"`
	SparkpostWeight     string `json:"SparkpostWeight"`
}

type EmailPayload struct {
	Personalizations []Personalization `json:"personalizations"`
	From             EmailAddress      `json:"from"`
	ReplyTo          *EmailAddress     `json:"reply_to,omitempty"`
	Subject          string            `json:"subject"`
	Content          []Content         `json:"content"`
	Attachments      []Attachment      `json:"attachments"`
	Headers          map[string]string `json:"headers"`
	Categories       []string          `json:"categories"`
	CustomArgs       map[string]string `json:"custom_args"`
	Sections         map[string]string `json:"sections"`
}

type BatchInfo struct {
	ID              int
	UserID          int
	TotalEmails     int
	CreatedAt       time.Time
	UpdatedAt       time.Time
	Status          string
	InitialWeights  map[string]int
	BatchSize       int
	IntervalSeconds int
}

type kafkaPayload struct {
	BatchID   int
	MessageID string
	UserID    int
	Body      EmailPayload
}
