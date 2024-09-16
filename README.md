# Email Processing System

This is a Go-based email processing system that handles batch email sending, webhook events from various Email Service Providers (ESPs), and integrates with Kafka for message queuing. The system is designed to be scalable and flexible, supporting multiple ESPs and providing a unified interface for email sending and event processing.

## Features

- Batch email processing
- Real-time email sending
- Webhook handling for multiple ESPs (SendGrid, SparkPost, Postmark, SocketLabs)
- Kafka integration for message queuing
- API key authentication
- Environment variable configuration
- Database integration for user and event association

## Prerequisites

- Go 1.16 or higher
- Redis server
- Kafka cluster
- PostgreSQL database
- API keys for supported ESPs (SendGrid, SparkPost, Postmark, SocketLabs)

## Configuration

The application uses environment variables for configuration. Create a `.env` file in the root directory with the following variables:

```
REDIS_HOST=your_redis_host
REDIS_PASSWORD=your_redis_password
KAFKA_BROKERS=your_kafka_brokers
EMAIL_TOPIC=your_email_topic
WEBHOOK_TOPIC_SENDGRID=your_sendgrid_webhook_topic
WEBHOOK_TOPIC_SPARKPOST=your_sparkpost_webhook_topic
WEBHOOK_TOPIC_POSTMARK=your_postmark_webhook_topic
WEBHOOK_TOPIC_SOCKETLABS=your_socketlabs_webhook_topic
HTTP_SERVER_PORT=your_http_server_port
```

## Installation

1. Clone the repository
2. Install dependencies: `go mod tidy`
3. Build the application: `go build`
4. Run the application: `./email-processing-system`

## Usage

### Sending Emails

Send a POST request to `/emails` with the following JSON payload:

```json
{
  "from": {"email": "sender@example.com"},
  "personalizations": [
    {"to": {"email": "recipient@example.com"}}
  ],
  "content": [
    {"type": "text/plain", "value": "Hello, World!"},
    {"type": "text/html", "value": "<h1>Hello, World!</h1>"}
  ],
  "custom_args": {
    "IsBatch": "true"  // Set to "true" for batch processing
  }
}
```

Include the API key in the Authorization header:

```
Authorization: Bearer your_api_key
```

### Webhook Endpoints

The system provides webhook endpoints for various ESPs:

- SendGrid: `/webhook-events/sendgrid`
- SparkPost: `/webhook-events/sparkpost`
- Postmark: `/webhook-events/postmark`
- SocketLabs: `/webhook-events/socketlabs`

Configure your ESP accounts to send webhook events to these endpoints.

## Architecture

The system consists of the following main components:

1. HTTP server for handling email requests and webhook events
2. BatchProcessor for managing batch email sending
3. Kafka integration for message queuing
4. Database integration for user authentication and event association
5. Redis for storing scheduled emails

## Error Handling

The system includes error handling for various scenarios, including:

- Invalid API keys
- Malformed request bodies
- Failed database operations
- Kafka producer errors

Errors are logged and appropriate HTTP status codes are returned to the client.

## Security

- API key authentication is required for sending emails
- Webhook verification is implemented for supported ESPs
- HTTPS is recommended for production deployments (not included in this code)

## Extending the System

To add support for a new ESP:

1. Create a new webhook handler function
2. Implement the necessary verification logic
3. Add a new webhook topic in the configuration
4. Update the main function to include the new webhook endpoint

## Contributing

Contributions are welcome! Please submit a pull request or create an issue for any features or bug fixes.

## License


This README provides an overview of the email processing system, including its features, setup instructions, usage guidelines, and architectural details. You may want to customize it further based on any additional functionality or specific requirements of your project.