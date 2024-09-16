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

## Usage

### Sending Emails

The system accepts two types of payload formats for email sending: HandleBars and Hyphens. Send a POST request to `/emails` with the appropriate JSON payload.

Include the API key in the Authorization header:

```
Authorization: Bearer your_api_key
```

#### HandleBars Format

Use this format when your email content uses HandleBars-style placeholders (e.g., `{{name}}`).

Example payload:

```json
{
  "from": {
    "name": "Batch Testing",
    "email": "batch@example.com"
  },
  "personalizations": [
    {
      "to": {
        "name": "Nick Martinez, Jr.",
        "email": "nick@example.com"
      },
      "subject": "Personalized Email for Nick",
      "substitutions": {
        "name": "Nick",
        "email": "nick@example.com",
        "order_id": "12345",
        "confirmations": "confirmation_001",
        "customer_since": "2020-01-01",
        "loyalty_level": "Gold",
        "product_recommendation": "premium_plan"
      }
    }
  ],
  "subject": "Default Subject (if no personalization)",
  "content": [
    {
      "type": "text/plain",
      "value": "Hello {{name}},\n {{confirmations}}\n Customer since: {{customer_since}}\n Loyalty Level: {{loyalty_level}}\n {{product_recommendation}}"
    },
    {
      "type": "text/html",
      "value": "<html><body><h1>Welcome, {{name}}!</h1><p>{{confirmations}}</p></body></html>"
    }
  ],
  "sections": {
    "confirmation_001": "Thanks for choosing our service. This email is to confirm that we have processed your order {{order_id}}."
  },
  "attachments": [
    {
      "filename": "example.txt",
      "type": "text/plain",
      "content": "SGVsbG8gd29ybGQh",
      "content_id": "ii_139db99fdb5c3704",
      "disposition": "attachment"
    }
  ],
  "headers": {
    "X-Custom-Header-1": "Custom Value 1"
  },
  "custom_args": {
    "TrackOpens": "true",
    "TrackLinks": "HtmlOnly",
    "MessageStream": "outbound",
    "IsBatch": "true",
    "BatchSize": "1",
    "BatchInterval": "60"
  },
  "categories": ["test", "example"]
}
```

#### Hyphens Format

Use this format when your email content uses hyphen-style placeholders (e.g., `-name-`).

Example payload:

```json
{
  "from": {
    "name": "Email Service",
    "email": "admin@example.com"
  },
  "personalizations": [
    {
      "to": {
        "name": "Jane Doe",
        "email": "jane@example.com"
      },
      "subject": "Personalized Email for Jane",
      "substitutions": {
        "name": "Jane",
        "email": "jane@example.com",
        "order_id": "67890",
        "confirmations": "confirmation_002",
        "customer_since": "2021-06-15",
        "loyalty_level": "Silver",
        "product_recommendation": "standard_plan"
      }
    }
  ],
  "subject": "Default Subject (if no personalization)",
  "content": [
    {
      "type": "text/plain",
      "value": "Hello -name-,\n -confirmations-\n Customer since: -customer_since-\n Loyalty Level: -loyalty_level-\n -product_recommendation-"
    },
    {
      "type": "text/html",
      "value": "<html><body><h1>Welcome, -name-!</h1><p>-confirmations-</p></body></html>"
    }
  ],
  "sections": {
    "confirmation_002": "Thanks for your order. We've processed your order -order_id-. You can download your invoice as a PDF for your records."
  },
  "attachments": [
    {
      "filename": "example.txt",
      "type": "text/plain",
      "content": "SGVsbG8gd29ybGQh",
      "content_id": "ii_139db99fdb5c3704",
      "disposition": "attachment"
    }
  ],
  "headers": {
    "X-Custom-Header-1": "Custom Value 1"
  },
  "custom_args": {
    "TrackOpens": "true",
    "TrackLinks": "HtmlOnly",
    "MessageStream": "outbound",
    "IsBatch": "true",
    "BatchSize": "1",
    "BatchInterval": "60"
  },
  "categories": ["test", "example"]
}
```

### Batch Processing

To enable batch processing, set the `IsBatch` custom argument to "true" in your payload. You can also specify the `BatchSize` and `BatchInterval` (in seconds) in the `custom_args` section.

### Webhook Endpoints

The system provides webhook endpoints for various ESPs:

- SendGrid: `/webhook-events/sendgrid`
- SparkPost: `/webhook-events/sparkpost`
- Postmark: `/webhook-events/postmark`
- SocketLabs: `/webhook-events/socketlabs`

Configure your ESP accounts to send webhook events to these endpoints.

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

## Development Environment

Our development environment is containerized using Docker Compose, which allows for easy setup and consistent development across different machines. The environment consists of several services:

1. Kafka: Message broker for handling email and webhook event queues.
2. Kafka UI: Web interface for monitoring and managing Kafka.
3. Relay-Go: Our main application for processing emails and webhook events.
4. Relay-ESP: Service for interacting with Email Service Providers.
5. PostgreSQL: Database for storing application data.
6. Redis: In-memory data structure store used for caching and message queuing.
7. Redis Insights: Web-based GUI for monitoring and managing Redis.
8. Nginx: Web server used as a reverse proxy.
9. Ngrok: Secure tunneling service for exposing local servers to the internet.

### Proxy Setup with Nginx and Ngrok

We use Nginx and Ngrok in our development environment for the following reasons:

1. Nginx as a Reverse Proxy:
   - Load Balancing: Nginx can distribute incoming requests across multiple application instances.
   - SSL Termination: Handles HTTPS connections, offloading this task from our application servers.
   - Caching: Can cache responses, reducing load on our application servers.
   - Security: Acts as an additional layer of security, hiding our internal network structure.

2. Ngrok for External Access:
   - Secure Tunneling: Allows us to expose our local development environment to the internet securely.
   - Testing Webhooks: Enables testing of webhook integrations without deploying to a public server.
   - Collaboration: Facilitates sharing of local development instances with team members or clients.
   - Bypassing Firewalls: Useful when developing behind restrictive firewalls or NATs.

This setup allows us to develop and test our application in an environment that closely mimics production, including the ability to receive webhooks from external services. It also provides flexibility in routing requests and securing our application endpoints.

To use this development environment:

1. Ensure Docker and Docker Compose are installed on your system.
2. Set up the required environment variables in a `.env` file.
3. Run `docker-compose up` to start all services.
4. Access the Kafka UI at `http://localhost:8080` for monitoring Kafka.
5. Access Redis Insights at `http://localhost:5540` for monitoring Redis.
6. The main application will be available through Ngrok, with the URL displayed in the Ngrok logs.

Note: Make sure to never expose sensitive information or production data through Ngrok in a public development environment.


## Contributing

Contributions are welcome! Please submit a pull request or create an issue for any features or bug fixes.

## License
