# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

relay-go is an email processing system that serves as the ingress point for handling email sending requests and webhook events from various Email Service Providers (ESPs). It uses Kafka for message queuing and works with a separate consumer component (go-relay-consumer) for actual email delivery.

**Key Features:**
- Dual-mode operation (MySQL+Kafka or DynamoDB+S3)
- Multi-ESP support with unified interface
- Batch email processing with Redis
- Webhook signature verification
- Flexible deployment (ECS/containerized or serverless)

## Common Development Commands

### Build and Run
```bash
# Install dependencies
go mod tidy

# Build the application
go build

# Run the application locally (requires .env file)
./relay-go

# Run tests
go test ./...

# Run tests with coverage
go test -v -cover ./...

# Run a single test
go test -v -run TestFunctionName ./path/to/package
```

### Code Quality
```bash
# Format code
go fmt ./...

# Run static analysis
go vet ./...

# Install and run golangci-lint (recommended for comprehensive linting)
curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(go env GOPATH)/bin v1.61.0
golangci-lint run
```

### Docker Development
```bash
# Start development environment with all services
docker-compose up

# Start specific environment
docker-compose -f docker-compose-dev.yaml up
docker-compose -f docker-compose-test.yaml up

# Build and push to AWS ECR
./build_and_push.sh

# Manual AWS ECR push (if needed)
aws ecr get-login-password --region us-east-2 | docker login --username AWS --password-stdin 257394459269.dkr.ecr.us-east-2.amazonaws.com
docker tag relay-go:latest 257394459269.dkr.ecr.us-east-2.amazonaws.com/sh-consulting/relay-go:latest
docker push 257394459269.dkr.ecr.us-east-2.amazonaws.com/sh-consulting/relay-go:latest
```

**Development Services:**
- Kafka UI: `http://localhost:8080`
- Redis Insights: `http://localhost:5540`
- Nginx (reverse proxy)
- Ngrok (for webhook testing with external ESPs)

### Database Setup
```bash
# Create DynamoDB table
./scripts/create_dynamodb_table.sh
```

## Architecture

### Dual Mode Operation
1. **MySQL + Kafka mode**: Full processing with database and message queue
2. **Light mode**: Direct processing with S3 and optional Splunk (no database dependency)

### Key Components
- **Main Application** (`main.go`): HTTP server with endpoints for email sending and webhook handling
- **Email Processing** (`email.go`, `email_batch_handler.go`): Handles email validation and routing
- **ESP Webhook Verification** (`*_verification.go` files): Signature verification for each ESP
- **Database Layer** (`/database`): MySQL/PostgreSQL, DynamoDB, Redis, and Kafka integrations
- **Webhook Processing** (`/webhook`): Event processing with Kafka, Splunk, and S3 support
- **Logging** (`/logger`): Structured logging with request ID and user ID tracking

### Supported ESPs
- SendGrid
- SparkPost
- Postmark
- SocketLabs
- Mandrill
- Resend

### API Endpoints
- `POST /emails`: Send emails (requires API key authentication)
- `POST /webhook-events/sendgrid`: SendGrid webhook events
- `POST /webhook-events/sparkpost`: SparkPost webhook events
- `POST /webhook-events/postmark`: Postmark webhook events
- `POST /webhook-events/socketlabs`: SocketLabs webhook events
- `POST /webhook-events/mandrill`: Mandrill webhook events
- `POST /webhook-events/resend`: Resend webhook events
- `GET /health`: Health check endpoint (previously `/healthcheck`)

## Environment Configuration

The application requires environment variables defined in `.env` file:
- `REDIS_HOST`, `REDIS_PASSWORD`: Redis connection
- `KAFKA_BROKERS`, `EMAIL_TOPIC`, `WEBHOOK_TOPIC_*`: Kafka configuration
- `MYSQL_*` or `POSTGRES_*`: Database connection (for non-light mode)
- `DYNAMODB_*`: DynamoDB configuration
- `SENDGRID_WEBHOOK_VERIFICATION_KEY`, etc.: ESP webhook verification keys (EC public keys for SendGrid, webhook secrets for others)
- `SPLUNK_*`: Splunk integration (optional)
- `S3_*`: S3 bucket configuration (for light mode)
- `HTTP_SERVER_PORT`: Server port (default: 8080)
- `MODE`: Operation mode ("mysql+kafka" or "light")

## Testing Approach

Tests use `stretchr/testify` for assertions. Main test coverage includes:
- Environment variable configuration validation
- HTTP endpoint testing
- Health check functionality
- Splunk configuration testing
- ESP webhook signature verification
- Email payload validation

Run tests before committing changes to ensure nothing is broken.

### Testing Commands
```bash
# Run all tests
go test ./...

# Run tests with verbose output
go test -v ./...

# Run tests for a specific package
go test -v ./webhook

# Run a specific test function
go test -v -run TestSendEmail ./

# Run tests with race detection
go test -race ./...
```

## Email Payload Formats

The system accepts two template formats:
1. **HandleBars format**: Uses `{{variable}}` placeholders
2. **Hyphens format**: Uses `-variable-` placeholders

Both formats support personalizations, substitutions, sections, attachments, headers, and custom arguments.

## Development Workflow

1. Make changes to the code
2. Run tests: `go test ./...`
3. Test locally with Docker Compose
4. For webhook testing, use ngrok (included in docker-compose)
5. Build and deploy using `./build_and_push.sh` when ready

## Important Notes

- API key authentication is required for `/emails` endpoint
- Webhook endpoints require signature verification
- The system can operate in light mode without MySQL/PostgreSQL dependency
- Kafka topics must be created before running the application
- Redis is used for batch email processing and caching
- DynamoDB users table uses atomic counters for ID generation
- Event batching improves S3 storage efficiency in light mode
- The application automatically detects operation mode based on available resources