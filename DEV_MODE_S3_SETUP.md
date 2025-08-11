# Running relay-go in Development Mode with S3 Uploads

This guide explains how to run relay-go in "light mode" which uploads webhook events to S3 instead of using Kafka.

## Quick Start (if DynamoDB is already running)

```bash
# 1. Start Redis (port 6381)
docker-compose -f docker-compose-dev.yaml up -d redis

# 2. Check if users table exists
aws dynamodb list-tables --endpoint-url http://localhost:8000

# 3. Set environment variables in .env
REDIS_HOST=localhost:6381
REDIS_PASSWORD=yourpassword
S3_BUCKET_NAME=your-webhook-events-bucket
DYNAMODB_ENDPOINT=http://localhost:8000
DEV_MODE=true
HTTP_SERVER_PORT=8888

# 4. Build and run
go build
./relay-go
```

## Prerequisites

1. AWS credentials configured (or use dummy credentials for local testing)
2. Docker and Docker Compose installed
3. An S3 bucket created (or use DEV_MODE=true to skip actual uploads)

## Step 1: Create/Update .env file

Create a `.env` file in the project root with the following configuration:

```bash
# Redis Configuration (using port 6381 to avoid conflicts)
REDIS_HOST=localhost:6381
REDIS_PASSWORD=yourpassword

# AWS Configuration
AWS_REGION=us-east-1
AWS_ACCESS_KEY_ID=your-access-key-or-dummy
AWS_SECRET_ACCESS_KEY=your-secret-key-or-dummy

# S3 Configuration
S3_BUCKET_NAME=your-webhook-events-bucket

# DynamoDB Configuration
DYNAMODB_ENDPOINT=http://localhost:8000

# Development Mode (set to true to log S3 operations instead of uploading)
DEV_MODE=true

# HTTP Server
HTTP_SERVER_PORT=8888

# Optional: Webhook verification keys for testing
# Add these as you create test users in DynamoDB
```

## Step 2: Start the services

```bash
# Start Redis and DynamoDB using the dev compose file
docker-compose -f docker-compose-dev.yaml up -d redis dynamodb-local

# Optional: Also start Redis Insights to monitor Redis
docker-compose -f docker-compose-dev.yaml up -d redis-insights
```

## Step 3: Verify/Create the DynamoDB users table

```bash
# Check if the table already exists
aws dynamodb describe-table --table-name users --endpoint-url http://localhost:8000 2>/dev/null

# If the table doesn't exist, create it using the provided script:
./scripts/create_dynamodb_table.sh

# Or list all tables to see what's available
aws dynamodb list-tables --endpoint-url http://localhost:8000
```

## Step 4: Create test users with webhook secrets

```bash
# Note: The table uses string IDs, not numeric IDs

# Create a test user for Resend
aws dynamodb put-item \
  --table-name users \
  --item '{
    "id": {"N": "10"},
    "email": {"S": "test@example.com"},
    "resend_webhook_secret": {"S": "whsec_bwuUrabtMbY17NVe2pHawyN+2BNp9Tch"},
    "created_at": {"N": "1716336000"},
    "updated_at": {"N": "1716336000"}
  }' \
  --endpoint-url http://localhost:8000

# Create a test user for SendGrid
aws dynamodb put-item \
  --table-name users \
  --item '{
    "id": {"S": "101"},
    "email": {"S": "sendgrid@example.com"},
    "sendgrid_verification_key": {"S": "MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAE/uhoEqKTdq2vESSPbe08UuH3RVVUwyJOXckuFxllsK0CoUk4x1XGzSmvBjWDtZZ18bYB+Pud/7DydfS+wc/GSA=="},
    "created_at": {"N": "1716336000"},
    "updated_at": {"N": "1716336000"}
  }' \
  --endpoint-url http://localhost:8000
```

## Step 5: Build and run the application

```bash
# Build the application
go build

# Run the application (it will automatically detect light mode)
./relay-go
```

## Expected Output

When starting in light mode, you should see logs like:

```
INFO[0000] MySQL connection failed, trying DynamoDB mode
INFO[0000] DynamoDB client created successfully
INFO[0000] Running in DynamoDB mode (light mode)
INFO[0000] Event batcher processor created with S3 bucket: your-webhook-events-bucket
INFO[0000] Listening on port 8888...
```

## How It Works

1. **Mode Detection**: The app tries MySQL first. When it fails, it switches to DynamoDB (light mode)
2. **Event Processing**: Webhook events are:
   - Received and verified against user webhook secrets
   - Temporarily stored in Redis
   - Batched based on size/time thresholds
   - Uploaded to S3 in JSON format

3. **Batching Configuration** (Development):
   - Max 10 events per batch
   - Max 1MB per batch
   - Flushes every 30 seconds

4. **S3 Path Structure**:
   ```
   events/user_{userID}/{provider}/{year}/{month}/{day}/{hour}/events_{timestamp}_{batchID}.json
   ```

## Testing Webhooks

Send a test webhook to verify the setup:

```bash
# Test Resend webhook (adjust the signature headers as needed)
curl -X POST http://localhost:8888/webhook-events/resend \
  -H "Content-Type: application/json" \
  -H "svix-id: msg_123" \
  -H "svix-timestamp: $(date +%s)" \
  -H "svix-signature: v1,test_signature" \
  -d '{
    "type": "email.sent",
    "created_at": "2023-01-01T00:00:00Z",
    "data": {
      "email_id": "test123",
      "from": "sender@example.com",
      "to": ["recipient@example.com"],
      "subject": "Test Email"
    }
  }'
```

## Monitoring

- **Redis Insights**: http://localhost:5540 (to see batched events)
- **Application Logs**: Watch for "S3 upload" messages (in DEV_MODE, these will be logged instead of actual uploads)

## Troubleshooting

1. **Port conflicts**: If Redis port 6381 is in use, update it in both docker-compose-dev.yaml and .env
2. **DynamoDB connection issues**: Ensure DynamoDB is running on port 8000
3. **No S3 uploads**: Check that DEV_MODE is set appropriately and Redis is accessible
4. **Webhook verification fails**: Ensure user exists in DynamoDB with correct webhook secret

## Production Mode

To run with actual S3 uploads:
1. Set `DEV_MODE=false` or remove it from .env
2. Ensure valid AWS credentials are configured
3. Verify S3 bucket exists and has proper permissions