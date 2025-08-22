#\!/bin/bash

echo "========================================="
echo "Starting relay-go with MinIO Integration"
echo "========================================="

# Check if MinIO is running
if \! nc -z localhost 9010 2>/dev/null; then
    echo "❌ MinIO is not running on port 9010"
    echo "Please start MinIO first with: ./start-local-minio.sh"
    exit 1
fi

# Check if Docker Redis is running
if \! docker ps | grep -q relay-redis; then
    echo "⚠️ Docker Redis (relay-redis) is not running"
    echo "Starting Docker Redis..."
    docker run -d --name relay-redis -p 6381:6379 redis:6.2-alpine
    sleep 2
fi

# Check if DynamoDB Local is running
if \! nc -z localhost 8000 2>/dev/null; then
    echo "❌ DynamoDB Local is not running on port 8000"
    echo "Please start DynamoDB with: ./docker-local-dynamo.sh up -d"
    exit 1
fi

# Kill existing relay-go process
if pgrep -f "./relay-go" > /dev/null; then
    echo "Stopping existing relay-go process..."
    pkill -f "./relay-go"
    sleep 2
fi

# Source environment variables
echo "Loading environment variables..."
source .env

# Export critical MinIO variables explicitly
export S3_ENDPOINT_URL=http://127.0.0.1:9010
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin
export S3_BUCKET_NAME=production-relay-go-events-998623545110
export DEV_MODE=true
export REDIS_HOST=localhost:6381
export REDIS_PASSWORD=

# Build if necessary
if [ \! -f "./relay-go" ] || [ "main.go" -nt "./relay-go" ]; then
    echo "Building relay-go..."
    go build -o relay-go .
    if [ $? -ne 0 ]; then
        echo "❌ Build failed"
        exit 1
    fi
fi

# Start relay-go
echo "Starting relay-go on port 8080..."
./relay-go > relay-go.log 2>&1 &
RELAY_PID=$\!

# Wait for service to start
sleep 3

# Check if relay-go started successfully
if ps -p $RELAY_PID > /dev/null; then
    echo "✅ relay-go started successfully (PID: $RELAY_PID)"
    echo ""
    echo "Service Status:"
    echo "  relay-go:      http://localhost:8080"
    echo "  MinIO:         http://localhost:9010"
    echo "  MinIO Console: http://localhost:9011"
    echo "  Redis:         localhost:6381"
    echo "  DynamoDB:      http://localhost:8000"
    echo ""
    echo "Logs: tail -f relay-go.log"
    echo ""
    echo "Test webhook endpoint:"
    echo "  http://localhost:8080/webhook-events/sendgrid"
else
    echo "❌ Failed to start relay-go"
    echo "Check relay-go.log for errors"
    exit 1
fi
