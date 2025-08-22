#!/bin/bash

# Comprehensive start script for testing relay-go with MinIO and parallel PARQUET processor
# This script ensures proper integration between relay-go, MinIO, Redis, and DynamoDB

set -e  # Exit on error

echo "============================================="
echo "RELAY-GO PARQUET INTEGRATION TEST STARTUP"
echo "============================================="
echo ""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to check if a service is running
check_service() {
    local name=$1
    local check_cmd=$2
    local start_msg=$3
    
    echo -n "Checking $name... "
    if eval "$check_cmd" > /dev/null 2>&1; then
        echo -e "${GREEN}✓ Running${NC}"
        return 0
    else
        echo -e "${RED}✗ Not running${NC}"
        echo -e "${YELLOW}$start_msg${NC}"
        return 1
    fi
}

# Check prerequisites
echo "Checking prerequisites..."
echo ""

# Check Redis
if ! check_service "Redis" "redis-cli -h localhost -p 6379 -a app_password ping 2>/dev/null" \
    "Please start Redis: cd ../relay-go-dataviz && ./start-redis-local.sh"; then
    exit 1
fi

# Check MinIO
if ! check_service "MinIO" "curl -s http://localhost:9010" \
    "Please start MinIO: cd ../relay-go-dataviz && ./start-local-minio.sh"; then
    exit 1
fi

# Check DynamoDB Local
if ! check_service "DynamoDB Local" "curl -s http://localhost:8000" \
    "Please start DynamoDB: cd ../relay-go-dataviz && ./docker-local-dynamo.sh up"; then
    exit 1
fi

echo ""
echo "All prerequisites satisfied!"
echo ""

# Navigate to relay-go directory
cd /Users/nickmartinez/personal/relay-go

# Kill any existing relay-go process
echo "Stopping any existing relay-go process..."
pkill -f "^\./relay-go$" 2>/dev/null || pkill -f "^relay-go$" 2>/dev/null || true
sleep 2

# Clear old logs for clean testing
echo "Clearing old logs..."
> relay-go.log

# Build the application
echo "Building relay-go..."
go build -o relay-go .

if [ $? -ne 0 ]; then
    echo -e "${RED}Build failed!${NC}"
    exit 1
fi
echo -e "${GREEN}✓ Build successful${NC}"
echo ""

# Create environment file for relay-go
echo "Configuring environment..."
cat > .relay-go-test.env << 'EOF'
# MinIO Configuration (CRITICAL - must override AWS defaults)
AWS_ENDPOINT_URL=http://localhost:9010
S3_ENDPOINT_URL=http://localhost:9010
S3_BUCKET_NAME=production-relay-go-events-998623545110
AWS_ACCESS_KEY_ID=minioadmin
AWS_SECRET_ACCESS_KEY=minioadmin
AWS_REGION=us-east-1
S3_USE_PATH_STYLE=true

# PARQUET Configuration
ENABLE_PARQUET_OUTPUT=true
ENABLE_PARALLEL_PARQUET=true

# Redis Configuration
REDIS_HOST=localhost:6379
REDIS_PASSWORD=app_password

# DynamoDB Local
DYNAMODB_ENDPOINT=http://localhost:8000

# Development Mode (faster batching for testing)
DEV_MODE=true
DEV_MODE_BATCH_INTERVAL=30s
DEV_MODE_PARQUET_BATCH_INTERVAL=120s

# HTTP Server
HTTP_SERVER_PORT=8080

# Disable unused services
KAFKA_BROKERS=localhost:9092
EMAIL_TOPIC=emails
SPLUNK_ENABLED=false

# Logging
LOG_LEVEL=INFO
EOF

# Export all environment variables
export $(grep -v '^#' .relay-go-test.env | xargs)

# Ensure MinIO bucket exists
echo "Ensuring MinIO bucket exists..."
mc alias set local http://localhost:9010 minioadmin minioadmin 2>/dev/null || true
mc mb local/production-relay-go-events-998623545110 2>/dev/null || true
echo -e "${GREEN}✓ MinIO bucket ready${NC}"
echo ""

# Start relay-go with explicit environment
echo "Starting relay-go with parallel PARQUET processor..."
echo "Configuration:"
echo "  - JSON batching: Every 30 seconds (Dev mode)"
echo "  - PARQUET batching: Every 2 minutes or 5000 events (Dev mode)"
echo "  - Parallel processing: User X → User 9999000X"
echo ""

# Start with environment variables explicitly set
AWS_ENDPOINT_URL="http://localhost:9010" \
S3_ENDPOINT_URL="http://localhost:9010" \
S3_BUCKET_NAME="production-relay-go-events-998623545110" \
AWS_ACCESS_KEY_ID="minioadmin" \
AWS_SECRET_ACCESS_KEY="minioadmin" \
AWS_REGION="us-east-1" \
S3_USE_PATH_STYLE="true" \
ENABLE_PARQUET_OUTPUT="true" \
ENABLE_PARALLEL_PARQUET="true" \
REDIS_HOST="localhost:6379" \
REDIS_PASSWORD="app_password" \
DYNAMODB_ENDPOINT="http://localhost:8000" \
DEV_MODE="true" \
HTTP_SERVER_PORT="8080" \
KAFKA_BROKERS="localhost:9092" \
EMAIL_TOPIC="emails" \
SPLUNK_ENABLED="false" \
./relay-go > relay-go.log 2>&1 &

RELAY_PID=$!
echo "Started relay-go with PID: $RELAY_PID"

# Wait for startup
echo -n "Waiting for relay-go to start"
for i in {1..10}; do
    sleep 1
    echo -n "."
    if curl -s http://localhost:8080/healthcheck > /dev/null 2>&1; then
        echo -e " ${GREEN}✓${NC}"
        echo -e "${GREEN}relay-go is running!${NC}"
        break
    fi
    if [ $i -eq 10 ]; then
        echo -e " ${RED}✗${NC}"
        echo -e "${RED}Failed to start relay-go. Check relay-go.log for errors.${NC}"
        tail -20 relay-go.log
        exit 1
    fi
done

echo ""
echo "============================================="
echo "RELAY-GO STARTED SUCCESSFULLY"
echo "============================================="
echo ""
echo "Service endpoints:"
echo "  - relay-go webhook: http://localhost:8080/webhook-events/{provider}"
echo "  - MinIO console: http://localhost:9011 (minioadmin/minioadmin)"
echo "  - MinIO S3: http://localhost:9010"
echo ""
echo "Monitoring:"
echo "  - Logs: tail -f relay-go.log"
echo "  - Redis events: redis-cli -h localhost -p 6379 -a app_password keys 'provider:*:user:*:events'"
echo "  - MinIO files: mc ls local/production-relay-go-events-998623545110/events/ --recursive"
echo ""
echo "To test the parallel PARQUET processor:"
echo "  ./test-parallel-parquet.sh"
echo ""
echo "To send test events:"
echo '  curl -X POST "http://localhost:8080/webhook-events/sendgrid" \'
echo '    -H "Content-Type: application/json" \'
echo '    -H "X-Test-Mode: true" \'
echo '    -H "X-Test-User-ID: 7" \'
echo '    -d '"'"'[{"email":"test@example.com","timestamp":'"'"'$(date +%s)'"'"',"event":"delivered"}]'"'"
echo ""
echo "To stop:"
echo "  pkill -f relay-go"
echo ""