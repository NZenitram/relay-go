#!/bin/bash

echo "==========================================="
echo "Parallel PARQUET Integration Test with MinIO"
echo "==========================================="

# Configuration
WEBHOOK_URL="http://localhost:8080/webhook-events/sendgrid"
TEST_USER=8
PARQUET_USER=$((99990000 + TEST_USER))
REDIS_CLI="redis-cli -h localhost -p 6379 -a app_password --no-auth-warning"
MINIO_ENDPOINT="http://localhost:9010"
BUCKET="production-relay-go-events-998623545110"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to check MinIO files
check_minio_files() {
    local user_id=$1
    local provider=$2
    local format=$3
    
    echo -e "\n📁 Checking MinIO for user $user_id ($format)..."
    
    AWS_ACCESS_KEY_ID=minioadmin \
    AWS_SECRET_ACCESS_KEY=minioadmin \
    aws --endpoint-url=$MINIO_ENDPOINT \
        s3 ls s3://$BUCKET/events/user_${user_id}/${provider}/ \
        --recursive 2>/dev/null | grep "\.${format}$" | head -5
}

# Clear previous test data
echo "🧹 Clearing previous test data..."
$REDIS_CLI DEL "provider:sendgrid:user:$TEST_USER:events" > /dev/null
$REDIS_CLI DEL "provider:sendgrid:user:$PARQUET_USER:events" > /dev/null

# Clear MinIO test data (optional)
AWS_ACCESS_KEY_ID=minioadmin \
AWS_SECRET_ACCESS_KEY=minioadmin \
aws --endpoint-url=$MINIO_ENDPOINT \
    s3 rm s3://$BUCKET/events/user_${TEST_USER}/ --recursive 2>/dev/null
    
AWS_ACCESS_KEY_ID=minioadmin \
AWS_SECRET_ACCESS_KEY=minioadmin \
aws --endpoint-url=$MINIO_ENDPOINT \
    s3 rm s3://$BUCKET/events/user_${PARQUET_USER}/ --recursive 2>/dev/null

echo "✅ Test data cleared"

# Create test events
echo -e "\n📨 Sending test events..."
for i in {1..10}; do
    EVENT_JSON=$(cat <<EOF
[{
    "email": "test$i@example.com",
    "timestamp": $(date +%s),
    "event": "processed",
    "sg_event_id": "test-event-$i-$(date +%s%N)",
    "sg_message_id": "test-msg-$i",
    "category": ["test"],
    "smtp-id": "<test$i@example.com>",
    "unique_args": {}
}]
EOF
    )
    
    curl -s -X POST "$WEBHOOK_URL" \
        -H "Content-Type: application/json" \
        -H "X-Test-Mode: true" \
        -H "X-Test-User-ID: $TEST_USER" \
        -d "$EVENT_JSON" > /dev/null
    
    if [ $? -eq 0 ]; then
        echo -n "."
    else
        echo -n "!"
    fi
done
echo -e "\n✅ Sent 10 test events"

# Check Redis for immediate storage
echo -e "\n📊 Checking Redis storage..."
sleep 2

ORIGINAL_COUNT=$($REDIS_CLI HLEN "provider:sendgrid:user:$TEST_USER:events")
PARQUET_COUNT=$($REDIS_CLI HLEN "provider:sendgrid:user:$PARQUET_USER:events")

echo "  Original user $TEST_USER: $ORIGINAL_COUNT events"
echo "  PARQUET user $PARQUET_USER: $PARQUET_COUNT events"

if [ "$ORIGINAL_COUNT" -eq "10" ] && [ "$PARQUET_COUNT" -eq "10" ]; then
    echo -e "${GREEN}✅ Event duplication working correctly${NC}"
else
    echo -e "${RED}❌ Event duplication failed${NC}"
fi

# Wait for batch processing
echo -e "\n⏳ Waiting for batch processing (35 seconds for JSON, up to 2 min for PARQUET)..."
echo "   JSON batches every 30s, PARQUET batches every 2min or 5000 events"

# Monitor for JSON upload (should happen at 30s)
echo -e "\n📤 Monitoring JSON batch processing..."
for i in {1..40}; do
    sleep 1
    echo -n "."
    
    # Check if JSON files appeared in MinIO
    JSON_FILES=$(AWS_ACCESS_KEY_ID=minioadmin \
                 AWS_SECRET_ACCESS_KEY=minioadmin \
                 aws --endpoint-url=$MINIO_ENDPOINT \
                     s3 ls s3://$BUCKET/events/user_${TEST_USER}/sendgrid/ \
                     --recursive 2>/dev/null | grep "\.json$" | wc -l)
    
    if [ "$JSON_FILES" -gt 0 ]; then
        echo -e "\n${GREEN}✅ JSON file uploaded after ~${i} seconds${NC}"
        check_minio_files $TEST_USER "sendgrid" "json"
        break
    fi
done

# Check Redis to see if events were processed
echo -e "\n📊 Checking Redis after batch processing..."
ORIGINAL_COUNT=$($REDIS_CLI HLEN "provider:sendgrid:user:$TEST_USER:events")
PARQUET_COUNT=$($REDIS_CLI HLEN "provider:sendgrid:user:$PARQUET_USER:events")

echo "  Original user $TEST_USER: $ORIGINAL_COUNT events remaining"
echo "  PARQUET user $PARQUET_USER: $PARQUET_COUNT events remaining"

# Send more events to trigger PARQUET batch (if needed)
if [ "$PARQUET_COUNT" -gt 0 ]; then
    echo -e "\n📨 Sending more events to trigger PARQUET batch..."
    for i in {11..50}; do
        EVENT_JSON=$(cat <<EOF
[{
    "email": "test$i@example.com",
    "timestamp": $(date +%s),
    "event": "delivered",
    "sg_event_id": "test-event-$i-$(date +%s%N)",
    "sg_message_id": "test-msg-$i",
    "category": ["test"],
    "smtp-id": "<test$i@example.com>",
    "unique_args": {}
}]
EOF
        )
        
        curl -s -X POST "$WEBHOOK_URL" \
            -H "Content-Type: application/json" \
            -H "X-Test-Mode: true" \
            -H "X-Test-User-ID: $TEST_USER" \
            -d "$EVENT_JSON" > /dev/null
        
        echo -n "."
    done
    echo -e "\n✅ Sent 40 more events"
fi

# Wait for PARQUET processing
echo -e "\n📤 Monitoring PARQUET batch processing..."
for i in {1..130}; do
    sleep 1
    echo -n "."
    
    # Check if PARQUET files appeared in MinIO
    PARQUET_FILES=$(AWS_ACCESS_KEY_ID=minioadmin \
                    AWS_SECRET_ACCESS_KEY=minioadmin \
                    aws --endpoint-url=$MINIO_ENDPOINT \
                        s3 ls s3://$BUCKET/events/user_${PARQUET_USER}/sendgrid/ \
                        --recursive 2>/dev/null | grep "\.parquet$" | wc -l)
    
    if [ "$PARQUET_FILES" -gt 0 ]; then
        echo -e "\n${GREEN}✅ PARQUET file uploaded after ~${i} seconds${NC}"
        check_minio_files $PARQUET_USER "sendgrid" "parquet"
        break
    fi
    
    # Check every 30 seconds
    if [ $((i % 30)) -eq 0 ]; then
        PARQUET_COUNT=$($REDIS_CLI HLEN "provider:sendgrid:user:$PARQUET_USER:events")
        echo -e "\n   PARQUET user still has $PARQUET_COUNT events in Redis"
    fi
done

# Final summary
echo -e "\n==========================================="
echo "📊 Final Summary"
echo "==========================================="

# Check MinIO for both file types
JSON_FILES=$(AWS_ACCESS_KEY_ID=minioadmin \
             AWS_SECRET_ACCESS_KEY=minioadmin \
             aws --endpoint-url=$MINIO_ENDPOINT \
                 s3 ls s3://$BUCKET/events/user_${TEST_USER}/sendgrid/ \
                 --recursive 2>/dev/null | grep "\.json$" | wc -l)

PARQUET_FILES=$(AWS_ACCESS_KEY_ID=minioadmin \
                AWS_SECRET_ACCESS_KEY=minioadmin \
                aws --endpoint-url=$MINIO_ENDPOINT \
                    s3 ls s3://$BUCKET/events/user_${PARQUET_USER}/sendgrid/ \
                    --recursive 2>/dev/null | grep "\.parquet$" | wc -l)

echo "  JSON files in MinIO: $JSON_FILES"
echo "  PARQUET files in MinIO: $PARQUET_FILES"

# Get file sizes for comparison
if [ "$JSON_FILES" -gt 0 ] && [ "$PARQUET_FILES" -gt 0 ]; then
    echo -e "\n📏 File size comparison:"
    
    # Get latest JSON file size
    JSON_SIZE=$(AWS_ACCESS_KEY_ID=minioadmin \
                AWS_SECRET_ACCESS_KEY=minioadmin \
                aws --endpoint-url=$MINIO_ENDPOINT \
                    s3 ls s3://$BUCKET/events/user_${TEST_USER}/sendgrid/ \
                    --recursive 2>/dev/null | grep "\.json$" | tail -1 | awk '{print $3}')
    
    # Get latest PARQUET file size
    PARQUET_SIZE=$(AWS_ACCESS_KEY_ID=minioadmin \
                   AWS_SECRET_ACCESS_KEY=minioadmin \
                   aws --endpoint-url=$MINIO_ENDPOINT \
                       s3 ls s3://$BUCKET/events/user_${PARQUET_USER}/sendgrid/ \
                       --recursive 2>/dev/null | grep "\.parquet$" | tail -1 | awk '{print $3}')
    
    echo "  Latest JSON file: ${JSON_SIZE} bytes"
    echo "  Latest PARQUET file: ${PARQUET_SIZE} bytes"
    
    if [ -n "$JSON_SIZE" ] && [ -n "$PARQUET_SIZE" ] && [ "$PARQUET_SIZE" -lt "$JSON_SIZE" ]; then
        RATIO=$(echo "scale=2; $JSON_SIZE / $PARQUET_SIZE" | bc)
        echo -e "  ${GREEN}✅ PARQUET is ${RATIO}x smaller than JSON${NC}"
    fi
fi

# Overall status
echo -e "\n==========================================="
if [ "$JSON_FILES" -gt 0 ] && [ "$PARQUET_FILES" -gt 0 ]; then
    echo -e "${GREEN}✅ PARALLEL PARQUET INTEGRATION TEST PASSED${NC}"
    echo "   - JSON pipeline: Working"
    echo "   - PARQUET pipeline: Working"
    echo "   - MinIO integration: Working"
    echo "   - Event duplication: Working"
else
    echo -e "${RED}❌ PARALLEL PARQUET INTEGRATION TEST FAILED${NC}"
    [ "$JSON_FILES" -eq 0 ] && echo "   - JSON pipeline: FAILED"
    [ "$PARQUET_FILES" -eq 0 ] && echo "   - PARQUET pipeline: FAILED"
fi
echo "==========================================="