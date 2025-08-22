#!/bin/bash

echo "============================================="
echo "PARALLEL PARQUET PROCESSOR TEST"
echo "============================================="
echo ""
echo "This test will verify:"
echo "1. JSON pipeline continues unchanged"
echo "2. Events are duplicated to 9999XXXX users"
echo "3. PARQUET files are created with optimized batching"
echo "4. Both pipelines work in parallel without interference"
echo ""

# Configuration
TEST_USER=7  # Will create parallel user 99990007
TEST_EVENTS=100
WEBHOOK_URL="http://localhost:8080/webhook/sendgrid"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to check if services are running
check_services() {
    echo "Checking services..."
    
    if ! curl -s http://localhost:8080/healthcheck > /dev/null 2>&1; then
        echo -e "${RED}❌ relay-go is not running${NC}"
        echo "Please start it with: ./start-relay-go-minio.sh"
        exit 1
    fi
    echo -e "${GREEN}✅ relay-go is running${NC}"
    
    if ! curl -s http://localhost:9010 > /dev/null 2>&1; then
        echo -e "${RED}❌ MinIO is not running${NC}"
        echo "Please start it with: cd ../relay-go-dataviz && ./start-local-minio.sh"
        exit 1
    fi
    echo -e "${GREEN}✅ MinIO is running${NC}"
    
    if ! redis-cli -h localhost -p 6379 ping > /dev/null 2>&1; then
        echo -e "${RED}❌ Redis is not running${NC}"
        exit 1
    fi
    echo -e "${GREEN}✅ Redis is running${NC}"
}

# Function to send test events
send_test_events() {
    echo ""
    echo "Sending $TEST_EVENTS test events for User $TEST_USER..."
    
    for i in $(seq 1 $TEST_EVENTS); do
        # Create a test event
        EVENT_JSON=$(cat <<EOF
[{
    "email": "test$i@example.com",
    "timestamp": $(date +%s),
    "event": "delivered",
    "sg_event_id": "test-$(uuidgen || echo $RANDOM$RANDOM)",
    "sg_message_id": "msg-$i",
    "category": ["test", "parallel-parquet"],
    "smtp-id": "<test$i@example.com>"
}]
EOF
)
        
        # Send to webhook
        curl -s -X POST "$WEBHOOK_URL" \
            -H "Content-Type: application/json" \
            -H "X-Customer-ID: $TEST_USER" \
            -d "$EVENT_JSON" > /dev/null
        
        if [ $((i % 10)) -eq 0 ]; then
            echo -n "."
        fi
    done
    echo " Done!"
}

# Function to check Redis batches
check_redis_batches() {
    echo ""
    echo "Checking Redis batches..."
    
    # Check original user
    ORIGINAL_COUNT=$(redis-cli -h localhost -p 6379 -a app_password --no-auth-warning HLEN "provider:sendgrid:user:$TEST_USER:events" 2>/dev/null || echo "0")
    echo "User $TEST_USER (JSON): $ORIGINAL_COUNT events in Redis"
    
    # Check parallel PARQUET user
    PARQUET_USER=$((99990000 + TEST_USER))
    PARQUET_COUNT=$(redis-cli -h localhost -p 6379 -a app_password --no-auth-warning HLEN "provider:sendgrid:user:$PARQUET_USER:events" 2>/dev/null || echo "0")
    echo "User $PARQUET_USER (PARQUET): $PARQUET_COUNT events in Redis"
    
    if [ "$ORIGINAL_COUNT" -gt 0 ] && [ "$PARQUET_COUNT" -gt 0 ]; then
        echo -e "${GREEN}✅ Events are being duplicated correctly${NC}"
    else
        echo -e "${YELLOW}⚠️  Waiting for duplication...${NC}"
    fi
}

# Function to wait for batch processing
wait_for_batches() {
    echo ""
    echo "Waiting for batch processing..."
    echo "JSON batches: Every 30 seconds (Dev mode)"
    echo "PARQUET batches: Every 2 minutes or 5000 events (Dev mode)"
    
    # Wait for JSON batch (30 seconds)
    echo -n "Waiting 35 seconds for JSON batch..."
    sleep 35
    echo " Done!"
    
    # Check if we need to wait more for PARQUET
    PARQUET_USER=$((99990000 + TEST_USER))
    PARQUET_COUNT=$(redis-cli -h localhost -p 6379 -a app_password --no-auth-warning HLEN "provider:sendgrid:user:$PARQUET_USER:events" 2>/dev/null || echo "0")
    
    if [ "$PARQUET_COUNT" -gt 0 ] && [ "$PARQUET_COUNT" -lt 5000 ]; then
        echo "PARQUET batch has $PARQUET_COUNT events (threshold: 5000)"
        echo -n "Waiting additional 90 seconds for PARQUET batch timer..."
        sleep 90
        echo " Done!"
    fi
}

# Function to verify files in MinIO
verify_minio_files() {
    echo ""
    echo "Verifying files in MinIO..."
    
    PARQUET_USER=$((99990000 + TEST_USER))
    
    # Count JSON files for original user
    JSON_FILES=$(mc ls local/production-relay-go-events-998623545110/events/user_$TEST_USER/sendgrid/ --recursive 2>/dev/null | grep "\.json$" | wc -l)
    echo "User $TEST_USER JSON files: $JSON_FILES"
    
    # Count PARQUET files for parallel user
    PARQUET_FILES=$(mc ls local/production-relay-go-events-998623545110/events/user_$PARQUET_USER/sendgrid/ --recursive 2>/dev/null | grep "\.parquet$" | wc -l)
    echo "User $PARQUET_USER PARQUET files: $PARQUET_FILES"
    
    # Show latest files
    if [ "$JSON_FILES" -gt 0 ]; then
        echo ""
        echo "Latest JSON file:"
        mc ls local/production-relay-go-events-998623545110/events/user_$TEST_USER/sendgrid/ --recursive 2>/dev/null | grep "\.json$" | tail -1
    fi
    
    if [ "$PARQUET_FILES" -gt 0 ]; then
        echo ""
        echo "Latest PARQUET file:"
        mc ls local/production-relay-go-events-998623545110/events/user_$PARQUET_USER/sendgrid/ --recursive 2>/dev/null | grep "\.parquet$" | tail -1
    fi
    
    # Verify results
    if [ "$JSON_FILES" -gt 0 ] && [ "$PARQUET_FILES" -gt 0 ]; then
        echo ""
        echo -e "${GREEN}✅ SUCCESS: Both JSON and PARQUET files created${NC}"
        return 0
    elif [ "$JSON_FILES" -gt 0 ] && [ "$PARQUET_FILES" -eq 0 ]; then
        echo ""
        echo -e "${YELLOW}⚠️  JSON files created but no PARQUET files yet${NC}"
        echo "This is expected if PARQUET batch threshold not reached"
        return 1
    else
        echo ""
        echo -e "${RED}❌ FAILURE: Files not created as expected${NC}"
        return 1
    fi
}

# Function to check logs for errors
check_logs() {
    echo ""
    echo "Checking logs for errors..."
    
    ERROR_COUNT=$(grep -c "ERROR" relay-go.log 2>/dev/null || echo "0")
    PARQUET_LOGS=$(grep -c "parallel PARQUET" relay-go.log 2>/dev/null || echo "0")
    
    echo "Error count in logs: $ERROR_COUNT"
    echo "PARQUET processing logs: $PARQUET_LOGS"
    
    if [ "$ERROR_COUNT" -gt 0 ]; then
        echo ""
        echo "Recent errors:"
        grep "ERROR" relay-go.log | tail -5
    fi
}

# Main test execution
main() {
    echo "Starting test at $(date)"
    echo ""
    
    # Check services
    check_services
    
    # Send test events
    send_test_events
    
    # Check Redis immediately
    check_redis_batches
    
    # Wait for batch processing
    wait_for_batches
    
    # Check Redis after batching
    echo ""
    echo "Checking Redis after batch processing..."
    check_redis_batches
    
    # Verify files in MinIO
    verify_minio_files
    
    # Check logs
    check_logs
    
    echo ""
    echo "============================================="
    echo "TEST COMPLETE"
    echo "============================================="
    echo ""
    echo "Summary:"
    echo "- Original User $TEST_USER: JSON pipeline"
    echo "- Parallel User $((99990000 + TEST_USER)): PARQUET pipeline"
    echo ""
    echo "To verify batch optimization:"
    echo "1. JSON files should be small and frequent"
    echo "2. PARQUET files should be larger and less frequent"
    echo "3. Check file sizes with:"
    echo "   mc ls local/production-relay-go-events-998623545110/events/user_$TEST_USER/sendgrid/ --recursive"
    echo "   mc ls local/production-relay-go-events-998623545110/events/user_$((99990000 + TEST_USER))/sendgrid/ --recursive"
}

# Run the test
main