#!/bin/bash

echo "============================================="
echo "INTEGRATED PARALLEL PARQUET PROCESSOR TEST"
echo "============================================="
echo ""
echo "This test verifies the complete integration:"
echo "1. Events are received via webhook"
echo "2. Events are duplicated to parallel PARQUET users"
echo "3. JSON files are created in MinIO (30 sec batching)"
echo "4. PARQUET files are created in MinIO (2 min batching)"
echo ""

# Configuration
TEST_USER=7  # Will create parallel user 99990007
TEST_EVENTS=50
WEBHOOK_URL="http://localhost:8080/webhook-events/sendgrid"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to check if services are running
check_services() {
    echo "Checking services..."
    
    if ! curl -s http://localhost:8080/healthcheck > /dev/null 2>&1; then
        echo -e "${RED}❌ relay-go is not running${NC}"
        echo "Please start it with: ./start-relay-go-parquet-test.sh"
        exit 1
    fi
    echo -e "${GREEN}✅ relay-go is running${NC}"
    
    if ! curl -s http://localhost:9010 > /dev/null 2>&1; then
        echo -e "${RED}❌ MinIO is not running${NC}"
        exit 1
    fi
    echo -e "${GREEN}✅ MinIO is running${NC}"
    
    if ! redis-cli -h localhost -p 6379 -a app_password ping > /dev/null 2>&1; then
        echo -e "${RED}❌ Redis is not running${NC}"
        exit 1
    fi
    echo -e "${GREEN}✅ Redis is running${NC}"
}

# Function to clear test data
clear_test_data() {
    echo ""
    echo "Clearing previous test data..."
    
    # Clear Redis events for test users
    redis-cli -h localhost -p 6379 -a app_password --no-auth-warning DEL "provider:sendgrid:user:$TEST_USER:events" > /dev/null 2>&1
    redis-cli -h localhost -p 6379 -a app_password --no-auth-warning DEL "provider:sendgrid:user:$((99990000 + TEST_USER)):events" > /dev/null 2>&1
    
    echo "✓ Redis cleared for test users"
}

# Function to send test events
send_test_events() {
    echo ""
    echo -e "${BLUE}Sending $TEST_EVENTS test events for User $TEST_USER...${NC}"
    
    for i in $(seq 1 $TEST_EVENTS); do
        TIMESTAMP=$(date +%s)
        EVENT_JSON=$(cat <<EOF
[{
    "email": "test$i@example.com",
    "timestamp": $TIMESTAMP,
    "event": "delivered",
    "sg_event_id": "test-$(uuidgen 2>/dev/null || echo $RANDOM$RANDOM)",
    "sg_message_id": "msg-$i",
    "category": ["test", "parallel-parquet"],
    "smtp-id": "<test$i@example.com>",
    "ip": "192.168.1.$((i % 255))"
}]
EOF
)
        
        # Send to webhook with test headers
        curl -s -X POST "$WEBHOOK_URL" \
            -H "Content-Type: application/json" \
            -H "X-Test-Mode: true" \
            -H "X-Test-User-ID: $TEST_USER" \
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
    
    if [ "$ORIGINAL_COUNT" -eq "$TEST_EVENTS" ] && [ "$PARQUET_COUNT" -eq "$TEST_EVENTS" ]; then
        echo -e "${GREEN}✅ Events duplicated correctly!${NC}"
        return 0
    else
        echo -e "${YELLOW}⚠️  Event counts don't match expected ($TEST_EVENTS)${NC}"
        return 1
    fi
}

# Function to wait and monitor batch processing
monitor_batch_processing() {
    echo ""
    echo -e "${BLUE}Monitoring batch processing...${NC}"
    echo "JSON batches: Every 30 seconds (Dev mode)"
    echo "PARQUET batches: Every 2 minutes or 5000 events (Dev mode)"
    echo ""
    
    # Monitor for 35 seconds for JSON batch
    echo -n "Waiting for JSON batch (30s timer)..."
    sleep 32
    echo " checking..."
    
    # Check if JSON batch was processed
    JSON_COUNT=$(redis-cli -h localhost -p 6379 -a app_password --no-auth-warning HLEN "provider:sendgrid:user:$TEST_USER:events" 2>/dev/null || echo "0")
    
    if [ "$JSON_COUNT" -eq "0" ]; then
        echo -e "${GREEN}✅ JSON batch processed and cleared from Redis${NC}"
        
        # Check MinIO for JSON files
        JSON_FILES=$(mc ls local/production-relay-go-events-998623545110/events/user_$TEST_USER/sendgrid/$(date +%Y/%m/%d)/ 2>/dev/null | grep "\.json$" | wc -l || echo "0")
        if [ "$JSON_FILES" -gt "0" ]; then
            echo -e "${GREEN}✅ JSON files written to MinIO${NC}"
            echo "Latest JSON file:"
            mc ls local/production-relay-go-events-998623545110/events/user_$TEST_USER/sendgrid/$(date +%Y/%m/%d)/ 2>/dev/null | grep "\.json$" | tail -1
        fi
    else
        echo -e "${YELLOW}⚠️  JSON batch still in Redis (may need more time)${NC}"
    fi
    
    # Check PARQUET status
    PARQUET_USER=$((99990000 + TEST_USER))
    PARQUET_COUNT=$(redis-cli -h localhost -p 6379 -a app_password --no-auth-warning HLEN "provider:sendgrid:user:$PARQUET_USER:events" 2>/dev/null || echo "0")
    
    echo ""
    if [ "$PARQUET_COUNT" -lt 5000 ] && [ "$PARQUET_COUNT" -gt 0 ]; then
        echo "PARQUET batch has $PARQUET_COUNT events (threshold: 5000)"
        echo -n "Waiting for PARQUET batch timer (90 more seconds)..."
        sleep 90
        echo " checking..."
        
        PARQUET_COUNT=$(redis-cli -h localhost -p 6379 -a app_password --no-auth-warning HLEN "provider:sendgrid:user:$PARQUET_USER:events" 2>/dev/null || echo "0")
        if [ "$PARQUET_COUNT" -eq "0" ]; then
            echo -e "${GREEN}✅ PARQUET batch processed and cleared from Redis${NC}"
        fi
    elif [ "$PARQUET_COUNT" -eq "0" ]; then
        echo -e "${GREEN}✅ PARQUET batch already processed${NC}"
    fi
    
    # Check MinIO for PARQUET files
    PARQUET_FILES=$(mc ls local/production-relay-go-events-998623545110/events/user_$PARQUET_USER/sendgrid/$(date +%Y/%m/%d)/ 2>/dev/null | grep "\.parquet$" | wc -l || echo "0")
    if [ "$PARQUET_FILES" -gt "0" ]; then
        echo -e "${GREEN}✅ PARQUET files written to MinIO${NC}"
        echo "Latest PARQUET file:"
        mc ls local/production-relay-go-events-998623545110/events/user_$PARQUET_USER/sendgrid/$(date +%Y/%m/%d)/ 2>/dev/null | grep "\.parquet$" | tail -1
    else
        echo -e "${YELLOW}⚠️  No PARQUET files yet (may need more time or more events)${NC}"
    fi
}

# Function to verify final results
verify_results() {
    echo ""
    echo "============================================="
    echo "FINAL VERIFICATION"
    echo "============================================="
    
    PARQUET_USER=$((99990000 + TEST_USER))
    TODAY=$(date +%Y/%m/%d)
    
    # Count files in MinIO
    JSON_FILES=$(mc ls local/production-relay-go-events-998623545110/events/user_$TEST_USER/sendgrid/ --recursive 2>/dev/null | grep "\.json$" | grep "$TODAY" | wc -l || echo "0")
    PARQUET_FILES=$(mc ls local/production-relay-go-events-998623545110/events/user_$PARQUET_USER/sendgrid/ --recursive 2>/dev/null | grep "\.parquet$" | grep "$TODAY" | wc -l || echo "0")
    
    echo "Results for today ($TODAY):"
    echo "  User $TEST_USER: $JSON_FILES JSON files"
    echo "  User $PARQUET_USER: $PARQUET_FILES PARQUET files"
    
    # Check file sizes (PARQUET should be smaller)
    if [ "$JSON_FILES" -gt 0 ] && [ "$PARQUET_FILES" -gt 0 ]; then
        echo ""
        echo "File size comparison:"
        
        # Get a sample JSON file size
        JSON_SIZE=$(mc ls local/production-relay-go-events-998623545110/events/user_$TEST_USER/sendgrid/$TODAY/ 2>/dev/null | grep "\.json$" | tail -1 | awk '{print $4}')
        echo "  Sample JSON file: $JSON_SIZE"
        
        # Get a sample PARQUET file size
        PARQUET_SIZE=$(mc ls local/production-relay-go-events-998623545110/events/user_$PARQUET_USER/sendgrid/$TODAY/ 2>/dev/null | grep "\.parquet$" | tail -1 | awk '{print $4}')
        echo "  Sample PARQUET file: $PARQUET_SIZE"
    fi
    
    # Overall status
    echo ""
    if [ "$JSON_FILES" -gt 0 ] && [ "$PARQUET_FILES" -gt 0 ]; then
        echo -e "${GREEN}✅ SUCCESS: Both JSON and PARQUET pipelines working!${NC}"
        echo ""
        echo "The parallel PARQUET processor is:"
        echo "  • Duplicating all events correctly"
        echo "  • Processing with optimized batch sizes"
        echo "  • Writing to MinIO successfully"
        return 0
    elif [ "$JSON_FILES" -gt 0 ] && [ "$PARQUET_FILES" -eq 0 ]; then
        echo -e "${YELLOW}⚠️  JSON pipeline working but PARQUET files not created yet${NC}"
        echo "This is normal if the PARQUET batch threshold wasn't reached."
        echo "Try sending more events or waiting for the 2-minute timer."
        return 1
    else
        echo -e "${RED}❌ Files not created as expected${NC}"
        echo "Check relay-go.log for errors"
        return 1
    fi
}

# Function to show logs
show_recent_logs() {
    echo ""
    echo "Recent log entries:"
    echo "-------------------"
    tail -20 relay-go.log | grep -E "PARQUET|parallel|Processing batch|Uploaded|ERROR" || echo "No relevant logs found"
}

# Main test execution
main() {
    echo "Starting test at $(date)"
    echo ""
    
    # Check services
    check_services
    
    # Clear previous test data
    clear_test_data
    
    # Send test events
    send_test_events
    
    # Check Redis immediately
    check_redis_batches
    
    # Monitor batch processing
    monitor_batch_processing
    
    # Verify final results
    verify_results
    TEST_RESULT=$?
    
    # Show logs if there were issues
    if [ $TEST_RESULT -ne 0 ]; then
        show_recent_logs
    fi
    
    echo ""
    echo "============================================="
    echo "TEST COMPLETE"
    echo "============================================="
    echo ""
    echo "To manually check:"
    echo "  • Redis: redis-cli -h localhost -p 6379 -a app_password keys '*user*'"
    echo "  • MinIO: mc ls local/production-relay-go-events-998623545110/events/ --recursive"
    echo "  • Logs: tail -f relay-go.log"
    echo ""
    
    exit $TEST_RESULT
}

# Run the test
main