#!/bin/bash

echo "=================================================="
echo "PARQUET Conversion Test Suite"
echo "=================================================="
echo ""

# Check if MinIO is running
if ! curl -s http://localhost:9010 > /dev/null 2>&1; then
    echo "❌ MinIO is not running. Please start it first:"
    echo "   cd /Users/nickmartinez/personal/relay-go-dataviz"
    echo "   ./start-local-minio.sh"
    exit 1
fi
echo "✅ MinIO is running"

# Check if Redis is running
if ! redis-cli -h localhost -p 6379 ping > /dev/null 2>&1; then
    echo "❌ Redis is not running. Please start it first:"
    echo "   cd /Users/nickmartinez/personal/relay-go-dataviz"
    echo "   ./start-redis-local.sh"
    exit 1
fi
echo "✅ Redis is running"

# Start relay-go with PARQUET support
echo ""
echo "Starting relay-go with PARQUET support..."
./start-relay-go-minio.sh

# Wait for relay-go to start
sleep 5

# Check if relay-go is running
if ! curl -s http://localhost:8080/healthcheck > /dev/null 2>&1; then
    echo "❌ relay-go failed to start. Check relay-go.log"
    exit 1
fi
echo "✅ relay-go is running with PARQUET support"

echo ""
echo "=================================================="
echo "Running PARQUET conversion test..."
echo "=================================================="
echo ""

# Run the test replay tool
cd tools

# User selection - convert a manageable dataset (user 6 or 7 with ~92MB)
SOURCE_USER=6  # 92MB, 6,230 files - manageable size
TEST_USER=99996  # Different test user to keep data separate

echo "Converting ALL events for user $SOURCE_USER to PARQUET..."
echo "Source: User $SOURCE_USER (92MB, 6,230 JSON files)"
echo "Target: User $TEST_USER (PARQUET-only mode)"
echo "Using 5000 events per batch for maximum speed..."
echo "Directory structure will be preserved with historical timestamps"
echo ""
go run parquet_test_replay.go \
    -source-user $SOURCE_USER \
    -test-user $TEST_USER \
    -provider sendgrid \
    -batch-size 5000 \
    -max-files 0

echo ""
echo "=================================================="
echo "Test Complete!"
echo "=================================================="
echo ""
echo "Check the results in MinIO:"
echo "1. Open MinIO Console: http://localhost:9011"
echo "2. Login: minioadmin / minioadmin"
echo "3. Browse to: production-relay-go-events-998623545110/events/user_$TEST_USER/"
echo ""
echo "You should see:"
echo "  - PARQUET files ONLY: events/user_$TEST_USER/sendgrid/*/events_*.parquet"
echo "  - Directory structure preserved from original (by date/hour)"
echo "  - No JSON files (PARQUET-only mode)"
echo ""
echo "To check files:"
echo "  mc ls local/production-relay-go-events-998623545110/events/user_$TEST_USER/sendgrid/ --recursive"
echo ""
echo "To see total size comparison:"
echo "  Original JSON (user $SOURCE_USER): mc du local/production-relay-go-events-998623545110/events/user_$SOURCE_USER/sendgrid/"
echo "  PARQUET (user $TEST_USER):        mc du local/production-relay-go-events-998623545110/events/user_$TEST_USER/sendgrid/"
echo ""
echo "Logs:"
echo "  - relay-go: tail -f ../relay-go.log"
echo "  - Test tool output: See above"