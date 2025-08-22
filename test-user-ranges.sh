#\!/bin/bash

echo "Testing User ID Range Processing Logic"
echo "======================================="

WEBHOOK_URL="http://localhost:8080/webhook-events/sendgrid"

# Function to send test event
send_test_event() {
    local user_id=$1
    local test_name=$2
    
    echo -e "\n📧 Testing $test_name (User ID: $user_id)"
    
    curl -s -X POST "$WEBHOOK_URL" \
        -H "Content-Type: application/json" \
        -H "X-Test-Mode: true" \
        -H "X-Test-User-ID: $user_id" \
        -d '[{
            "email": "test-'$user_id'@example.com",
            "timestamp": '$(date +%s)',
            "event": "processed",
            "sg_event_id": "test-'$user_id'-'$(date +%s%N)'",
            "sg_message_id": "test-msg-'$user_id'",
            "category": ["range-test"],
            "smtp-id": "<test-'$user_id'@example.com>"
        }]' > /dev/null && echo "   ✅ Event sent"
}

# Test 1: Legacy customer (should create JSON)
send_test_event 8 "Legacy Customer"

# Test 2: Shadow user (should create PARQUET)
send_test_event 99990008 "Shadow User"

# Test 3: Another legacy customer 
send_test_event 42 "Legacy Customer #42"

# Test 4: Its shadow
send_test_event 99990042 "Shadow User #42"

# Test 5: New customer with Snowflake ID (should create PARQUET)
send_test_event 138629825757335552 "New Customer (Snowflake ID)"

echo -e "\n⏳ Waiting 35 seconds for batch processing..."
sleep 35

echo -e "\n📊 Checking results in MinIO:"

echo -e "\n1️⃣ User 8 (Legacy - should have JSON only):"
mc ls local/production-relay-go-events-998623545110/events/user_8/sendgrid/ --recursive 2>/dev/null | tail -2

echo -e "\n2️⃣ User 99990008 (Shadow - should have PARQUET only):"
mc ls local/production-relay-go-events-998623545110/events/user_99990008/sendgrid/ --recursive 2>/dev/null | tail -2

echo -e "\n3️⃣ User 42 (Legacy - should have JSON only):"
mc ls local/production-relay-go-events-998623545110/events/user_42/sendgrid/ --recursive 2>/dev/null | tail -2

echo -e "\n4️⃣ User 99990042 (Shadow - should have PARQUET only):"
mc ls local/production-relay-go-events-998623545110/events/user_99990042/sendgrid/ --recursive 2>/dev/null | tail -2

echo -e "\n5️⃣ User 138629825757335552 (New Customer - should have PARQUET only):"
mc ls local/production-relay-go-events-998623545110/events/user_138629825757335552/sendgrid/ --recursive 2>/dev/null | tail -2

echo -e "\n✅ Test complete\!"
