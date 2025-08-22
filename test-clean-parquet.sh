#!/bin/bash

echo "========================================="
echo "Clean PARQUET Test After Fix"
echo "========================================="

WEBHOOK_URL="http://localhost:8080/webhook-events/sendgrid"

# Clear Redis first
echo "🧹 Clearing Redis test data..."
redis-cli -h localhost -p 6379 -a app_password --no-auth-warning DEL "provider:sendgrid:user:8:events" > /dev/null
redis-cli -h localhost -p 6379 -a app_password --no-auth-warning DEL "provider:sendgrid:user:99990008:events" > /dev/null

# Send 5 test events for user 8
echo -e "\n📧 Sending 5 events for User 8 (legacy)..."
for i in {1..5}; do
    curl -s -X POST "$WEBHOOK_URL" \
        -H "Content-Type: application/json" \
        -H "X-Test-Mode: true" \
        -H "X-Test-User-ID: 8" \
        -d '[{
            "email": "clean-test-'$i'@example.com",
            "timestamp": '$(date +%s)',
            "event": "processed",
            "sg_event_id": "clean-'$i'-'$(date +%s%N)'",
            "sg_message_id": "clean-msg-'$i'",
            "category": ["clean-test"],
            "smtp-id": "<clean-'$i'@example.com>"
        }]' > /dev/null
    echo -n "."
done
echo " ✅"

# Check Redis immediately
echo -e "\n📊 Redis after sending:"
echo -n "  User 8: "
redis-cli -h localhost -p 6379 -a app_password --no-auth-warning HLEN "provider:sendgrid:user:8:events"
echo -n "  User 99990008: "
redis-cli -h localhost -p 6379 -a app_password --no-auth-warning HLEN "provider:sendgrid:user:99990008:events"

# Wait for JSON batch processing
echo -e "\n⏳ Waiting 35 seconds for JSON batch..."
sleep 35

# Check Redis after batch
echo -e "\n📊 Redis after JSON batch:"
echo -n "  User 8: "
redis-cli -h localhost -p 6379 -a app_password --no-auth-warning HLEN "provider:sendgrid:user:8:events"
echo -n "  User 99990008: "
redis-cli -h localhost -p 6379 -a app_password --no-auth-warning HLEN "provider:sendgrid:user:99990008:events"

# Check files in MinIO
echo -e "\n📁 Files in MinIO:"
echo "User 8 (should have ONLY JSON):"
mc ls local/production-relay-go-events-998623545110/events/user_8/sendgrid/2025/08/12/ --recursive 2>/dev/null | tail -3

echo -e "\nChecking for PARQUET files in user_8:"
PARQUET_COUNT=$(mc ls local/production-relay-go-events-998623545110/events/user_8/sendgrid/ --recursive 2>/dev/null | grep -c "\.parquet" || echo "0")
if [ "$PARQUET_COUNT" -eq "0" ]; then
    echo "✅ No PARQUET files in user_8 directory"
else
    echo "❌ Found $PARQUET_COUNT PARQUET files in user_8 directory (SHOULD BE 0!)"
    mc ls local/production-relay-go-events-998623545110/events/user_8/sendgrid/ --recursive 2>/dev/null | grep "\.parquet"
fi

echo -e "\nUser 99990008 (shadow - waiting for PARQUET batch):"
mc ls local/production-relay-go-events-998623545110/events/user_99990008/sendgrid/ --recursive 2>/dev/null | tail -3

echo -e "\n✅ Test complete!"