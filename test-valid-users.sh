#\!/bin/bash

echo "Testing with Valid Users (have SendGrid verification keys)"
echo "=========================================================="

WEBHOOK_URL="http://localhost:8080/webhook-events/sendgrid"

# Send test events for user 11 (exists in DB with verification key)
echo -e "\n📧 Sending 20 events for User 11 (Legacy user with verification key)..."

for i in {1..20}; do
    curl -s -X POST "$WEBHOOK_URL" \
        -H "Content-Type: application/json" \
        -H "X-Test-Mode: true" \
        -H "X-Test-User-ID: 11" \
        -d '[{
            "email": "test-'$i'@sgtest.com",
            "timestamp": '$(date +%s)',
            "event": "processed",
            "sg_event_id": "valid-test-'$i'-'$(date +%s%N)'",
            "sg_message_id": "valid-msg-'$i'",
            "category": ["valid-user-test"],
            "smtp-id": "<test-'$i'@sgtest.com>"
        }]' > /dev/null
    echo -n "."
done
echo " ✅ 20 events sent"

echo -e "\n⏳ Waiting 35 seconds for JSON batch processing..."
sleep 35

echo -e "\n📊 Checking results:"

echo -e "\n1️⃣ User 11 (should have JSON):"
mc ls local/production-relay-go-events-998623545110/events/user_11/sendgrid/ --recursive 2>/dev/null | grep "\.json$" | tail -3

echo -e "\n2️⃣ User 99990011 (shadow - should have PARQUET):"
mc ls local/production-relay-go-events-998623545110/events/user_99990011/sendgrid/ --recursive 2>/dev/null | grep "\.parquet$" | tail -3

# Check Redis
echo -e "\n📦 Redis status:"
echo -n "User 11: "
redis-cli -h localhost -p 6379 -a app_password --no-auth-warning HLEN "provider:sendgrid:user:11:events"
echo -n "User 99990011: "
redis-cli -h localhost -p 6379 -a app_password --no-auth-warning HLEN "provider:sendgrid:user:99990011:events"

echo -e "\n✅ Test complete\!"
