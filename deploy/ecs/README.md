# ECS Task Definition Deployment

This directory contains the ECS task definition for deploying the relay-go application with Redis and DynamoDB.

## Prerequisites

1. AWS CLI installed and configured
2. Appropriate AWS permissions
3. ECR repository for the application image
4. CloudWatch log group created
5. EFS file system for Redis data persistence
6. DynamoDB table for user data

## Required AWS Resources

Before deploying, ensure you have:

1. ECR Repository:
   ```bash
   aws ecr create-repository --repository-name sh-relay-go/sh-relay-go-ingest
   ```

2. CloudWatch Log Group:
   ```bash
   aws logs create-log-group --log-group-name /ecs/relay-go
   ```

3. DynamoDB Table:
   ```bash
   # Create the users table
   aws dynamodb create-table \
     --table-name users \
     --attribute-definitions \
       AttributeName=id,AttributeType=N \
     --key-schema \
       AttributeName=id,KeyType=HASH \
     --provisioned-throughput \
       ReadCapacityUnits=5,WriteCapacityUnits=5 \
     --tags Key=Name,Value=relay-go-users

   # Wait for table creation
   aws dynamodb wait table-exists --table-name users
   ```

4. DynamoDB CRUD Operations:
   ```bash
   # Create a user
   aws dynamodb put-item \
     --table-name users \
     --item '{
       "id": {"N": "1"},
       "email": {"S": "user@example.com"},
       "sendgrid_verification_key": {"S": "key123"},
       "created_at": {"N": "1234567890"},
       "updated_at": {"N": "1234567890"}
     }'

   # Get a user
   aws dynamodb get-item \
     --table-name users \
     --key '{"id": {"N": "1"}}'

   # Update a user
   aws dynamodb update-item \
     --table-name users \
     --key '{"id": {"N": "1"}}' \
     --update-expression "SET email = :email, updated_at = :updated_at" \
     --expression-attribute-values '{
       ":email": {"S": "newemail@example.com"},
       ":updated_at": {"N": "1234567891"}
     }'

   # Delete a user
   aws dynamodb delete-item \
     --table-name users \
     --key '{"id": {"N": "1"}}'

   # List all users (scan)
   aws dynamodb scan \
     --table-name users
   ```

5. EFS File System and Access Point:
   ```bash
   # Create EFS file system
   aws efs create-file-system \
     --performance-mode generalPurpose \
     --throughput-mode bursting \
     --encrypted \
     --tags Key=Name,Value=relay-go-redis

   # Create mount targets in your VPC subnets
   aws efs create-mount-target \
     --file-system-id fs-xxxxx \
     --subnet-id subnet-xxxxx \
     --security-groups sg-xxxxx

   # Create access point for Redis
   aws efs create-access-point \
     --file-system-id fs-xxxxx \
     --posix-user Uid=999,Gid=999 \
     --root-directory "Path=/redis-data,CreationInfo={OwnerUid=999,OwnerGid=999,Permissions=755}"
   ```

## Deployment Steps

1. Build and push the Docker image:
   ```bash
   # Build the image
   docker build -t relay-go:latest .

   # Login to ECR
   aws ecr get-login-password --region us-east-2 | docker login --username AWS --password-stdin 257394459269.dkr.ecr.us-east-2.amazonaws.com

   # Tag the image
   docker tag relay-go:latest 257394459269.dkr.ecr.us-east-2.amazonaws.com/sh-relay-go/sh-relay-go-ingest:latest

   # Push the image
   docker push 257394459269.dkr.ecr.us-east-2.amazonaws.com/sh-relay-go/sh-relay-go-ingest:latest
   ```

2. Update the task definition with your EFS and DynamoDB details:
   - Replace `fs-xxxxx` with your EFS file system ID
   - Replace `fsap-xxxxx` with your EFS access point ID
   - Update DynamoDB environment variables with your table name and region

3. Register the task definition:
   ```bash
   aws ecs register-task-definition --cli-input-json file://task-definition.json
   ```

4. Create or update the ECS service:
   ```bash
   # Create a new service
   aws ecs create-service \
     --cluster your-cluster-name \
     --service-name relay-go-service \
     --task-definition relay-go \
     --desired-count 1 \
     --launch-type FARGATE \
     --network-configuration "awsvpcConfiguration={subnets=[subnet-xxxxx],securityGroups=[sg-xxxxx],assignPublicIp=ENABLED}" \
     --load-balancers "targetGroupArn=arn:aws:elasticloadbalancing:us-east-2:257394459269:targetgroup/relay-go-tg/xxxxx,containerName=relay-go,containerPort=8080"

   # Or update an existing service
   aws ecs update-service \
     --cluster your-cluster-name \
     --service relay-go-service \
     --task-definition relay-go:1
   ```

## Environment Variables

The task definition includes the following environment variables:
- `HTTP_SERVER_PORT`: Port for the HTTP server (8080)
- `REDIS_HOST`: Redis host (localhost)
- `REDIS_PASSWORD`: Redis password (change this in production)
- `DYNAMODB_TABLE_NAME`: Name of the DynamoDB table (users)
- `AWS_REGION`: AWS region for DynamoDB (us-east-2)

## Security Considerations

1. Change the Redis password in production
2. Use AWS Secrets Manager for sensitive values
3. Ensure proper IAM roles and security groups are configured
4. Enable encryption at rest for Redis data
5. Configure EFS security groups to allow access only from ECS tasks
6. Use IAM roles for EFS access
7. Configure DynamoDB table encryption
8. Set up appropriate IAM permissions for DynamoDB access

## Monitoring

The task definition is configured to send logs to CloudWatch:
- Application logs: `/ecs/relay-go/ecs`
- Redis logs: `/ecs/relay-go/redis`

## Troubleshooting

1. Check CloudWatch logs for container issues
2. Verify network connectivity between containers
3. Ensure security groups allow necessary traffic
4. Check ECS task status and events
5. Verify EFS mount points and permissions
6. Check Redis persistence configuration
7. Verify DynamoDB table access and permissions
8. Check IAM role permissions for DynamoDB

## Cleanup

To remove the deployed resources:
```bash
# Delete the service
aws ecs delete-service --cluster your-cluster-name --service relay-go-service --force

# Deregister the task definition
aws ecs deregister-task-definition --task-definition relay-go:1

# Delete the log group
aws logs delete-log-group --log-group-name /ecs/relay-go

# Delete EFS resources
aws efs delete-access-point --access-point-id fsap-xxxxx
aws efs delete-file-system --file-system-id fs-xxxxx

# Delete DynamoDB table
aws dynamodb delete-table --table-name users
``` 