# ECS Task Definition Deployment

This directory contains the ECS task definition for deploying the relay-go application with Redis and DynamoDB.

## Prerequisites

1. AWS CLI installed and configured
2. Appropriate AWS permissions
3. ECR repository for the application image
4. CloudWatch log group created
5. EFS file system for Redis data persistence
6. DynamoDB table for user data
7. VPC with public and private subnets
8. Security groups for ALB and ECS tasks

## Required AWS Resources

### 1. VPC and Network Setup
```bash
# Create VPC
aws ec2 create-vpc \
  --cidr-block 10.0.0.0/16 \
  --tag-specifications 'ResourceType=vpc,Tags=[{Key=Name,Value=relay-go-vpc}]'

# Create public subnets
aws ec2 create-subnet \
  --vpc-id vpc-xxxxx \
  --cidr-block 10.0.1.0/24 \
  --availability-zone us-east-2a \
  --tag-specifications 'ResourceType=subnet,Tags=[{Key=Name,Value=relay-go-public-1}]'

aws ec2 create-subnet \
  --vpc-id vpc-xxxxx \
  --cidr-block 10.0.2.0/24 \
  --availability-zone us-east-2b \
  --tag-specifications 'ResourceType=subnet,Tags=[{Key=Name,Value=relay-go-public-2}]'

# Create private subnets
aws ec2 create-subnet \
  --vpc-id vpc-xxxxx \
  --cidr-block 10.0.3.0/24 \
  --availability-zone us-east-2a \
  --tag-specifications 'ResourceType=subnet,Tags=[{Key=Name,Value=relay-go-private-1}]'

aws ec2 create-subnet \
  --vpc-id vpc-xxxxx \
  --cidr-block 10.0.4.0/24 \
  --availability-zone us-east-2b \
  --tag-specifications 'ResourceType=subnet,Tags=[{Key=Name,Value=relay-go-private-2}]'

# Create Internet Gateway
aws ec2 create-internet-gateway \
  --tag-specifications 'ResourceType=internet-gateway,Tags=[{Key=Name,Value=relay-go-igw}]'

# Attach Internet Gateway to VPC
aws ec2 attach-internet-gateway \
  --vpc-id vpc-xxxxx \
  --internet-gateway-id igw-xxxxx

# Create NAT Gateway
aws ec2 create-nat-gateway \
  --subnet-id subnet-xxxxx \
  --allocation-id eipalloc-xxxxx \
  --tag-specifications 'ResourceType=natgateway,Tags=[{Key=Name,Value=relay-go-nat}]'
```

### 2. Security Groups
```bash
# Create ALB Security Group
aws ec2 create-security-group \
  --group-name relay-go-alb-sg \
  --description "Security group for relay-go ALB" \
  --vpc-id vpc-xxxxx

# Allow inbound HTTP/HTTPS
aws ec2 authorize-security-group-ingress \
  --group-id sg-xxxxx \
  --protocol tcp \
  --port 80 \
  --cidr 0.0.0.0/0

aws ec2 authorize-security-group-ingress \
  --group-id sg-xxxxx \
  --protocol tcp \
  --port 443 \
  --cidr 0.0.0.0/0

# Create ECS Task Security Group
aws ec2 create-security-group \
  --group-name relay-go-ecs-sg \
  --description "Security group for relay-go ECS tasks" \
  --vpc-id vpc-xxxxx

# Allow inbound from ALB
aws ec2 authorize-security-group-ingress \
  --group-id sg-xxxxx \
  --protocol tcp \
  --port 8080 \
  --source-group sg-xxxxx
```

### 3. Application Load Balancer
```bash
# Create ALB
aws elbv2 create-load-balancer \
  --name relay-go-alb \
  --subnets subnet-xxxxx subnet-yyyyy \
  --security-groups sg-xxxxx \
  --scheme internet-facing

# Create target group
aws elbv2 create-target-group \
  --name relay-go-tg \
  --protocol HTTP \
  --port 8080 \
  --vpc-id vpc-xxxxx \
  --target-type ip \
  --health-check-path /healthcheck \
  --health-check-interval-seconds 30 \
  --health-check-timeout-seconds 5 \
  --healthy-threshold-count 2 \
  --unhealthy-threshold-count 2

# Create listener
aws elbv2 create-listener \
  --load-balancer-arn arn:aws:elasticloadbalancing:us-east-2:123456789012:loadbalancer/app/relay-go-alb/xxxxx \
  --protocol HTTP \
  --port 80 \
  --default-actions Type=forward,TargetGroupArn=arn:aws:elasticloadbalancing:us-east-2:123456789012:targetgroup/relay-go-tg/xxxxx
```

### 4. ECS Cluster
```bash
# Create ECS cluster
aws ecs create-cluster \
  --cluster-name relay-go-cluster \
  --capacity-providers FARGATE \
  --default-capacity-provider-strategy capacityProvider=FARGATE,weight=1 \
  --tags Key=Name,Value=relay-go-cluster
```

### 5. ECR Repository
```bash
aws ecr create-repository --repository-name sh-relay-go/sh-relay-go-ingest
```

### 6. CloudWatch Log Group
```bash
aws logs create-log-group --log-group-name /ecs/relay-go
```

### 7. DynamoDB Table
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

### 8. EFS File System
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

4. Create the ECS service:
```bash
aws ecs create-service \
  --cluster relay-go-cluster \
  --service-name relay-go-service \
  --task-definition relay-go \
  --desired-count 1 \
  --launch-type FARGATE \
  --network-configuration "awsvpcConfiguration={subnets=[subnet-xxxxx,subnet-yyyyy],securityGroups=[sg-xxxxx],assignPublicIp=DISABLED}" \
  --load-balancers "targetGroupArn=arn:aws:elasticloadbalancing:us-east-2:123456789012:targetgroup/relay-go-tg/xxxxx,containerName=relay-go,containerPort=8080"
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
9. Use HTTPS for the ALB listener in production
10. Implement WAF rules for the ALB
11. Enable VPC Flow Logs for network monitoring

## Monitoring

The task definition is configured to send logs to CloudWatch:
- Application logs: `/ecs/relay-go/ecs`
- Redis logs: `/ecs/relay-go/redis`

Set up CloudWatch Alarms for:
- ALB 5xx errors
- ECS task CPU/Memory utilization
- DynamoDB throttling
- EFS burst credit balance

## Troubleshooting

1. Check CloudWatch logs for container issues
2. Verify network connectivity between containers
3. Ensure security groups allow necessary traffic
4. Check ECS task status and events
5. Verify EFS mount points and permissions
6. Check Redis persistence configuration
7. Verify DynamoDB table access and permissions
8. Check IAM role permissions for DynamoDB
9. Verify ALB target group health checks
10. Check VPC endpoints for AWS services

## Cleanup

To remove the deployed resources:
```bash
# Delete the service
aws ecs delete-service --cluster relay-go-cluster --service relay-go-service --force

# Deregister the task definition
aws ecs deregister-task-definition --task-definition relay-go:1

# Delete the cluster
aws ecs delete-cluster --cluster relay-go-cluster

# Delete the ALB
aws elbv2 delete-load-balancer --load-balancer-arn arn:aws:elasticloadbalancing:us-east-2:123456789012:loadbalancer/app/relay-go-alb/xxxxx

# Delete the target group
aws elbv2 delete-target-group --target-group-arn arn:aws:elasticloadbalancing:us-east-2:123456789012:targetgroup/relay-go-tg/xxxxx

# Delete the log group
aws logs delete-log-group --log-group-name /ecs/relay-go

# Delete the EFS file system
aws efs delete-file-system --file-system-id fs-xxxxx

# Delete the DynamoDB table
aws dynamodb delete-table --table-name users
``` 

aws dynamodb put-item \
    --table-name users \
    --item '{
        "id": {"N": "5"},
        "email": {"S": "info@theshcompany.com"},
        "sendgrid_verification_key": {"S": "MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAEr2ZTHTIgfE02xTr72mfejHh2vLPccnY1HpENM0N0CJ2PA+zEzGtr73Odwpix/R9svSFsySurYIsbZMPs+2CefA=="},
        "created_at": {"N": "1747085130"},
        "updated_at": {"N": "1747085130"}
    }'