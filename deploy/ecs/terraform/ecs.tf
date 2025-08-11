# Get current AWS account ID
data "aws_caller_identity" "current" {}

# ECS Cluster
resource "aws_ecs_cluster" "main" {
  name = "${var.environment}-relay-go-cluster"

  tags = {
    Name        = "${var.environment}-relay-go-cluster"
    Environment = var.environment
  }
}

# CloudWatch Log Groups
resource "aws_cloudwatch_log_group" "app" {
  name              = "/ecs/relay-go"
  retention_in_days = 30

  tags = {
    Name        = "${var.environment}-relay-go-logs"
    Environment = var.environment
  }
}

resource "aws_cloudwatch_log_group" "redis" {
  name              = "/ecs/relay-go-redis"
  retention_in_days = 30

  tags = {
    Name        = "${var.environment}-relay-go-redis-logs"
    Environment = var.environment
  }
}



# ECS Task Execution Role
resource "aws_iam_role" "ecs_execution_role" {
  name = "${var.environment}-relay-go-execution-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "ecs-tasks.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "ecs_execution_role_policy" {
  role       = aws_iam_role.ecs_execution_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

# Secrets Manager access policy
resource "aws_iam_role_policy" "secrets_access" {
  name = "${var.environment}-relay-go-secrets-access"
  role = aws_iam_role.ecs_execution_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "secretsmanager:GetSecretValue"
        ]
        Resource = [
          aws_secretsmanager_secret.app_secrets.arn
        ]
      }
    ]
  })
}

# ECR access policy for execution role
resource "aws_iam_role_policy" "ecr_access" {
  name = "${var.environment}-relay-go-ecr-access"
  role = aws_iam_role.ecs_execution_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "ecr:GetAuthorizationToken",
          "ecr:BatchCheckLayerAvailability",
          "ecr:GetDownloadUrlForLayer",
          "ecr:BatchGetImage"
        ]
        Resource = "*"
      }
    ]
  })
}

# ECS Task Role
resource "aws_iam_role" "ecs_task_role" {
  name = "${var.environment}-relay-go-task-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "ecs-tasks.amazonaws.com"
        }
      }
    ]
  })
}

# DynamoDB access policy
resource "aws_iam_role_policy" "dynamodb_access" {
  name = "${var.environment}-relay-go-dynamodb-access"
  role = aws_iam_role.ecs_task_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "dynamodb:GetItem",
          "dynamodb:PutItem",
          "dynamodb:UpdateItem",
          "dynamodb:DeleteItem",
          "dynamodb:Scan",
          "dynamodb:Query",
          "dynamodb:ListTables",
          "dynamodb:DescribeTable"
        ]
        Resource = [
          aws_dynamodb_table.users.arn,
          "arn:aws:dynamodb:${var.aws_region}:${data.aws_caller_identity.current.account_id}:table/*"
        ]
      }
    ]
  })
}

# S3 access policy
resource "aws_iam_role_policy" "s3_access" {
  name = "${var.environment}-relay-go-s3-access"
  role = aws_iam_role.ecs_task_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:GetObject",
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.event_batches.arn,
          "${aws_s3_bucket.event_batches.arn}/*"
        ]
      }
    ]
  })
}

# ECS Task Definition
resource "aws_ecs_task_definition" "main" {
  family                   = "${var.environment}-relay-go"
  network_mode            = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                     = 512   # Original value before Kafka
  memory                  = 1024  # Original value before Kafka
  execution_role_arn      = aws_iam_role.ecs_execution_role.arn
  task_role_arn           = aws_iam_role.ecs_task_role.arn

  volume {
    name = "redis-data"
    efs_volume_configuration {
      file_system_id     = aws_efs_file_system.redis.id
      root_directory     = "/"
      transit_encryption = "ENABLED"
      authorization_config {
        access_point_id = aws_efs_access_point.redis.id
        iam             = "ENABLED"
      }
    }
  }



  container_definitions = jsonencode([
    {
      name      = "relay-go"
      image     = "${var.ecr_repository_url}:latest"
      essential = true
      dependsOn = [
        {
          containerName = "redis"
          condition     = "START"
        }
      ]
      portMappings = [
        {
          containerPort = 8888
          hostPort      = 8888
          protocol      = "tcp"
        }
      ]
      environment = [
        {
          name  = "HTTP_SERVER_PORT"
          value = "8888"
        },
        {
          name  = "REDIS_HOST"
          value = "localhost:6379"
        },
        {
          name  = "DYNAMODB_TABLE_NAME"
          value = var.dynamodb_table_name
        },
        {
          name  = "AWS_REGION"
          value = var.aws_region
        },
        {
          name  = "S3_BUCKET_NAME"
          value = aws_s3_bucket.event_batches.id
        },
        {
          name  = "LOG_LEVEL"
          value = "ERROR"
        },
        {
          name  = "KAFKA_BROKERS"
          value = "localhost:9092"
        },
        {
          name  = "EMAIL_TOPIC"
          value = "emails"
        },
        {
          name  = "WEBHOOK_TOPIC_SENDGRID"
          value = "webhook-events-sendgrid"
        },
        {
          name  = "WEBHOOK_TOPIC_POSTMARK"
          value = "webhook-events-postmark"
        },
        {
          name  = "WEBHOOK_TOPIC_SOCKETLABS"
          value = "webhook-events-socketlabs"
        },
        {
          name  = "WEBHOOK_TOPIC_SPARKPOST"
          value = "webhook-events-sparkpost"
        },
        {
          name  = "WEBHOOK_TOPIC_MANDRILL"
          value = "webhook-events-mandrill"
        },
        {
          name  = "MYSQL_HOST"
          value = "placeholder"
        },
        {
          name  = "MYSQL_PORT"
          value = "3306"
        },
        {
          name  = "MYSQL_USER"
          value = "placeholder"
        },
        {
          name  = "MYSQL_PASSWORD"
          value = "placeholder"
        },
        {
          name  = "MYSQL_DB"
          value = "placeholder"
        },
        {
          name  = "MYSQL_SSL_MODE"
          value = "disable"
        },
        {
          name  = "SPLUNK_ENABLED"
          value = "false"
        }
      ]
      secrets = [
        {
          name      = "SPLUNK_HOST"
          valueFrom = "${aws_secretsmanager_secret.app_secrets.arn}:splunk_host::"
        },
        {
          name      = "SPLUNK_KEY"
          valueFrom = "${aws_secretsmanager_secret.app_secrets.arn}:splunk_key::"
        }
      ]
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group"         = "/ecs/relay-go"
          "awslogs-region"        = var.aws_region
          "awslogs-stream-prefix" = "ecs"
        }
      }
    },
    {
      name      = "redis"
      image     = "redis:7.2-alpine"
      essential = true
      portMappings = [
        {
          containerPort = 6379
          hostPort      = 6379
          protocol      = "tcp"
        }
      ]
      mountPoints = [
        {
          sourceVolume  = "redis-data"
          containerPath = "/data"
          readOnly      = false
        }
      ]
      command = [
        "redis-server",
        "--save", "20", "1",
        "--loglevel", "warning",
        "--dir", "/data",
        "--dbfilename", "dump.rdb",
        "--bind", "0.0.0.0"
      ]
      healthCheck = {
        command     = ["CMD", "redis-cli", "ping"]
        interval    = 30
        timeout     = 5
        retries     = 3
        startPeriod = 60
      }
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group"         = "/ecs/relay-go-redis"
          "awslogs-region"        = var.aws_region
          "awslogs-stream-prefix" = "redis"
        }
      }
    }
  ])

  tags = {
    Name        = "${var.environment}-relay-go-task"
    Environment = var.environment
  }
}

# ECS Service
resource "aws_ecs_service" "main" {
  name            = "${var.environment}-relay-go-service"
  cluster         = aws_ecs_cluster.main.id
  task_definition = aws_ecs_task_definition.main.arn
  desired_count   = 1
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = module.vpc.private_subnets
    security_groups  = [aws_security_group.ecs_tasks.id]
    assign_public_ip = false
  }

  load_balancer {
    target_group_arn = aws_lb_target_group.main.arn
    container_name   = "relay-go"
    container_port   = 8888
  }

  tags = {
    Name        = "${var.environment}-relay-go-service"
    Environment = var.environment
  }
}

