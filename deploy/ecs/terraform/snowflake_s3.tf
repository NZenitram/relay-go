# S3 bucket for event batching
resource "aws_s3_bucket" "event_batches" {
  bucket = "${var.environment}-relay-go-events-${data.aws_caller_identity.current.account_id}"

  tags = {
    Name        = "${var.environment}-relay-go-events"
    Environment = var.environment
  }
}

# Bucket policy for Snowflake access
resource "aws_s3_bucket_policy" "snowflake_access" {
  bucket = aws_s3_bucket.event_batches.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "AllowSnowflakeAccess"
        Effect = "Allow"
        Principal = {
          AWS = "arn:aws:iam::762978521768:root"
        }
        Action = [
          "s3:GetObject",
          "s3:GetObjectVersion",
          "s3:ListBucket",
          "s3:GetBucketLocation"
        ]
        Resource = [
          aws_s3_bucket.event_batches.arn,
          "${aws_s3_bucket.event_batches.arn}/*"
        ]
      }
    ]
  })
}

# Enable versioning for the bucket
resource "aws_s3_bucket_versioning" "event_batches" {
  bucket = aws_s3_bucket.event_batches.id
  versioning_configuration {
    status = "Enabled"
  }
}

# Server-side encryption configuration
resource "aws_s3_bucket_server_side_encryption_configuration" "event_batches" {
  bucket = aws_s3_bucket.event_batches.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# # Lifecycle rules for the bucket
# resource "aws_s3_bucket_lifecycle_configuration" "event_batches" {
#   bucket = aws_s3_bucket.event_batches.id

#   rule {
#     id     = "tiered_storage"
#     status = "Enabled"

#     # Keep data in standard storage for 90 days
#     transition {
#       days          = 90
#       storage_class = "STANDARD_IA"
#     }

#     # Move to Glacier after 180 days
#     transition {
#       days          = 180
#       storage_class = "GLACIER"
#     }

#     # Keep data for 3 years before deletion
#     expiration {
#       days = 1095  # 3 years
#     }

#     # Keep noncurrent versions for 1 year
#     noncurrent_version_expiration {
#       noncurrent_days = 365
#     }
#   }
# }

resource "aws_iam_role" "ecs_task_execution_role" {
  name = "${var.environment}-relay-go-ecs-task-execution-role"

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

  tags = {
    Name        = "${var.environment}-relay-go-ecs-task-execution-role"
    Environment = var.environment
  }
}

# IAM policy for ECS tasks to access the S3 bucket
resource "aws_iam_policy" "s3_event_batches" {
  name        = "${var.environment}-relay-go-s3-events-policy"
  description = "Policy for ECS tasks to access event batches S3 bucket"

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

# Attach the S3 policy to the ECS task role
resource "aws_iam_role_policy_attachment" "ecs_s3_events" {
  role       = aws_iam_role.ecs_task_execution_role.name
  policy_arn = aws_iam_policy.s3_event_batches.arn
}

# Attach the AWS managed policy for ECS task execution
resource "aws_iam_role_policy_attachment" "ecs_task_execution_role_policy" {
  role       = aws_iam_role.ecs_task_execution_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

# IAM role for Snowflake
resource "aws_iam_role" "snowflake_role" {
  name = "${var.environment}-relay-go-snowflake-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          AWS = [
            "arn:aws:iam::762978521768:root",
            "arn:aws:iam::762978521768:user/bs411000-s"
          ]
        }
        Action = "sts:AssumeRole"
        Condition = {
          StringEquals = {
            "sts:ExternalId": "TT05841_SFCRole=4_pVv5Yca0zNI/i5qnueb1Drj+ylY="
          }
        }
      }
    ]
  })

  tags = {
    Name        = "${var.environment}-relay-go-snowflake-role"
    Environment = var.environment
  }
}

# IAM policy for Snowflake to access S3
resource "aws_iam_policy" "snowflake_s3_access" {
  name        = "${var.environment}-relay-go-snowflake-s3-policy"
  description = "Policy for Snowflake to access event batches S3 bucket"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:GetObjectVersion",
          "s3:ListBucket",
          "s3:GetBucketLocation"
        ]
        Resource = [
          aws_s3_bucket.event_batches.arn,
          "${aws_s3_bucket.event_batches.arn}/*"
        ]
      }
    ]
  })
}

# Attach the S3 policy to the Snowflake role
resource "aws_iam_role_policy_attachment" "snowflake_s3_access" {
  role       = aws_iam_role.snowflake_role.name
  policy_arn = aws_iam_policy.snowflake_s3_access.arn
}

# Output the role ARN for Snowflake configuration
output "snowflake_role_arn" {
  value       = aws_iam_role.snowflake_role.arn
  description = "ARN of the IAM role for Snowflake to access S3"
}