# S3 Gateway Endpoint - FREE
resource "aws_vpc_endpoint" "s3" {
  vpc_id            = module.vpc.vpc_id
  service_name      = "com.amazonaws.${var.aws_region}.s3"
  vpc_endpoint_type = "Gateway"
  route_table_ids   = module.vpc.private_route_table_ids

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = "*"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:ListBucket",
          "s3:DeleteObject"
        ]
        Resource = [
          aws_s3_bucket.event_batches.arn,
          "${aws_s3_bucket.event_batches.arn}/*"
        ]
      }
    ]
  })

  tags = {
    Name        = "${var.environment}-s3-gateway-endpoint"
    Environment = var.environment
    Service     = "S3"
  }
}

# DynamoDB Gateway Endpoint - FREE
resource "aws_vpc_endpoint" "dynamodb" {
  vpc_id            = module.vpc.vpc_id
  service_name      = "com.amazonaws.${var.aws_region}.dynamodb"
  vpc_endpoint_type = "Gateway"
  route_table_ids   = module.vpc.private_route_table_ids

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = "*"
        Action = [
          "dynamodb:GetItem",
          "dynamodb:PutItem",
          "dynamodb:UpdateItem",
          "dynamodb:DeleteItem",
          "dynamodb:Scan",
          "dynamodb:Query",
          "dynamodb:BatchGetItem",
          "dynamodb:BatchWriteItem"
        ]
        Resource = [
          aws_dynamodb_table.users.arn,
          "${aws_dynamodb_table.users.arn}/index/*"
        ]
      }
    ]
  })

  tags = {
    Name        = "${var.environment}-dynamodb-gateway-endpoint"
    Environment = var.environment
    Service     = "DynamoDB"
  }
}

# Outputs for visibility
output "vpc_endpoints" {
  description = "VPC Endpoints created for the environment"
  value = {
    s3_gateway = {
      id           = aws_vpc_endpoint.s3.id
      service_name = aws_vpc_endpoint.s3.service_name
      type         = "Gateway"
      cost         = "FREE"
    }
    dynamodb_gateway = {
      id           = aws_vpc_endpoint.dynamodb.id
      service_name = aws_vpc_endpoint.dynamodb.service_name
      type         = "Gateway"
      cost         = "FREE"
    }
  }
}

output "vpc_endpoints_summary" {
  description = "Summary of VPC endpoints and their costs"
  value = {
    total_monthly_cost = "$0.00"
    free_endpoints     = "S3 Gateway, DynamoDB Gateway"
    benefits = [
      "S3 and DynamoDB traffic stays on AWS backbone",
      "No NAT Gateway data charges for S3/DynamoDB",
      "Better security and compliance for data storage",
      "Improved performance for S3 uploads and DynamoDB queries"
    ]
    note = "Other AWS services (Secrets Manager, CloudWatch, ECR) will continue using NAT Gateway"
  }
} 