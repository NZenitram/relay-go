# DynamoDB Table
resource "aws_dynamodb_table" "users" {
  name         = var.dynamodb_table_name
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "id"
deletion_protection_enabled = true
  attribute {
    name = "id"
    type = "N"
  }

  tags = {
    Name        = var.dynamodb_table_name
    Environment = var.environment
  }
} 