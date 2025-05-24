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

# Counter table for auto-incrementing IDs
resource "aws_dynamodb_table" "id_counter" {
  name         = "${var.environment}-id-counter"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "counter_name"
  
  attribute {
    name = "counter_name"
    type = "S"
  }

  tags = {
    Name        = "${var.environment}-id-counter"
    Environment = var.environment
  }
}

# Initialize the counter
resource "aws_dynamodb_table_item" "user_id_counter" {
  table_name = aws_dynamodb_table.id_counter.name
  hash_key   = aws_dynamodb_table.id_counter.hash_key

  item = jsonencode({
    counter_name = { S = "user_id" }
    current_value = { N = "0" }
  })
} 