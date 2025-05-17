# Secrets Manager
resource "aws_secretsmanager_secret" "app_secrets" {
  name = "${var.environment}-relay-go-secrets"
  description = "Secrets for relay-go application"

  tags = {
    Name        = "${var.environment}-relay-go-secrets"
    Environment = var.environment
  }
}

resource "aws_secretsmanager_secret_version" "app_secrets" {
  secret_id = aws_secretsmanager_secret.app_secrets.id
  secret_string = jsonencode({
    redis_password = var.redis_password
    splunk_host    = "prd-p-yvj3g"
    splunk_key     = "9e937222-ad2b-4081-918b-df9e539ccfca"
    # Add other sensitive values here
  })
} 