terraform {
    backend "s3" {
        bucket         = "relay-go-terraform-statev2"
        key            = "ecs/terraform.tfstate"
        region         = "us-east-2"
        encrypt        = true
        # dynamodb_table = "relay-go-terraform-locks"
    }
    required_providers {
        aws = {
        source  = "hashicorp/aws"
        version = "~> 5.0"
        }
    }
    required_version = ">= 1.2.0"
    }

provider "aws" {
  region = var.aws_region
}
