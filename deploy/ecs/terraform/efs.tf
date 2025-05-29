# EFS File System
resource "aws_efs_file_system" "redis" {
  creation_token = "${var.environment}-relay-go-redis"
  encrypted      = true

  performance_mode = "generalPurpose"
  throughput_mode  = "bursting"

  lifecycle_policy {
    transition_to_ia = "AFTER_30_DAYS"
  }

  tags = {
    Name        = "${var.environment}-relay-go-redis"
    Environment = var.environment
  }
}

# EFS Mount Targets in all private subnets
resource "aws_efs_mount_target" "redis" {
  count           = length(module.vpc.private_subnets)
  file_system_id  = aws_efs_file_system.redis.id
  subnet_id       = module.vpc.private_subnets[count.index]
  security_groups = [aws_security_group.efs.id]
}

# EFS Security Group
resource "aws_security_group" "efs" {
  name        = "${var.environment}-relay-go-efs-sg"
  description = "Security group for EFS mount targets"
  vpc_id      = module.vpc.vpc_id

  ingress {
    description     = "NFS from ECS tasks"
    from_port       = 2049
    to_port         = 2049
    protocol        = "tcp"
    security_groups = [aws_security_group.ecs_tasks.id]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name        = "${var.environment}-relay-go-efs-sg"
    Environment = var.environment
  }
}

# EFS Access Point for Redis
resource "aws_efs_access_point" "redis" {
  file_system_id = aws_efs_file_system.redis.id

  root_directory {
    path = "/redis"
    creation_info {
      owner_gid   = 999
      owner_uid   = 999
      permissions = "755"
    }
  }

  posix_user {
    gid = 999
    uid = 999
  }

    tags = {
    Name        = "${var.environment}-relay-go-redis-ap"
    Environment = var.environment
  }
}

