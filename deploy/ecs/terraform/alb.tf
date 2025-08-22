# ACM Certificate
resource "aws_acm_certificate" "main" {
  domain_name       = "soazcloud.com"
  validation_method = "DNS"

  subject_alternative_names = ["*.soazcloud.com"]

  lifecycle {
    create_before_destroy = true
  }

  tags = {
    Name        = "${var.environment}-relay-go-cert"
    Environment = var.environment
  }
}

# Certificate validation
resource "aws_acm_certificate_validation" "main" {
  certificate_arn         = aws_acm_certificate.main.arn
  validation_record_fqdns = [for record in aws_acm_certificate.main.domain_validation_options : record.resource_record_name]
}

# Application Load Balancer
resource "aws_lb" "main" {
  name               = "${var.environment}-relay-go-alb"
  internal           = false
  load_balancer_type = "application"
  security_groups    = [aws_security_group.alb.id]
  subnets            = module.vpc.public_subnets

  enable_deletion_protection = false

  tags = {
    Name        = "${var.environment}-relay-go-alb"
    Environment = var.environment
  }
}

# HTTP Listener (Redirect to HTTPS)
resource "aws_lb_listener" "http" {
  load_balancer_arn = aws_lb.main.arn
  port              = "80"
  protocol          = "HTTP"

  default_action {
    type = "redirect"
    redirect {
      port        = "443"
      protocol    = "HTTPS"
      status_code = "HTTP_301"
    }
  }
}

# HTTPS Listener
resource "aws_lb_listener" "https" {
  load_balancer_arn = aws_lb.main.arn
  port              = "443"
  protocol          = "HTTPS"
  ssl_policy        = "ELBSecurityPolicy-2016-08"
  certificate_arn   = aws_acm_certificate.main.arn

  default_action {
    type = "fixed-response"
    fixed_response {
      content_type = "text/plain"
      message_body = "I'm a little teapot"
      status_code  = "418"
    }
  }
}

resource "aws_lb_listener_rule" "analytics_service" {
  listener_arn = aws_lb_listener.https.arn
  priority     = 10

  action {
    type = "forward"
    
    forward {
      target_group {
        arn = "arn:aws:elasticloadbalancing:us-east-2:998623545110:targetgroup/relay-go-analytics-tg/43bf3dee415fe3da"
      }
      
      stickiness {
        enabled  = true
        duration = 3600
      }
    }
  }

  condition {
    host_header {
      values = ["app.soazcloud.com", "app.deliveriq.net"]
    }
  }

  tags = {
    Component   = "analytics-dashboard"
    Environment = "production"
    ManagedBy   = "terraform"
    Project     = "relay-go-analytics"
  }
}

# HTTPS Listener Rule for ECS Service
resource "aws_lb_listener_rule" "ecs_service" {
  listener_arn = aws_lb_listener.https.arn
  priority     = 100

  action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.main.arn
  }

  condition {
    host_header {
      values = ["soazcloud.com", "*.soazcloud.com","ingest.deliveriq.net"]
    }
  }
}

resource "aws_lb_target_group" "main" {
  name        = "${var.environment}-relay-go-tg"
  port        = 8888
  protocol    = "HTTP"
  vpc_id      = module.vpc.vpc_id
  target_type = "ip"

  health_check {
    enabled             = true
    healthy_threshold   = 2
    interval            = 30
    matcher            = "200"
    path               = "/healthcheck"
    port               = "traffic-port"
    protocol           = "HTTP"
    timeout            = 5
    unhealthy_threshold = 2
  }

  tags = {
    Name        = "${var.environment}-relay-go-tg"
    Environment = var.environment
  }
} 