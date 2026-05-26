# modules/synthetic-monitor/main.tf
# Synthetic monitor — Phase 1 (checker + S3 status only).
#
# A Lambda runs every 5 minutes, hits every monitored target (static sites
# and routing APIs), and writes the results as a single status.json to S3.
# Phase 2 adds a public-facing dashboard that reads this file; Phase 3
# adds CloudWatch metrics and alarms.

resource "random_string" "status_bucket_suffix" {
  length  = 6
  lower   = true
  upper   = false
  numeric = true
  special = false
}

# -----------------------------------------------------------------------------
# S3 — status bucket
#
# Holds a single status.json updated by each Lambda run. In phase 2, the
# dashboard distribution will read from this bucket.
# -----------------------------------------------------------------------------

resource "aws_s3_bucket" "status" {
  bucket = "${var.basename}-${var.environment}-synthetic-status-${random_string.status_bucket_suffix.result}"
}

resource "aws_s3_bucket_public_access_block" "status" {
  bucket                  = aws_s3_bucket.status.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# -----------------------------------------------------------------------------
# IAM — synthetic monitor Lambda execution role
#
# Permissions:
#   - logs:* on the Lambda's own log group
#   - s3:PutObject on the status bucket (for status.json)
#   - ssm:GetParameter on routing API keys, synthetic fixtures, and the
#     non-prod basic auth credentials (read at cold start)
# -----------------------------------------------------------------------------

resource "aws_iam_role" "synthetic_monitor" {
  name = "${var.basename}-${var.environment}-synthetic-monitor"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "synthetic_monitor" {
  name = "${var.basename}-${var.environment}-synthetic-monitor"
  role = aws_iam_role.synthetic_monitor.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "Logs"
        Effect = "Allow"
        Action = [
          "logs:CreateLogStream",
          "logs:PutLogEvents",
        ]
        Resource = "${aws_cloudwatch_log_group.synthetic_monitor.arn}:*"
      },
      {
        Sid      = "WriteStatusJson"
        Effect   = "Allow"
        Action   = "s3:PutObject"
        Resource = "${aws_s3_bucket.status.arn}/*"
      },
      {
        Sid    = "ReadSsm"
        Effect = "Allow"
        Action = ["ssm:GetParameter", "ssm:GetParameters", "ssm:GetParametersByPath"]
        Resource = [
          "arn:aws:ssm:*:*:parameter/${var.basename}/${var.environment}/*/bike-map/routing-api-key",
          "arn:aws:ssm:*:*:parameter/${var.basename}/${var.environment}/*/synthetic/fixture",
          "arn:aws:ssm:*:*:parameter/${var.basename}/${var.environment}/non-prod-auth/credentials",
        ]
      },
    ]
  })
}

resource "aws_cloudwatch_log_group" "synthetic_monitor" {
  name              = "/aws/lambda/${var.basename}-${var.environment}-synthetic-monitor"
  retention_in_days = 14
}

# -----------------------------------------------------------------------------
# Lambda — synthetic monitor
#
# Reads its target list from env vars (set at deploy time from var.cities).
# For each target, makes an HTTP check and records the result. Writes
# a single status.json to S3 with all results.
# -----------------------------------------------------------------------------

data "archive_file" "synthetic_monitor" {
  type        = "zip"
  source_file = "${path.module}/lambda/synthetic_check.py"
  output_path = "${path.module}/synthetic_check.zip"
}

resource "aws_lambda_function" "synthetic_monitor" {
  function_name    = "${var.basename}-${var.environment}-synthetic-monitor"
  role             = aws_iam_role.synthetic_monitor.arn
  filename         = data.archive_file.synthetic_monitor.output_path
  source_code_hash = data.archive_file.synthetic_monitor.output_base64sha256
  runtime          = "python3.12"
  handler          = "synthetic_check.lambda_handler"
  timeout          = 60
  memory_size      = 256

  environment {
    variables = {
      ENVIRONMENT         = var.environment
      STATUS_BUCKET       = aws_s3_bucket.status.bucket
      STATUS_KEY          = "status.json"
      STATIC_SITE_TARGETS = jsonencode(local.static_site_targets)
      ROUTING_API_TARGETS = jsonencode(local.routing_api_targets)
      BASIC_AUTH_SSM_PATH = var.lockdown_non_prod ? "/${var.basename}/${var.environment}/non-prod-auth/credentials" : ""
    }
  }

  depends_on = [
    aws_cloudwatch_log_group.synthetic_monitor,
    aws_iam_role_policy.synthetic_monitor,
  ]
}

locals {
  # Static-site targets: the landing page plus one per city.
  static_site_targets = concat(
    [{
      name = "landing"
      url  = "https://${var.zone_name}"
    }],
    [for city in var.cities : {
      name = "${city}-site"
      url  = "https://${city}.${var.zone_name}"
    }]
  )

  # Routing-API targets: one per city, with SSM paths for the API key and
  # the synthetic fixture. The Lambda skips any city whose fixture SSM
  # parameter doesn't exist (city hasn't had its fixture set yet).
  routing_api_targets = [for city in var.cities : {
    name             = "${city}-routing-api"
    url              = "https://routing-api.${city}.${var.zone_name}/route"
    api_key_ssm_path = "/${var.basename}/${var.environment}/${city}/bike-map/routing-api-key"
    fixture_ssm_path = "/${var.basename}/${var.environment}/${city}/synthetic/fixture"
  }]
}

# -----------------------------------------------------------------------------
# EventBridge Scheduler — invoke the Lambda every 5 minutes
# -----------------------------------------------------------------------------

resource "aws_scheduler_schedule" "synthetic_monitor" {
  name       = "${var.basename}-${var.environment}-synthetic-monitor"
  group_name = "default"

  flexible_time_window {
    mode = "OFF"
  }

  schedule_expression = "rate(5 minutes)"

  target {
    arn      = aws_lambda_function.synthetic_monitor.arn
    role_arn = aws_iam_role.scheduler.arn
  }
}

resource "aws_iam_role" "scheduler" {
  name = "${var.basename}-${var.environment}-synthetic-monitor-scheduler"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "scheduler.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "scheduler" {
  name = "${var.basename}-${var.environment}-synthetic-monitor-scheduler"
  role = aws_iam_role.scheduler.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = "lambda:InvokeFunction"
      Resource = aws_lambda_function.synthetic_monitor.arn
    }]
  })
}


# -----------------------------------------------------------------------------
# CloudWatch alarm — synthetic Lambda errors
#
# Fires if the synthetic monitor Lambda itself starts erroring.
# -----------------------------------------------------------------------------

resource "aws_cloudwatch_metric_alarm" "synthetic_lambda_errors" {
  alarm_name          = "${var.basename}-${var.environment}-synthetic-monitor-errors"
  alarm_description   = "Synthetic monitor Lambda is erroring. Per-target alarms may be silent during this period."
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 3
  metric_name         = "Errors"
  namespace           = "AWS/Lambda"
  period              = 300
  statistic           = "Sum"
  threshold           = 0
  treat_missing_data  = "notBreaching"

  dimensions = {
    FunctionName = aws_lambda_function.synthetic_monitor.function_name
  }

  alarm_actions = [var.alarm_sns_topic_arn]
  ok_actions    = [var.alarm_sns_topic_arn]
}

# -----------------------------------------------------------------------------
# Phase 2: status dashboard
#
# Dashboard HTML lives in a new bucket. CloudFront serves both the
# dashboard files and the status.json from the Phase 1 status bucket,
# routed by path. The dashboard URL is status.<env>.bikeinfra.com.
# -----------------------------------------------------------------------------

resource "random_string" "dashboard_bucket_suffix" {
  length  = 6
  lower   = true
  upper   = false
  numeric = true
  special = false
}

resource "aws_s3_bucket" "dashboard" {
  bucket = "${var.basename}-${var.environment}-synthetic-dashboard-${random_string.dashboard_bucket_suffix.result}"
}

resource "aws_s3_bucket_public_access_block" "dashboard" {
  bucket                  = aws_s3_bucket.dashboard.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_object" "dashboard_index" {
  bucket       = aws_s3_bucket.dashboard.id
  key          = "index.html"
  content      = file("${path.module}/static/index.html")
  content_type = "text/html"
  etag         = filemd5("${path.module}/static/index.html")
}

# ACM certificate for the dashboard domain (us-east-1 for CloudFront)
resource "aws_acm_certificate" "dashboard" {
  provider          = aws.us_east_1
  domain_name       = "status.${var.zone_name}"
  validation_method = "DNS"
  lifecycle { create_before_destroy = true }
}

resource "aws_route53_record" "dashboard_cert_validation" {
  provider = aws.dns
  for_each = {
    for dvo in aws_acm_certificate.dashboard.domain_validation_options : dvo.domain_name => {
      name   = dvo.resource_record_name
      type   = dvo.resource_record_type
      record = dvo.resource_record_value
    }
  }
  zone_id = var.zone_id
  name    = each.value.name
  type    = each.value.type
  ttl     = 300
  records = [each.value.record]
}

resource "aws_acm_certificate_validation" "dashboard" {
  provider                = aws.us_east_1
  certificate_arn         = aws_acm_certificate.dashboard.arn
  validation_record_fqdns = [for r in aws_route53_record.dashboard_cert_validation : r.fqdn]
}

# CloudFront Origin Access Control — shared by both origins
resource "aws_cloudfront_origin_access_control" "dashboard" {
  name                              = "${var.basename}-${var.environment}-synthetic-dashboard"
  origin_access_control_origin_type = "s3"
  signing_behavior                  = "always"
  signing_protocol                  = "sigv4"
}

# Two origins, one distribution. Default behavior serves index.html;
# the /status.json path goes to the Phase 1 status bucket.
resource "aws_cloudfront_distribution" "dashboard" {
  enabled             = true
  default_root_object = "index.html"
  aliases             = ["status.${var.zone_name}"]

  origin {
    domain_name              = aws_s3_bucket.dashboard.bucket_regional_domain_name
    origin_id                = "dashboard-s3"
    origin_access_control_id = aws_cloudfront_origin_access_control.dashboard.id
  }

  origin {
    domain_name              = aws_s3_bucket.status.bucket_regional_domain_name
    origin_id                = "status-s3"
    origin_access_control_id = aws_cloudfront_origin_access_control.dashboard.id
  }

  default_cache_behavior {
    target_origin_id       = "dashboard-s3"
    viewer_protocol_policy = "redirect-to-https"
    allowed_methods        = ["GET", "HEAD"]
    cached_methods         = ["GET", "HEAD"]
    forwarded_values {
      query_string = false
      cookies { forward = "none" }
    }

    response_headers_policy_id = var.response_headers_policy_id

    dynamic "function_association" {
      for_each = var.basic_auth_function_arn != null ? [1] : []
      content {
        event_type   = "viewer-request"
        function_arn = var.basic_auth_function_arn
      }
    }
  }

  # /status.json — read from the Phase 1 status bucket, with very short
  # caching so dashboard refreshes see new data quickly.
  ordered_cache_behavior {
    path_pattern           = "/status.json"
    target_origin_id       = "status-s3"
    viewer_protocol_policy = "redirect-to-https"
    allowed_methods        = ["GET", "HEAD"]
    cached_methods         = ["GET", "HEAD"]
    min_ttl                = 0
    default_ttl            = 30
    max_ttl                = 60

    forwarded_values {
      query_string = false
      cookies { forward = "none" }
    }

    response_headers_policy_id = var.response_headers_policy_id

    dynamic "function_association" {
      for_each = var.basic_auth_function_arn != null ? [1] : []
      content {
        event_type   = "viewer-request"
        function_arn = var.basic_auth_function_arn
      }
    }
  }

  restrictions {
    geo_restriction { restriction_type = "none" }
  }

  viewer_certificate {
    acm_certificate_arn      = aws_acm_certificate_validation.dashboard.certificate_arn
    ssl_support_method       = "sni-only"
    minimum_protocol_version = "TLSv1.2_2021"
  }
}

# Bucket policies — grant CloudFront read access to both buckets
resource "aws_s3_bucket_policy" "dashboard" {
  bucket = aws_s3_bucket.dashboard.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "AllowCloudFrontRead"
      Effect    = "Allow"
      Principal = { Service = "cloudfront.amazonaws.com" }
      Action    = "s3:GetObject"
      Resource  = "${aws_s3_bucket.dashboard.arn}/*"
      Condition = {
        StringEquals = {
          "AWS:SourceArn" = aws_cloudfront_distribution.dashboard.arn
        }
      }
    }]
  })
}

resource "aws_s3_bucket_policy" "status" {
  bucket = aws_s3_bucket.status.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "AllowCloudFrontRead"
      Effect    = "Allow"
      Principal = { Service = "cloudfront.amazonaws.com" }
      Action    = "s3:GetObject"
      Resource  = "${aws_s3_bucket.status.arn}/*"
      Condition = {
        StringEquals = {
          "AWS:SourceArn" = aws_cloudfront_distribution.dashboard.arn
        }
      }
    }]
  })
}

# Route53 A record
resource "aws_route53_record" "dashboard" {
  provider = aws.dns
  zone_id  = var.zone_id
  name     = "status.${var.zone_name}"
  type     = "A"
  alias {
    name                   = aws_cloudfront_distribution.dashboard.domain_name
    zone_id                = aws_cloudfront_distribution.dashboard.hosted_zone_id
    evaluate_target_health = false
  }
}

