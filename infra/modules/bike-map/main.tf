locals {
  resource_name = "${var.basename}-${var.environment}-${var.app_name}"
  domain        = "${var.app_name}.${var.zone_name}"
  api_domain    = "routing-api.${var.zone_name}"
}

# -----------------------------------------------------------------------------
# S3 — static site bucket
# -----------------------------------------------------------------------------

resource "aws_s3_bucket" "site" {
  bucket = local.resource_name
}

resource "aws_s3_bucket_public_access_block" "site" {
  bucket                  = aws_s3_bucket.site.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}


# -----------------------------------------------------------------------------
# S3 — routing graph bucket (Parquet export for Lambda)
# -----------------------------------------------------------------------------

resource "aws_s3_bucket" "routing_graph" {
  bucket = "${local.resource_name}-routing-graph"
}

resource "aws_s3_bucket_public_access_block" "routing_graph" {
  bucket                  = aws_s3_bucket.routing_graph.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}


# -----------------------------------------------------------------------------
# CloudFront
# -----------------------------------------------------------------------------

resource "aws_cloudfront_origin_access_control" "site" {
  name                              = local.resource_name
  origin_access_control_origin_type = "s3"
  signing_behavior                  = "always"
  signing_protocol                  = "sigv4"
}

resource "aws_cloudfront_distribution" "site" {
  enabled             = true
  default_root_object = "index.html"
  aliases             = [local.domain]

  origin {
    domain_name              = aws_s3_bucket.site.bucket_regional_domain_name
    origin_id                = "s3"
    origin_access_control_id = aws_cloudfront_origin_access_control.site.id
  }

  default_cache_behavior {
    target_origin_id       = "s3"
    viewer_protocol_policy = "redirect-to-https"
    allowed_methods        = ["GET", "HEAD"]
    cached_methods         = ["GET", "HEAD"]

    forwarded_values {
      query_string = false
      cookies {
        forward = "none"
      }
    }
  }

  restrictions {
    geo_restriction {
      restriction_type = "none"
    }
  }

  viewer_certificate {
    acm_certificate_arn      = aws_acm_certificate_validation.site.certificate_arn
    ssl_support_method       = "sni-only"
    minimum_protocol_version = "TLSv1.2_2021"
  }
}

# Grant CloudFront read access to the S3 bucket
resource "aws_s3_bucket_policy" "site" {
  bucket = aws_s3_bucket.site.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid       = "AllowCloudFrontRead"
        Effect    = "Allow"
        Principal = { Service = "cloudfront.amazonaws.com" }
        Action    = "s3:GetObject"
        Resource  = "${aws_s3_bucket.site.arn}/*"
        Condition = {
          StringEquals = {
            "AWS:SourceArn" = aws_cloudfront_distribution.site.arn
          }
        }
      }
    ]
  })
}


# -----------------------------------------------------------------------------
# ACM certificate (must be in us-east-1 for CloudFront)
# -----------------------------------------------------------------------------

resource "aws_acm_certificate" "site" {
  provider          = aws.us_east_1
  domain_name       = local.domain
  validation_method = "DNS"

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_route53_record" "cert_validation" {
  for_each = {
    for dvo in aws_acm_certificate.site.domain_validation_options : dvo.domain_name => {
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

resource "aws_acm_certificate_validation" "site" {
  provider                = aws.us_east_1
  certificate_arn         = aws_acm_certificate.site.arn
  validation_record_fqdns = [for r in aws_route53_record.cert_validation : r.fqdn]
}


# -----------------------------------------------------------------------------
# ACM certificate — routing API (must be in us-east-1 for API Gateway)
# -----------------------------------------------------------------------------

resource "aws_acm_certificate" "api" {
  domain_name       = local.api_domain
  validation_method = "DNS"

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_route53_record" "api_cert_validation" {
  for_each = {
    for dvo in aws_acm_certificate.api.domain_validation_options : dvo.domain_name => {
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

resource "aws_acm_certificate_validation" "api" {
  certificate_arn         = aws_acm_certificate.api.arn
  validation_record_fqdns = [for r in aws_route53_record.api_cert_validation : r.fqdn]
}


# -----------------------------------------------------------------------------
# Route53 — A Records pointing to CloudFront
# -----------------------------------------------------------------------------

resource "aws_route53_record" "site" {
  zone_id = var.zone_id
  name    = local.domain
  type    = "A"

  alias {
    name                   = aws_cloudfront_distribution.site.domain_name
    zone_id                = aws_cloudfront_distribution.site.hosted_zone_id
    evaluate_target_health = false
  }
}

resource "aws_route53_record" "api" {
  zone_id = var.zone_id
  name    = local.api_domain
  type    = "A"

  alias {
    name                   = aws_apigatewayv2_domain_name.api.domain_name_configuration[0].target_domain_name
    zone_id                = aws_apigatewayv2_domain_name.api.domain_name_configuration[0].hosted_zone_id
    evaluate_target_health = false
  }
}


# -----------------------------------------------------------------------------
# IAM — deploy user for S3 sync + CloudFront invalidation + Lambda deploy
# -----------------------------------------------------------------------------

resource "aws_iam_user" "deploy" {
  name = "${local.resource_name}-deploy"
}

resource "aws_iam_user_policy" "deploy" {
  name = "${local.resource_name}-deploy"
  user = aws_iam_user.deploy.name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "S3SyncSite"
        Effect   = "Allow"
        Action   = ["s3:PutObject", "s3:DeleteObject", "s3:ListBucket"]
        Resource = [aws_s3_bucket.site.arn, "${aws_s3_bucket.site.arn}/*"]
      },
      {
        Sid    = "S3RoutingGraph"
        Effect = "Allow"
        Action = ["s3:PutObject", "s3:GetObject", "s3:ListBucket"]
        Resource = [
          aws_s3_bucket.routing_graph.arn,
          "${aws_s3_bucket.routing_graph.arn}/*"
        ]
      },
      {
        Sid      = "CloudFrontInvalidate"
        Effect   = "Allow"
        Action   = "cloudfront:CreateInvalidation"
        Resource = aws_cloudfront_distribution.site.arn
      },
      {
        Sid      = "LambdaDeploy"
        Effect   = "Allow"
        Action   = "lambda:UpdateFunctionCode"
        Resource = aws_lambda_function.routing_api.arn
      }
    ]
  })
}


# -----------------------------------------------------------------------------
# IAM — Lambda execution role
# -----------------------------------------------------------------------------

resource "aws_iam_role" "routing_lambda" {
  name = "${local.resource_name}-routing-lambda"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect    = "Allow"
        Principal = { Service = "lambda.amazonaws.com" }
        Action    = "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_role_policy" "routing_lambda" {
  name = "${local.resource_name}-routing-lambda"
  role = aws_iam_role.routing_lambda.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "CloudWatchLogs"
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "arn:aws:logs:*:*:*"
      },
      {
        Sid    = "ReadRoutingGraph"
        Effect = "Allow"
        Action = ["s3:GetObject", "s3:ListBucket"]
        Resource = [
          aws_s3_bucket.routing_graph.arn,
          "${aws_s3_bucket.routing_graph.arn}/*"
        ]
      }
    ]
  })
}


# -----------------------------------------------------------------------------
# Lambda — routing API function
#
# The deployment package is managed by the Airflow deploy DAG, not OpenTofu.
# On first apply the zip must already exist at s3_key in the routing graph
# bucket. Subsequent code deploys are done via lambda:UpdateFunctionCode.
# -----------------------------------------------------------------------------

data "archive_file" "lambda_dummy" {
  type        = "zip"
  output_path = "${path.module}/dummy.zip"
  source {
    content  = "def lambda_handler(e, c): return {'statusCode': 503, 'body': 'Not deployed yet'}"
    filename = "handler.py"
  }
}

resource "aws_lambda_function" "routing_api" {
  function_name    = "${local.resource_name}-routing-api"
  role             = aws_iam_role.routing_lambda.arn
  package_type     = "Zip"
  filename         = data.archive_file.lambda_dummy.output_path
  source_code_hash = data.archive_file.lambda_dummy.output_base64sha256
  runtime          = "python3.12"
  handler          = "handler.lambda_handler"
  timeout          = 30
  memory_size      = 2048

  environment {
    variables = {
      BIKE_MAP_GRAPH_BUCKET = aws_s3_bucket.routing_graph.bucket
      BIKE_MAP_GRAPH_KEY    = "graph/routing_graph.pkl.gz"
      BIKE_MAP_API_KEY      = var.bike_map_routing_api_key
    }
  }

  lifecycle {
    ignore_changes = [
      filename,
      source_code_hash,
      s3_bucket,
      s3_key,
    ]
  }
}

resource "aws_lambda_permission" "api_gateway" {
  statement_id  = "AllowAPIGatewayInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.routing_api.function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_apigatewayv2_api.routing.execution_arn}/*/*"
}


# -----------------------------------------------------------------------------
# API Gateway v2 — HTTP API
# -----------------------------------------------------------------------------

resource "aws_apigatewayv2_api" "routing" {
  name          = "${local.resource_name}-routing-api"
  protocol_type = "HTTP"

  cors_configuration {
    allow_origins = concat(["https://${local.domain}"], var.extra_cors_origins)
    allow_methods = ["POST"]
    allow_headers = ["Content-Type", "X-Api-Key"]
    max_age       = 3600
  }
}

resource "aws_apigatewayv2_integration" "routing_lambda" {
  api_id                 = aws_apigatewayv2_api.routing.id
  integration_type       = "AWS_PROXY"
  integration_uri        = aws_lambda_function.routing_api.invoke_arn
  payload_format_version = "2.0"
}

resource "aws_apigatewayv2_route" "post_route" {
  api_id    = aws_apigatewayv2_api.routing.id
  route_key = "POST /route"
  target    = "integrations/${aws_apigatewayv2_integration.routing_lambda.id}"
}

resource "aws_apigatewayv2_stage" "default" {
  api_id      = aws_apigatewayv2_api.routing.id
  name        = "$default"
  auto_deploy = true

  default_route_settings {
    throttling_rate_limit  = var.api_throttle_rate
    throttling_burst_limit = var.api_throttle_burst
  }
}

resource "aws_apigatewayv2_domain_name" "api" {
  domain_name = local.api_domain

  domain_name_configuration {
    certificate_arn = aws_acm_certificate_validation.api.certificate_arn
    endpoint_type   = "REGIONAL"
    security_policy = "TLS_1_2"
  }
}

resource "aws_apigatewayv2_api_mapping" "api" {
  api_id      = aws_apigatewayv2_api.routing.id
  domain_name = aws_apigatewayv2_domain_name.api.id
  stage       = aws_apigatewayv2_stage.default.id
}
