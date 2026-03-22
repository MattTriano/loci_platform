# Route Logger — API Gateway → S3
#
# A single POST endpoint that writes route request logs directly to S3
# using an API Gateway AWS service integration (no Lambda).
#
# Each request is stored as a JSON object in S3 keyed by timestamp + request ID.
# The IP address is injected server-side via the mapping template.

# ── S3 bucket for logs ────────────────────────────────────
resource "aws_s3_bucket" "route_logs" {
  bucket = "${var.basename}-${var.environment}-route-logs"
}

resource "aws_s3_bucket_lifecycle_configuration" "route_logs" {
  bucket = aws_s3_bucket.route_logs.id

  rule {
    id     = "expire-old-logs"
    status = "Enabled"

    expiration {
      days = var.log_retention_days
    }
  }
}

# ── IAM role for API Gateway to write to S3 ───────────────
resource "aws_iam_role" "apigw_s3" {
  name = "${var.basename}-${var.environment}-route-logger-apigw"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "apigateway.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "apigw_s3_put" {
  name = "s3-put-route-logs"
  role = aws_iam_role.apigw_s3.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = "s3:PutObject"
      Resource = "${aws_s3_bucket.route_logs.arn}/*"
    }]
  })
}

# ── API Gateway REST API ──────────────────────────────────
resource "aws_api_gateway_rest_api" "route_logger" {
  name        = "${var.basename}-${var.environment}-route-logger"
  description = "Logs bike route requests to S3"

  endpoint_configuration {
    types = ["REGIONAL"]
  }
}

# POST /log
resource "aws_api_gateway_resource" "log" {
  rest_api_id = aws_api_gateway_rest_api.route_logger.id
  parent_id   = aws_api_gateway_rest_api.route_logger.root_resource_id
  path_part   = "log"
}

resource "aws_api_gateway_method" "post_log" {
  rest_api_id   = aws_api_gateway_rest_api.route_logger.id
  resource_id   = aws_api_gateway_resource.log.id
  http_method   = "POST"
  authorization = "NONE"
}

# AWS service integration: PUT object to S3
# The S3 key uses the request timestamp and request ID for uniqueness.
resource "aws_api_gateway_integration" "s3_put" {
  rest_api_id             = aws_api_gateway_rest_api.route_logger.id
  resource_id             = aws_api_gateway_resource.log.id
  http_method             = aws_api_gateway_method.post_log.http_method
  type                    = "AWS"
  integration_http_method = "PUT"
  uri                     = "arn:aws:apigateway:${data.aws_region.current.region}:s3:path/${aws_s3_bucket.route_logs.id}/{folder}/{object}"
  credentials             = aws_iam_role.apigw_s3.arn

  # Map the request body + inject server-side context
  request_parameters = {
    "integration.request.path.folder"         = "context.requestTimeEpoch"
    "integration.request.path.object"         = "context.requestId"
    "integration.request.header.Content-Type" = "'application/json'"
  }

  # Wrap the client payload with server-side metadata (IP, request ID, etc.)
  request_templates = {
    "application/json" = <<-EOF
#set($body = $input.json('$'))
{
  "server_timestamp": "$context.requestTime",
  "request_id": "$context.requestId",
  "source_ip": "$context.identity.sourceIp",
  "user_agent_header": "$context.identity.userAgent",
  "client_payload": $body
}
EOF
  }
}

# 200 response
resource "aws_api_gateway_method_response" "ok" {
  rest_api_id = aws_api_gateway_rest_api.route_logger.id
  resource_id = aws_api_gateway_resource.log.id
  http_method = aws_api_gateway_method.post_log.http_method
  status_code = "200"

  response_parameters = {
    "method.response.header.Access-Control-Allow-Origin" = true
  }
}

resource "aws_api_gateway_integration_response" "ok" {
  rest_api_id = aws_api_gateway_rest_api.route_logger.id
  resource_id = aws_api_gateway_resource.log.id
  http_method = aws_api_gateway_method.post_log.http_method
  status_code = aws_api_gateway_method_response.ok.status_code

  response_parameters = {
    "method.response.header.Access-Control-Allow-Origin" = "'https://${var.allowed_origin}'"
  }

  response_templates = {
    "application/json" = "{\"status\": \"logged\"}"
  }

  depends_on = [aws_api_gateway_integration.s3_put]
}

# ── CORS: OPTIONS preflight ───────────────────────────────
resource "aws_api_gateway_method" "options_log" {
  rest_api_id   = aws_api_gateway_rest_api.route_logger.id
  resource_id   = aws_api_gateway_resource.log.id
  http_method   = "OPTIONS"
  authorization = "NONE"
}

resource "aws_api_gateway_integration" "options_log" {
  rest_api_id = aws_api_gateway_rest_api.route_logger.id
  resource_id = aws_api_gateway_resource.log.id
  http_method = aws_api_gateway_method.options_log.http_method
  type        = "MOCK"

  request_templates = {
    "application/json" = "{\"statusCode\": 200}"
  }
}

resource "aws_api_gateway_method_response" "options_ok" {
  rest_api_id = aws_api_gateway_rest_api.route_logger.id
  resource_id = aws_api_gateway_resource.log.id
  http_method = aws_api_gateway_method.options_log.http_method
  status_code = "200"

  response_parameters = {
    "method.response.header.Access-Control-Allow-Headers" = true
    "method.response.header.Access-Control-Allow-Methods" = true
    "method.response.header.Access-Control-Allow-Origin"  = true
  }
}

resource "aws_api_gateway_integration_response" "options_ok" {
  rest_api_id = aws_api_gateway_rest_api.route_logger.id
  resource_id = aws_api_gateway_resource.log.id
  http_method = aws_api_gateway_method.options_log.http_method
  status_code = "200"

  response_parameters = {
    "method.response.header.Access-Control-Allow-Headers" = "'Content-Type'"
    "method.response.header.Access-Control-Allow-Methods" = "'POST,OPTIONS'"
    "method.response.header.Access-Control-Allow-Origin"  = "'https://${var.allowed_origin}'"
  }

  depends_on = [aws_api_gateway_integration.options_log]
}

# ── Deployment + stage ────────────────────────────────────
resource "aws_api_gateway_deployment" "route_logger" {
  rest_api_id = aws_api_gateway_rest_api.route_logger.id

  # Redeploy when any of these resources change
  triggers = {
    redeployment = sha1(jsonencode([
      aws_api_gateway_resource.log,
      aws_api_gateway_method.post_log,
      aws_api_gateway_integration.s3_put,
      aws_api_gateway_method.options_log,
      aws_api_gateway_integration.options_log,
    ]))
  }

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_api_gateway_stage" "v1" {
  rest_api_id   = aws_api_gateway_rest_api.route_logger.id
  deployment_id = aws_api_gateway_deployment.route_logger.id
  stage_name    = "v1"
}

resource "aws_api_gateway_method_settings" "throttle" {
  rest_api_id = aws_api_gateway_rest_api.route_logger.id
  stage_name  = aws_api_gateway_stage.v1.stage_name
  method_path = "*/*"

  settings {
    throttling_rate_limit  = 10 # sustained requests per second
    throttling_burst_limit = 50 # burst allowance
  }
}

# ── Data sources ──────────────────────────────────────────
data "aws_region" "current" {}
