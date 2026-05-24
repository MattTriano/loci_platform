# modules/cost-guard/main.tf
# Cost guard — AWS Budgets with multi-tier alerts and an automated kill switch.
#
# Three tiers (all alert via email; the highest also triggers the kill-switch
# Lambda):
#   - alert: informational
#   - warn:  warning
#   - kill:  invokes the kill-switch Lambda, which sets API Gateway stage
#           throttles to 0 across every routing HTTP API and route-logger
#           REST API passed in via var.http_api_stages and var.rest_api_stages.
#
# To restore service after a kill, run `tofu apply` — Terraform will detect
# the drifted throttle limits and put them back.

# -----------------------------------------------------------------------------
# AWS Budget
#
# time_period_start is omitted so AWS uses the start of the current month.
# Notifications fire on absolute dollar thresholds, not percentages, so they
# don't drift if var.budget_kill_amount changes.
# -----------------------------------------------------------------------------

resource "aws_budgets_budget" "account" {
  name         = "${var.basename}-${var.environment}-account"
  budget_type  = "COST"
  limit_amount = tostring(var.budget_kill_amount)
  limit_unit   = "USD"
  time_unit    = "MONTHLY"

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = var.budget_alert_amount
    threshold_type             = "ABSOLUTE_VALUE"
    notification_type          = "ACTUAL"
    subscriber_email_addresses = [var.alarm_email]
  }

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = var.budget_warn_amount
    threshold_type             = "ABSOLUTE_VALUE"
    notification_type          = "ACTUAL"
    subscriber_email_addresses = [var.alarm_email]
  }

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = var.budget_kill_amount
    threshold_type             = "ABSOLUTE_VALUE"
    notification_type          = "ACTUAL"
    subscriber_email_addresses = [var.alarm_email]
    subscriber_sns_topic_arns  = [aws_sns_topic.kill_switch.arn]
  }
}

# -----------------------------------------------------------------------------
# SNS topic — kill switch trigger
#
# AWS Budgets publishes here when monthly spend exceeds the kill threshold.
# The kill-switch Lambda subscribes here.
# -----------------------------------------------------------------------------

resource "aws_sns_topic" "kill_switch" {
  name = "${var.basename}-${var.environment}-cost-guard-kill"
}

resource "aws_sns_topic_policy" "kill_switch" {
  arn = aws_sns_topic.kill_switch.arn
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "AllowBudgetsPublish"
      Effect    = "Allow"
      Principal = { Service = "budgets.amazonaws.com" }
      Action    = "SNS:Publish"
      Resource  = aws_sns_topic.kill_switch.arn
    }]
  })
}

# -----------------------------------------------------------------------------
# IAM — kill-switch Lambda execution role
#
# Permissions:
#   - apigateway:PATCH on HTTP and REST API stages (wildcarded; the Lambda
#     only touches stages explicitly listed in its env vars, so the wildcard
#     is for ARN matching convenience, not actual scope expansion).
#   - sns:Publish to the alerts topic for the "kill switch fired" confirmation.
#   - logs:* for CloudWatch.
# -----------------------------------------------------------------------------

resource "aws_iam_role" "kill_switch" {
  name = "${var.basename}-${var.environment}-cost-guard-kill"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "kill_switch" {
  name = "${var.basename}-${var.environment}-cost-guard-kill"
  role = aws_iam_role.kill_switch.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "Logs"
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents",
        ]
        Resource = "${aws_cloudwatch_log_group.kill_switch.arn}:*"
      },
      {
        Sid    = "DisableApiGatewayStages"
        Effect = "Allow"
        Action = "apigateway:PATCH"
        Resource = [
          "arn:aws:apigateway:*::/apis/*/stages/*",
          "arn:aws:apigateway:*::/restapis/*/stages/*",
        ]
      },
      {
        Sid      = "NotifyAlerts"
        Effect   = "Allow"
        Action   = "sns:Publish"
        Resource = var.alarm_sns_topic_arn
      },
    ]
  })
}

# -----------------------------------------------------------------------------
# CloudWatch log group — kill-switch Lambda
# -----------------------------------------------------------------------------

resource "aws_cloudwatch_log_group" "kill_switch" {
  name              = "/aws/lambda/${var.basename}-${var.environment}-cost-guard-kill"
  retention_in_days = 30
}

# -----------------------------------------------------------------------------
# Lambda — kill switch
# -----------------------------------------------------------------------------

data "archive_file" "kill_switch" {
  type        = "zip"
  source_file = "${path.module}/lambda/kill_switch.py"
  output_path = "${path.module}/kill_switch.zip"
}

resource "aws_lambda_function" "kill_switch" {
  function_name    = "${var.basename}-${var.environment}-cost-guard-kill"
  role             = aws_iam_role.kill_switch.arn
  filename         = data.archive_file.kill_switch.output_path
  source_code_hash = data.archive_file.kill_switch.output_base64sha256
  runtime          = "python3.12"
  handler          = "kill_switch.lambda_handler"
  timeout          = 60

  environment {
    variables = {
      HTTP_API_STAGES     = jsonencode(var.http_api_stages)
      REST_API_STAGES     = jsonencode(var.rest_api_stages)
      ALERT_SNS_TOPIC_ARN = var.alarm_sns_topic_arn
    }
  }

  depends_on = [
    aws_cloudwatch_log_group.kill_switch,
    aws_iam_role_policy.kill_switch,
  ]
}

resource "aws_sns_topic_subscription" "kill_switch" {
  topic_arn = aws_sns_topic.kill_switch.arn
  protocol  = "lambda"
  endpoint  = aws_lambda_function.kill_switch.arn
}

resource "aws_lambda_permission" "kill_switch_sns" {
  statement_id  = "AllowSNSInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.kill_switch.function_name
  principal     = "sns.amazonaws.com"
  source_arn    = aws_sns_topic.kill_switch.arn
}
