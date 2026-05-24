# modules/waf/main.tf
# AWS WAF for CloudFront distributions.
#
# One WebACL per environment, attached to every routing-API CloudFront
# distribution plus the landing distribution. Two rules:
#   - AWSManagedRulesCommonRuleSet: blocks common attack patterns
#     (injection probes, bad user agents, etc).
#   - rate-limit-per-ip: 100 req / 5min per IP. The WAF minimum.

# WAF for CloudFront must be created in us-east-1 regardless of where the
# distributions live, because the WebACL scope is CLOUDFRONT (global).
resource "aws_wafv2_web_acl" "cloudfront" {
  provider = aws.us_east_1
  name     = "${var.basename}-${var.environment}-cloudfront"
  scope    = "CLOUDFRONT"

  default_action {
    allow {}
  }

  rule {
    name     = "common-rule-set"
    priority = 1

    override_action {
      none {}
    }

    statement {
      managed_rule_group_statement {
        name        = "AWSManagedRulesCommonRuleSet"
        vendor_name = "AWS"
      }
    }

    visibility_config {
      cloudwatch_metrics_enabled = true
      metric_name                = "common-rule-set"
      sampled_requests_enabled   = true
    }
  }

  rule {
    name     = "rate-limit-per-ip"
    priority = 2

    action {
      block {}
    }

    statement {
      rate_based_statement {
        limit              = 100
        aggregate_key_type = "IP"
      }
    }

    visibility_config {
      cloudwatch_metrics_enabled = true
      metric_name                = "rate-limit-per-ip"
      sampled_requests_enabled   = true
    }
  }

  visibility_config {
    cloudwatch_metrics_enabled = true
    metric_name                = "${var.basename}-${var.environment}-cloudfront"
    sampled_requests_enabled   = true
  }
}

# -----------------------------------------------------------------------------
# WAF logging — CloudWatch Logs
#
# Required log group name format: aws-waf-logs-<anything>. AWS WAF won't
# accept any other prefix.
# -----------------------------------------------------------------------------

resource "aws_cloudwatch_log_group" "waf" {
  provider          = aws.us_east_1
  name              = "aws-waf-logs-${var.basename}-${var.environment}-cloudfront"
  retention_in_days = 14
}

resource "aws_wafv2_web_acl_logging_configuration" "cloudfront" {
  provider                = aws.us_east_1
  log_destination_configs = [aws_cloudwatch_log_group.waf.arn]
  resource_arn            = aws_wafv2_web_acl.cloudfront.arn
}

# -----------------------------------------------------------------------------
# CloudWatch alarm — blocked requests
#
# Fires when WAF blocks more than the threshold in a 5-minute window. That's
# the signal that an attack is in progress. The threshold is set higher than
# expected background noise (scanners, broken clients) so it only fires on
# real abuse.
# -----------------------------------------------------------------------------

resource "aws_cloudwatch_metric_alarm" "blocked_requests" {
  provider            = aws.us_east_1
  alarm_name          = "${var.basename}-${var.environment}-waf-blocked"
  alarm_description   = "WAF is blocking a significant volume of requests; likely an attack in progress."
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "BlockedRequests"
  namespace           = "AWS/WAFV2"
  period              = 300
  statistic           = "Sum"
  threshold           = 100
  treat_missing_data  = "notBreaching"

  dimensions = {
    WebACL = aws_wafv2_web_acl.cloudfront.name
    Region = "CloudFront"
    Rule   = "ALL"
  }

  alarm_actions = [aws_sns_topic.waf_alerts.arn]
}

resource "aws_sns_topic" "waf_alerts" {
  provider = aws.us_east_1
  name     = "${var.basename}-${var.environment}-waf-alerts"
}

resource "aws_sns_topic_subscription" "waf_alerts_email" {
  provider  = aws.us_east_1
  topic_arn = aws_sns_topic.waf_alerts.arn
  protocol  = "email"
  endpoint  = var.alarm_email
}
