# modules/waf/variables.tf

variable "basename" {
  description = "Base name for resource naming."
  type        = string
}

variable "environment" {
  description = "Deployment environment (dev, staging, prod)."
  type        = string
}

variable "alarm_email" {
  description = "Email subscribed to the WAF-specific SNS topic. Required because CloudWatch alarms in us-east-1 (where WAF for CloudFront emits metrics) cannot invoke SNS topics in us-east-2."
  type        = string
}
