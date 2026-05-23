# modules/cost-guard/variables.tf

variable "basename" {
  description = "Base name for resource naming."
  type        = string
}

variable "environment" {
  description = "Deployment environment (dev, staging, prod)."
  type        = string
}

variable "alarm_email" {
  description = "Email subscribed to budget alerts."
  type        = string
}

variable "alarm_sns_topic_arn" {
  description = "ARN of the shared alerts SNS topic. Kill-switch Lambda publishes its confirmation here."
  type        = string
}

variable "budget_alert_amount" {
  description = "Monthly spend (USD) that triggers an informational email alert."
  type        = number
  default     = 5
}

variable "budget_warn_amount" {
  description = "Monthly spend (USD) that triggers a warning email alert."
  type        = number
  default     = 15
}

variable "budget_kill_amount" {
  description = "Monthly spend (USD) that triggers the kill switch."
  type        = number
  default     = 30
}

variable "http_api_stages" {
  description = "HTTP API Gateway v2 stages the kill switch will disable."
  type = list(object({
    api_id     = string
    stage_name = string
  }))
}

variable "rest_api_stages" {
  description = "REST API Gateway v1 stages the kill switch will disable."
  type = list(object({
    rest_api_id = string
    stage_name  = string
  }))
}
