# /loci_platform/infra/variables.tf
variable "basename" {
  description = "The base name for created resources."
  type        = string
  default     = "loci-infra"
}

variable "environment" {
  description = "The environment to deploy to"
  type        = string
  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "Invalid env value, accepted env values: [dev, staging, prod]"
  }
}

variable "base_domain" {
  description = "Base domain name (e.g. bikeinfra.com)."
  type        = string
}

variable "cities" {
  type        = list(string)
  description = "Cities to deploy. Each must have a corresponding module configuration."
}

variable "admin_mgmt_dns_role_arn" {
  description = "ARN of the IAM role in admin-mgmt allowing writes to bikeinfra.com. Only used for prod."
  type        = string
  default     = null
}

variable "admin_mgmt_zone_id" {
  description = "Hosted zone ID of bikeinfra.com in admin-mgmt. Only used for prod."
  type        = string
  default     = null
}

variable "extra_cors_origins" {
  description = "Additional CORS origins for routing APIs (e.g. local dev). Applied to all cities."
  type        = list(string)
  default     = []
}

variable "routing_lambda_memory_by_city" {
  description = "Per-city memory (MB) for the routing Lambda. Must include every city in var.cities."
  type        = map(number)
  default = {
    boston   = 512
    chicago  = 1024
    dc       = 512
    denver   = 512
    detroit  = 1024
    madison  = 512
    nola     = 512
    nyc      = 1536
    portland = 1024
    sf       = 1536
    toronto  = 1024
  }
}

variable "routing_lambda_timeout_seconds" {
  description = "Lambda timeout (seconds) for the routing function. Defaults to 8, slightly above the ~7s observed worst case. Tighten if real workloads stay well under; loosen if real users hit timeouts."
  type        = number
  default     = 8
}

variable "alarm_email" {
  description = "Email subscribed to the CloudWatch alarms SNS topic. AWS sends a confirmation link on first apply that I must click to activate the subscription."
  type        = string
}

variable "budget_alert_amount" {
  description = "Monthly spend (USD) that triggers an informational budget alert."
  type        = number
  default     = 5
}

variable "budget_warn_amount" {
  description = "Monthly spend (USD) that triggers a warning budget alert."
  type        = number
  default     = 15
}

variable "budget_kill_amount" {
  description = "Monthly spend (USD) that triggers the cost-guard kill switch."
  type        = number
  default     = 30
}

variable "lockdown_non_prod" {
  description = "Enable basic auth on CloudFront distributions, noindex headers, and tighter cost limits. Should be true for dev and staging, false for prod."
  type        = bool
  default     = true
}

variable "api_throttle_rate" {
  description = "API Gateway sustained requests per second for the routing API."
  type        = number
  default     = 5
}

variable "api_throttle_burst" {
  description = "API Gateway burst requests per second for the routing API."
  type        = number
  default     = 10
}

variable "enable_waf" {
  description = "Enable AWS WAF on CloudFront distributions. Defaults to false; should be true for prod only."
  type        = bool
  default     = false
}
