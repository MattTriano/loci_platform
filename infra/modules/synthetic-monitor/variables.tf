# modules/synthetic-monitor/variables.tf

variable "basename" {
  description = "Base name for resource naming."
  type        = string
}

variable "alarm_sns_topic_arn" {
  description = "ARN of the alerts SNS topic for the Lambda-errors alarm."
  type        = string
}

variable "environment" {
  description = "Deployment environment (dev, staging, prod)."
  type        = string
}

variable "cities" {
  description = "List of city identifiers to monitor."
  type        = list(string)
}

variable "zone_name" {
  description = "Route53 zone name for building target URLs (e.g. 'dev.bikeinfra.com', 'bikeinfra.com')."
  type        = string
}

variable "lockdown_non_prod" {
  description = "Whether non-prod basic auth is active. When true, the synthetic Lambda reads credentials from SSM to include in static-site checks."
  type        = bool
  default     = false
}

variable "zone_id" {
  description = "Route53 hosted zone ID for the dashboard DNS record."
  type        = string
}

variable "basic_auth_function_arn" {
  description = "ARN of the basic auth CloudFront Function. Null means no auth (prod). Reuses the function created in modules/bike-map-landing or root for non-prod."
  type        = string
  default     = null
}

variable "response_headers_policy_id" {
  description = "ID of a CloudFront response headers policy (for noindex in non-prod). Null means no policy."
  type        = string
  default     = null
}
