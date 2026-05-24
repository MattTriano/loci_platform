# loci_platform/infra/modules/bike-map-landing/variables.tf
variable "basename" {
  description = "Base name for resource naming."
  type        = string
}

variable "environment" {
  description = "Deployment environment (dev, staging, prod)."
  type        = string
  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "Invalid environment. Must be dev, staging, or prod."
  }
}

variable "zone_id" {
  description = "Route53 hosted zone ID for DNS records."
  type        = string
}

variable "zone_name" {
  description = "Route53 hosted zone domain name. The landing page is served at the apex of this zone (e.g. 'dev.bikeinfra.com' or 'bikeinfra.com')."
  type        = string
}

variable "cities" {
  description = "List of city identifiers deployed in this environment. Written to SSM so the landing deploy task can read it."
  type        = list(string)
}

variable "basic_auth_function_arn" {
  description = "ARN of a CloudFront Function to attach at viewer-request for basic auth. Null means no auth attached."
  type        = string
  default     = null
}

variable "response_headers_policy_id" {
  description = "ID of a CloudFront Response Headers Policy to attach. Null means no policy attached."
  type        = string
  default     = null
}

variable "waf_web_acl_arn" {
  description = "ARN of a WAF WebACL to attach to the routing API CloudFront distribution. Null means no WAF."
  type        = string
  default     = null
}
