# /loci_platform/infra/modules/bike-map/variables.tf
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

variable "city" {
  description = "City identifier (e.g. 'chicago'). Used in resource naming and the public URL."
  type        = string
}

variable "app_name" {
  description = "Application name for resource naming."
  type        = string
  default     = "bike-map"
}

variable "zone_id" {
  description = "Route53 hosted zone ID for DNS records."
  type        = string
}

variable "zone_name" {
  description = "Route53 hosted zone domain name (e.g. dev.bikeinfra.com, or bikeinfra.com for prod)."
  type        = string
}

variable "api_throttle_rate" {
  description = "API Gateway sustained requests per second."
  type        = number
  default     = 5
}

variable "api_throttle_burst" {
  description = "API Gateway burst requests per second."
  type        = number
  default     = 10
}

variable "extra_cors_origins" {
  description = "Additional CORS origins for the bike map routing API (e.g. local dev)"
  type        = list(string)
  default     = []
}

variable "routing_lambda_memory_mb" {
  description = "Memory (in MB) allocated to the routing Lambda. Lambda accepts 128–10240 in 1 MB increments."
  type        = number
  validation {
    condition     = var.routing_lambda_memory_mb >= 128 && var.routing_lambda_memory_mb <= 10240
    error_message = "routing_lambda_memory_mb must be between 128 and 10240."
  }
}
