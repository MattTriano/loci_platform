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
  description = "Route53 hosted zone domain name (e.g. dev.missinglastmile.net)."
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

variable "bike_map_routing_api_key" {
  description = "API key required on all routing requests (sent as X-Api-Key header)."
  type        = string
  sensitive   = true
}

variable "extra_cors_origins" {
  description = "Additional CORS origins for the bike map routing API (e.g. local dev)"
  type        = list(string)
  default     = []
}
