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
  description = "Base domain name (e.g. missinglastmile.net)."
  type        = string
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
