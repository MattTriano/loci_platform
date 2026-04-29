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
  description = "Map of city identifier to per-city config. One entry per city deployed in this env."
  type = map(object({
    routing_api_key = string
  }))
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
