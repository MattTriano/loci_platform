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
