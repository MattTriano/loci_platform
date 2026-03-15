variable "basename" {
  description = "Base name for resource naming."
  type        = string
}

variable "environment" {
  description = "Deployment environment (dev, staging, prod)."
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
  description = "Route53 hosted zone domain name (e.g. dev.missinglastmile.net)."
  type        = string
}
