# /loci_platform/infra/modules/dns-zone/variables.tf
variable "environment" {
  description = "Deployment environment (dev, staging, prod)."
  type        = string
}

variable "base_domain" {
  description = "Base domain name (e.g. missinglastmile.net)."
  type        = string
}
