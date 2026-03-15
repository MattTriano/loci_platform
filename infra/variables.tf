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
