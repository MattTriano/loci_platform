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
