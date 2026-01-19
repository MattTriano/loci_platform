variable "basename" {
  description = "The base name for created resources."
  type        = string
}

variable "environment" {
  description = "The environment to deploy resources to."
  type        = string
  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "Invalid env value, accepted env values: [dev, stage, prod]"
  }
}

variable "aws_region" {
  description = "AWS region to create resources in."
  type        = string
  default     = "us-east-2"
}
