# /loci_platform/infra/modules/route-logger/variables.tf
variable "basename" {
  description = "Project base name, e.g. 'missinglastmile'"
  type        = string
}

variable "environment" {
  description = "Environment name, e.g. 'dev', 'prod'"
  type        = string
}

variable "city" {
  description = "City identifier (e.g. 'chicago'). Used in resource naming and the log bucket name."
  type        = string
}

variable "log_retention_days" {
  description = "Days to keep route log objects in S3 before expiring"
  type        = number
  default     = 365
}

variable "allowed_origin" {
  description = "Domain allowed to make requests (e.g. 'chicago.bikeinfra.com'). Used for CORS Access-Control-Allow-Origin header."
  type        = string
}

variable "log_bucket_name" {
  description = "Optional override for the log S3 bucket name. Defaults to '<basename>-<environment>-<city>-route-logs'."
  type        = string
  default     = null
}