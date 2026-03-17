variable "basename" {
  description = "Project base name, e.g. 'missinglastmile'"
  type        = string
}

variable "environment" {
  description = "Environment name, e.g. 'dev', 'prod'"
  type        = string
}

variable "log_retention_days" {
  description = "Days to keep route log objects in S3 before expiring"
  type        = number
  default     = 365
}

variable "allowed_origin" {
  description = "Domain allowed to make requests (e.g. 'bikemap.missinglastmile.net'). Used for CORS Access-Control-Allow-Origin header."
  type        = string
}
