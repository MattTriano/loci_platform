# modules/synthetic-monitor/outputs.tf

output "lambda_function_name" {
  description = "Name of the synthetic monitor Lambda. Useful for manual invocation during testing."
  value       = aws_lambda_function.synthetic_monitor.function_name
}

output "status_bucket" {
  description = "S3 bucket holding status.json. Phase 2 dashboard will read from this."
  value       = aws_s3_bucket.status.bucket
}

output "dashboard_url" {
  description = "Public URL for the status dashboard."
  value       = "https://status.${var.zone_name}"
}
