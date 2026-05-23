# /loci_platform/infra/modules/route-logger/outputs.tf
output "log_endpoint" {
  description = "POST endpoint URL for route logging (set this as LOG_ENDPOINT in index.html)"
  value       = "${aws_api_gateway_stage.v1.invoke_url}/log"
}

output "log_bucket_name" {
  description = "S3 bucket where route logs are stored"
  value       = aws_s3_bucket.route_logs.id
}

output "log_bucket_arn" {
  description = "ARN of the route logs bucket (for Athena or warehouse ingestion)"
  value       = aws_s3_bucket.route_logs.arn
}

output "rest_api_id" {
  description = "ID of the route-logger REST API. Used by the cost-guard kill switch."
  value       = aws_api_gateway_rest_api.route_logger.id
}
