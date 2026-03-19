output "s3_bucket_name" {
  description = "S3 bucket name for site content."
  value       = aws_s3_bucket.site.bucket
}

output "routing_graph_bucket_name" {
  description = "S3 bucket name for routing graph Parquet export."
  value       = aws_s3_bucket.routing_graph.bucket
}

output "cloudfront_distribution_id" {
  description = "CloudFront distribution ID for cache invalidation."
  value       = aws_cloudfront_distribution.site.id
}

output "cloudfront_domain_name" {
  description = "CloudFront distribution domain name."
  value       = aws_cloudfront_distribution.site.domain_name
}

output "deploy_user_name" {
  description = "IAM deploy user name."
  value       = aws_iam_user.deploy.name
}

output "site_url" {
  description = "Public URL of the site."
  value       = "https://${local.domain}"
}
