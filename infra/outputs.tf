# /loci_platform/infra/outputs.tf
output "dns_zone_name_servers" {
  description = "NS records to add to the parent zone in admin-mgmt. (Dev/staging only.)"
  value       = var.environment == "prod" ? null : module.dns_zone[0].name_servers
}

output "bike_map_urls" {
  description = "Public URLs of the bike map, per city."
  value       = { for c, m in module.bike_map : c => m.site_url }
}

output "routing_api_urls" {
  description = "Public URLs of the routing APIs, per city."
  value       = { for c, m in module.bike_map : c => m.routing_api_url }
}

output "routing_lambda_arns" {
  description = "ARNs of the routing Lambda functions, per city."
  value       = { for c, m in module.bike_map : c => m.routing_lambda_arn }
}

output "routing_graph_bucket_names" {
  description = "S3 bucket names for routing graphs, per city."
  value       = { for c, m in module.bike_map : c => m.routing_graph_bucket_name }
}

output "route_log_endpoints" {
  description = "Per-city POST endpoint URLs for route logging."
  value       = { for c, m in module.route_logger : c => m.log_endpoint }
}

output "cloudfront_dist_ids" {
  description = "CloudFront distribution IDs, per city."
  value       = { for c, m in module.bike_map : c => m.cloudfront_distribution_id }
}
