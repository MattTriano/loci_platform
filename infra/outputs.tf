output "dns_zone_name_servers" {
  description = "NS records to add to the parent zone in the mgmt account."
  value       = module.dns_zone.name_servers
}

output "bike_map_url" {
  description = "Public URL of the bike map."
  value       = module.bike_map.site_url
}

output "routing_lambda_arn" {
  description = "ARN of the routing Lambda function."
  value       = module.bike_map.routing_lambda_arn
}

output "routing_graph_bucket_name" {
  description = "S3 bucket name for the routing graph and Lambda package."
  value       = module.bike_map.routing_graph_bucket_name
}

output "routing_api_url" {
  description = "Public URL of the routing API."
  value       = module.bike_map.routing_api_url
}
