output "dns_zone_name_servers" {
  description = "NS records to add to the parent zone in the mgmt account."
  value       = module.dns_zone.name_servers
}

output "bike_map_url" {
  description = "Public URL of the bike map."
  value       = module.bike_map.site_url
}
