output "zone_id" {
  description = "Route53 hosted zone ID."
  value       = aws_route53_zone.env.zone_id
}

output "name_servers" {
  description = "NS records to add to the parent zone in the mgmt account."
  value       = aws_route53_zone.env.name_servers
}

output "zone_name" {
  description = "The zone domain name."
  value       = aws_route53_zone.env.name
}
