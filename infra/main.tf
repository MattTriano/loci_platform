provider "aws" {
  region = "us-east-2"
}

provider "aws" {
  alias  = "us_east_1"
  region = "us-east-1"
}

module "dns_zone" {
  source = "./modules/dns-zone"

  environment = var.environment
  base_domain = var.base_domain
}

module "bike_map" {
  source = "./modules/bike-map"

  providers = {
    aws           = aws
    aws.us_east_1 = aws.us_east_1
  }

  basename                 = var.basename
  environment              = var.environment
  zone_id                  = module.dns_zone.zone_id
  zone_name                = module.dns_zone.zone_name
  bike_map_routing_api_key = var.bike_map_routing_api_key
  extra_cors_origins       = var.extra_cors_origins
}

module "route_logger" {
  source = "./modules/route-logger"

  basename           = var.basename
  environment        = var.environment
  allowed_origin     = "bike-map.${var.environment}.${var.base_domain}"
  log_retention_days = 730 # default; adjust as needed
}

output "route_log_endpoint" {
  description = "Set this as LOG_ENDPOINT in index.html"
  value       = module.route_logger.log_endpoint
}
