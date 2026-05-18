# /loci_platform/infra/main.tf
provider "aws" {
  region = "us-east-2"
}

provider "aws" {
  alias  = "us_east_1"
  region = "us-east-1"
}

provider "aws" {
  alias  = "admin_mgmt"
  region = "us-east-2"

  dynamic "assume_role" {
    for_each = var.environment == "prod" ? [1] : []
    content {
      role_arn     = var.admin_mgmt_dns_role_arn
      external_id  = "bikeinfra-dns-writer-v1"
      session_name = "tofu-bikeinfra-dns"
    }
  }
}

# DNS zone — only for dev and staging.
# Prod uses the admin-mgmt apex zone passed in via variable.
module "dns_zone" {
  source = "./modules/dns-zone"
  count  = var.environment == "prod" ? 0 : 1

  environment = var.environment
  base_domain = var.base_domain
}

locals {
  zone_id   = var.environment == "prod" ? var.admin_mgmt_zone_id : module.dns_zone[0].zone_id
  zone_name = var.environment == "prod" ? var.base_domain : module.dns_zone[0].zone_name
}

module "bike_map" {
  source   = "./modules/bike-map"
  for_each = toset(var.cities)

  providers = {
    aws           = aws
    aws.us_east_1 = aws.us_east_1
    aws.dns       = aws.admin_mgmt
  }

  basename                 = var.basename
  environment              = var.environment
  city                     = each.key
  zone_id                  = local.zone_id
  zone_name                = local.zone_name
  extra_cors_origins       = var.extra_cors_origins
  routing_lambda_memory_mb = var.routing_lambda_memory_by_city[each.key]
}

module "route_logger" {
  source   = "./modules/route-logger"
  for_each = toset(var.cities)

  basename           = var.basename
  environment        = var.environment
  city               = each.key
  allowed_origin     = "${each.key}.${local.zone_name}"
  log_retention_days = 730
}
