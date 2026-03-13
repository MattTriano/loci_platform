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

  basename    = var.basename
  environment = var.environment
  zone_id     = module.dns_zone.zone_id
  zone_name   = module.dns_zone.zone_name
}
