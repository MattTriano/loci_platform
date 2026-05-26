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


module "bike_map_landing" {
  source = "./modules/bike-map-landing"

  providers = {
    aws           = aws
    aws.us_east_1 = aws.us_east_1
    aws.dns       = aws.admin_mgmt
  }

  basename                   = var.basename
  environment                = var.environment
  zone_id                    = local.zone_id
  zone_name                  = local.zone_name
  cities                     = var.cities
  basic_auth_function_arn    = local.basic_auth_function_arn
  response_headers_policy_id = local.response_headers_policy_id
  waf_web_acl_arn            = local.waf_web_acl_arn
}

module "bike_map" {
  source   = "./modules/bike-map"
  for_each = toset(var.cities)

  providers = {
    aws           = aws
    aws.us_east_1 = aws.us_east_1
    aws.dns       = aws.admin_mgmt
  }

  basename                       = var.basename
  environment                    = var.environment
  city                           = each.key
  zone_id                        = local.zone_id
  zone_name                      = local.zone_name
  extra_cors_origins             = var.extra_cors_origins
  routing_lambda_memory_mb       = var.routing_lambda_memory_by_city[each.key]
  routing_lambda_timeout_seconds = var.routing_lambda_timeout_seconds
  alarm_sns_topic_arn            = aws_sns_topic.alerts.arn
  api_throttle_rate              = var.api_throttle_rate
  api_throttle_burst             = var.api_throttle_burst
  basic_auth_function_arn        = local.basic_auth_function_arn
  response_headers_policy_id     = local.response_headers_policy_id
  waf_web_acl_arn                = local.waf_web_acl_arn
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


module "cost_guard" {
  source = "./modules/cost-guard"

  basename            = var.basename
  environment         = var.environment
  alarm_email         = var.alarm_email
  alarm_sns_topic_arn = aws_sns_topic.alerts.arn

  budget_alert_amount = var.budget_alert_amount
  budget_warn_amount  = var.budget_warn_amount
  budget_kill_amount  = var.budget_kill_amount

  http_api_stages = [
    for k, m in module.bike_map : {
      api_id     = m.routing_api_id
      stage_name = "$default"
    }
  ]

  rest_api_stages = [
    for k, m in module.route_logger : {
      rest_api_id = m.rest_api_id
      stage_name  = "v1"
    }
  ]
}

module "waf" {
  source = "./modules/waf"
  count  = var.enable_waf ? 1 : 0

  providers = {
    aws.us_east_1 = aws.us_east_1
  }

  basename    = var.basename
  environment = var.environment
  alarm_email = var.alarm_email
}

locals {
  waf_web_acl_arn = var.enable_waf ? module.waf[0].web_acl_arn : null
}


module "synthetic_monitor" {
  source = "./modules/synthetic-monitor"

  providers = {
    aws           = aws
    aws.us_east_1 = aws.us_east_1
    aws.dns       = aws.admin_mgmt
  }

  basename                   = var.basename
  environment                = var.environment
  cities                     = var.cities
  zone_id                    = local.zone_id
  zone_name                  = local.zone_name
  lockdown_non_prod          = var.lockdown_non_prod
  alarm_sns_topic_arn        = aws_sns_topic.alerts.arn
  basic_auth_function_arn    = local.basic_auth_function_arn
  response_headers_policy_id = local.response_headers_policy_id
}

# -----------------------------------------------------------------------------
# Shared SNS topic for CloudWatch alarms
#
# One topic across all cities; per-city Lambda alarms publish here. Email
# subscription requires manual confirmation via a link AWS emails after the
# first apply.
# -----------------------------------------------------------------------------

resource "aws_sns_topic" "alerts" {
  name = "${var.basename}-${var.environment}-alerts"
}

resource "aws_sns_topic_subscription" "alerts_email" {
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = var.alarm_email
}


# -----------------------------------------------------------------------------
# Non-prod lockdown
#
# When lockdown_non_prod is true, the static-site CloudFront distributions
# are protected by HTTP Basic Auth (so only authenticated users can see them)
# and serve a noindex header (so search engines never crawl them).
# The credentials are stored in SSM for retrieval.
#
# To get the credentials:
#   aws ssm get-parameter \
#     --name /<basename>/<env>/non-prod-auth/credentials \
#     --with-decryption --query 'Parameter.Value' --output text
# -----------------------------------------------------------------------------

resource "random_password" "non_prod_auth" {
  count   = var.lockdown_non_prod ? 1 : 0
  length  = 24
  special = false
}

resource "aws_ssm_parameter" "non_prod_auth_credentials" {
  count       = var.lockdown_non_prod ? 1 : 0
  name        = "/${var.basename}/${var.environment}/non-prod-auth/credentials"
  description = "Basic auth credentials for non-prod CloudFront distributions. Format: username:password."
  type        = "SecureString"
  value       = "bikeinfra:${random_password.non_prod_auth[0].result}"
}

resource "aws_cloudfront_function" "basic_auth" {
  count   = var.lockdown_non_prod ? 1 : 0
  name    = "${var.basename}-${var.environment}-basic-auth"
  runtime = "cloudfront-js-2.0"
  publish = true
  code = templatefile("${path.module}/templates/basic_auth.js", {
    expected_credentials = base64encode("bikeinfra:${random_password.non_prod_auth[0].result}")
    realm                = "bikeinfra-${var.environment}"
  })
}

resource "aws_cloudfront_response_headers_policy" "non_prod_noindex" {
  count = var.lockdown_non_prod ? 1 : 0
  name  = "${var.basename}-${var.environment}-non-prod-noindex"

  custom_headers_config {
    items {
      header   = "X-Robots-Tag"
      value    = "noindex, nofollow"
      override = true
    }
  }
}

locals {
  basic_auth_function_arn    = var.lockdown_non_prod ? aws_cloudfront_function.basic_auth[0].arn : null
  response_headers_policy_id = var.lockdown_non_prod ? aws_cloudfront_response_headers_policy.non_prod_noindex[0].id : null
}
