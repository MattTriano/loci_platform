# loci_platform/infra/modules/bike-map-landing/versions.tf
terraform {
  required_providers {
    aws = {
      source                = "hashicorp/aws"
      configuration_aliases = [aws.us_east_1, aws.dns]
    }
    random = {
      source = "hashicorp/random"
    }
  }
}
