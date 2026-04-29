# /loci_platform/infra/modules/bike-map/versions.tf
terraform {
  required_providers {
    aws = {
      source                = "hashicorp/aws"
      configuration_aliases = [aws, aws.us_east_1, aws.dns]
    }
    archive = {
      source = "hashicorp/archive"
    }
  }
}
