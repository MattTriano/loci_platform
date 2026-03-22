terraform {
  required_providers {
    aws = {
      source                = "hashicorp/aws"
      configuration_aliases = [aws, aws.us_east_1]
    }
    archive = {
      source = "hashicorp/archive"
    }
  }
}
