provider "aws" {
  region = "us-east-2"
}

module "state" {
  source  = "../../modules/state"

  basename = "loci-infra"
  environment = "dev"
}
