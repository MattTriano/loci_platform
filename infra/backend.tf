terraform {
  backend "s3" {
    bucket         = "${var.basename}-${var.environment}-state-storage"
    key            = "backend/${var.environment}/state.tfstate"
    region         = "us-east-2"
    encrypt        = true
    dynamodb_table = "${var.basename}-${var.environment}-state-locking"
  }
}
