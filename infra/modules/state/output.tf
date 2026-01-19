output "state_bucket_id" {
  description = "The id of the S3 bucket storing the tfstate."
  value = aws_s3_bucket.tofu_state.id
}

output "state_lock_table_id" {
  description = "The id of the DynamoDB table locking the tfstate."
  value = aws_dynamodb_table.tofu_locks.id
}
