# modules/waf/outputs.tf

output "web_acl_arn" {
  description = "ARN of the WebACL. CloudFront distributions reference this on their web_acl_id attribute."
  value       = aws_wafv2_web_acl.cloudfront.arn
}
