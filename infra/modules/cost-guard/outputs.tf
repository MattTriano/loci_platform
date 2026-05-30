# modules/cost-guard/outputs.tf

output "kill_switch_lambda_arn" {
  description = "ARN of the kill-switch Lambda. Useful for manual invocation during testing."
  value       = aws_lambda_function.kill_switch.arn
}

output "kill_switch_sns_topic_arn" {
  description = "ARN of the kill-switch SNS topic. Useful for manual publication during testing."
  value       = aws_sns_topic.kill_switch.arn
}

output "budget_name" {
  description = "Name of the AWS Budget."
  value       = aws_budgets_budget.account.name
}
