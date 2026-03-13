resource "aws_route53_zone" "env" {
  name = "${var.environment}.${var.base_domain}"
}
