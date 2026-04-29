# /loci_platform/infra/modules/dns-zone/main.tf
resource "aws_route53_zone" "env" {
  name = "${var.environment}.${var.base_domain}"
}
