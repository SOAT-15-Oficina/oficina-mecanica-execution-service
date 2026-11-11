locals {
  service    = "execution-service"
  name       = "oficina-${var.environment}-execution"
  ssm_prefix = "/oficina-mecanica/${var.environment}"
}

data "aws_caller_identity" "current" {}

data "aws_ssm_parameter" "platform" {
  for_each = toset([
    "vpc_id",
    "private_subnet_ids",
    "kube_namespace",
    "eks_oidc_provider_arn",
    "alb_listener_arn",
  ])

  name = "${local.ssm_prefix}/${each.key}"
}

locals {
  vpc_id              = data.aws_ssm_parameter.platform["vpc_id"].value
  private_subnet_ids  = split(",", data.aws_ssm_parameter.platform["private_subnet_ids"].value)
  kube_namespace      = data.aws_ssm_parameter.platform["kube_namespace"].value
  oidc_provider_arn   = data.aws_ssm_parameter.platform["eks_oidc_provider_arn"].value
  oidc_provider_host  = element(split("oidc-provider/", local.oidc_provider_arn), 1)
  node_security_group = data.aws_ssm_parameter.platform["eks_node_security_group_id"].value
  alb_listener_arn    = data.aws_ssm_parameter.platform["alb_listener_arn"].value
}
