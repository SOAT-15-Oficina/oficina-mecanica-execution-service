locals {
  published = {
    execution_inbox_queue_url  = aws_sqs_queue.inbox.url
    execution_inbox_queue_arn  = aws_sqs_queue.inbox.arn
    execution_events_topic_arn = aws_sns_topic.events.arn
    execution_ecr_repository   = aws_ecr_repository.service.repository_url
    execution_table_name       = aws_dynamodb_table.tickets.name
    execution_role_arn         = aws_iam_role.service.arn
    execution_target_group_arn = aws_lb_target_group.service.arn
  }
}

resource "aws_ssm_parameter" "published" {
  for_each = local.published

  name      = "${local.ssm_prefix}/${each.key}"
  type      = "String"
  value     = each.value
  overwrite = true
}

output "published" {
  value = local.published
}
