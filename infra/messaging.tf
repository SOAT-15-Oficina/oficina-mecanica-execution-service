resource "aws_sqs_queue" "inbox_dlq" {
  name                      = "${local.name}-inbox-dlq.fifo"
  fifo_queue                = true
  message_retention_seconds = 1209600
  sqs_managed_sse_enabled   = true
}

resource "aws_sqs_queue" "inbox" {
  name                        = "${local.name}-inbox.fifo"
  fifo_queue                  = true
  content_based_deduplication = false
  deduplication_scope         = "messageGroup"
  fifo_throughput_limit       = "perMessageGroupId"
  visibility_timeout_seconds  = 60
  receive_wait_time_seconds   = 20
  sqs_managed_sse_enabled     = true

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.inbox_dlq.arn
    maxReceiveCount     = 5
  })
}

resource "aws_sns_topic" "events" {
  name                        = "${local.name}-events.fifo"
  fifo_topic                  = true
  content_based_deduplication = false
}
