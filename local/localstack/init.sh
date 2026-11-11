#!/bin/sh
set -eu

awslocal sqs create-queue --queue-name execution-inbox-dlq.fifo \
  --attributes FifoQueue=true,ContentBasedDeduplication=false

DLQ_ARN=$(awslocal sqs get-queue-attributes \
  --queue-url http://localhost:4566/000000000000/execution-inbox-dlq.fifo \
  --attribute-names QueueArn --query Attributes.QueueArn --output text)

awslocal sqs create-queue --queue-name execution-inbox.fifo \
  --attributes "{\"FifoQueue\":\"true\",\"ContentBasedDeduplication\":\"false\",\"RedrivePolicy\":\"{\\\"deadLetterTargetArn\\\":\\\"${DLQ_ARN}\\\",\\\"maxReceiveCount\\\":\\\"5\\\"}\"}"

awslocal sns create-topic --name execution-events.fifo \
  --attributes FifoTopic=true,ContentBasedDeduplication=false

awslocal dynamodb create-table --table-name execution-tickets \
  --billing-mode PAY_PER_REQUEST \
  --attribute-definitions AttributeName=PK,AttributeType=S AttributeName=SK,AttributeType=S \
    AttributeName=GSI1PK,AttributeType=S AttributeName=GSI1SK,AttributeType=S \
  --key-schema AttributeName=PK,KeyType=HASH AttributeName=SK,KeyType=RANGE \
  --global-secondary-indexes 'IndexName=queue,KeySchema=[{AttributeName=GSI1PK,KeyType=HASH},{AttributeName=GSI1SK,KeyType=RANGE}],Projection={ProjectionType=ALL}'

awslocal dynamodb update-time-to-live --table-name execution-tickets \
  --time-to-live-specification Enabled=true,AttributeName=expires_at
