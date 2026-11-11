package store

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/SOAT-15-Oficina/oficina-mecanica-execution-service/internal/messaging"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/google/uuid"
)

func (s *Store) FetchPending(ctx context.Context, limit int) ([]messaging.OutboxMessage, error) {
	out, err := s.client.Query(ctx, &dynamodb.QueryInput{
		TableName:                 aws.String(s.table),
		IndexName:                 aws.String(QueueIndex),
		KeyConditionExpression:    aws.String("GSI1PK = :pk"),
		ExpressionAttributeValues: map[string]types.AttributeValue{":pk": str(outboxQueue)},
		ScanIndexForward:          aws.Bool(true),
		Limit:                     aws.Int32(int32(limit)),
	})
	if err != nil {
		return nil, fmt.Errorf("query outbox: %w", err)
	}

	pending := make([]messaging.OutboxMessage, 0, len(out.Items))
	for _, row := range out.Items {
		var r outboxRecord
		if err := attributevalue.UnmarshalMap(row, &r); err != nil {
			return nil, fmt.Errorf("decode outbox: %w", err)
		}
		id, err := uuid.Parse(r.ID)
		if err != nil {
			return nil, fmt.Errorf("outbox %s: %w", r.PK, err)
		}
		var env messaging.Envelope
		if err := json.Unmarshal([]byte(r.Envelope), &env); err != nil {
			return nil, fmt.Errorf("outbox %s envelope: %w", r.PK, err)
		}
		pending = append(pending, messaging.OutboxMessage{
			ID:          id,
			Destination: r.Destination,
			Envelope:    env,
			Carrier:     messaging.Carrier(r.Carrier),
		})
	}
	return pending, nil
}

func (s *Store) MarkPublished(ctx context.Context, id uuid.UUID) error {
	now := s.now()
	_, err := s.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:           aws.String(s.table),
		Key:                 key(outboxPK(id), skOutbox),
		UpdateExpression:    aws.String("REMOVE GSI1PK, GSI1SK SET published_at = :at, expires_at = :ttl"),
		ConditionExpression: aws.String("attribute_exists(PK)"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":at":  str(keyTime(now)),
			":ttl": &types.AttributeValueMemberN{Value: fmt.Sprint(now.Add(OutboxTTL).Unix())},
		},
	})
	if err != nil {
		return fmt.Errorf("mark outbox %s published: %w", id, err)
	}
	return nil
}

var _ messaging.OutboxStore = (*Store)(nil)
