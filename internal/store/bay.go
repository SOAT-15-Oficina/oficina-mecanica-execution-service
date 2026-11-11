package store

import (
	"context"
	"fmt"

	"github.com/SOAT-15-Oficina/oficina-mecanica-execution-service/internal/ticket"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

func (s *Store) GetBay(ctx context.Context, branch string) (ticket.Bay, error) {
	out, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:      aws.String(s.table),
		Key:            key(bayPK(branch), skMeta),
		ConsistentRead: aws.Bool(true),
	})
	if err != nil {
		return ticket.Bay{}, fmt.Errorf("get bay %s: %w", branch, err)
	}
	if len(out.Item) == 0 {
		return ticket.Bay{Branch: branch, Available: true}, nil
	}
	var r bayRecord
	if err := attributevalue.UnmarshalMap(out.Item, &r); err != nil {
		return ticket.Bay{}, fmt.Errorf("decode bay: %w", err)
	}
	return ticket.Bay{Branch: r.Branch, Available: r.Available, UpdatedAt: r.UpdatedAt, UpdatedBy: r.UpdatedBy}, nil
}

func (s *Store) SaveBay(ctx context.Context, bay ticket.Bay) error {
	item, err := attributevalue.MarshalMap(bayRecord{
		PK:        bayPK(bay.Branch),
		SK:        skMeta,
		Branch:    bay.Branch,
		Available: bay.Available,
		UpdatedAt: bay.UpdatedAt,
		UpdatedBy: bay.UpdatedBy,
	})
	if err != nil {
		return fmt.Errorf("encode bay: %w", err)
	}
	if _, err := s.client.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String(s.table), Item: item}); err != nil {
		return fmt.Errorf("put bay %s: %w", bay.Branch, err)
	}
	return nil
}
