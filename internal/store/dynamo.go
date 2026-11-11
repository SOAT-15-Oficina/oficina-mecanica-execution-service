package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/SOAT-15-Oficina/oficina-mecanica-execution-service/internal/config"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

var ErrTableNotActive = errors.New("dynamodb table is not active")

const (
	QueueIndex   = "queue"
	MessageTTL   = 7 * 24 * time.Hour
	OutboxTTL    = 7 * 24 * time.Hour
	keyTimLayout = "2006-01-02T15:04:05.000000000Z"
)

type API interface {
	DescribeTable(ctx context.Context, in *dynamodb.DescribeTableInput, opts ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error)
	GetItem(ctx context.Context, in *dynamodb.GetItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error)
	PutItem(ctx context.Context, in *dynamodb.PutItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error)
	UpdateItem(ctx context.Context, in *dynamodb.UpdateItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error)
	Query(ctx context.Context, in *dynamodb.QueryInput, opts ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error)
	TransactWriteItems(ctx context.Context, in *dynamodb.TransactWriteItemsInput, opts ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error)
}

type Store struct {
	client API
	table  string
	now    func() time.Time
}

func New(client API, table string) *Store {
	return &Store{client: client, table: table, now: func() time.Time { return time.Now().UTC() }}
}

func NewClient(ctx context.Context, cfg *config.AWSConfig) (*dynamodb.Client, error) {
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(cfg.Region))
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}

	return dynamodb.NewFromConfig(awsCfg, func(o *dynamodb.Options) {
		if cfg.Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
		}
	}), nil
}

func (s *Store) Table() string { return s.table }

func (s *Store) Ping(ctx context.Context) error {
	out, err := s.client.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(s.table)})
	if err != nil {
		return fmt.Errorf("describe table %s: %w", s.table, err)
	}
	if out.Table == nil || out.Table.TableStatus != types.TableStatusActive {
		return ErrTableNotActive
	}
	return nil
}

func keyTime(t time.Time) string { return t.UTC().Format(keyTimLayout) }

func key(pk, sk string) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{
		"PK": &types.AttributeValueMemberS{Value: pk},
		"SK": &types.AttributeValueMemberS{Value: sk},
	}
}

func str(v string) types.AttributeValue { return &types.AttributeValueMemberS{Value: v} }

func (s *Store) queryAll(ctx context.Context, in *dynamodb.QueryInput) ([]map[string]types.AttributeValue, error) {
	in.TableName = aws.String(s.table)
	var items []map[string]types.AttributeValue
	for {
		out, err := s.client.Query(ctx, in)
		if err != nil {
			return nil, fmt.Errorf("query %s: %w", s.table, err)
		}
		items = append(items, out.Items...)
		if len(out.LastEvaluatedKey) == 0 {
			return items, nil
		}
		in.ExclusiveStartKey = out.LastEvaluatedKey
	}
}
