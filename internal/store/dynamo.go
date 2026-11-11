package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/SOAT-15-Oficina/oficina-mecanica-execution-service/internal/config"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

var ErrTableNotActive = errors.New("dynamodb table is not active")

type TableDescriber interface {
	DescribeTable(ctx context.Context, in *dynamodb.DescribeTableInput, opts ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error)
}

type Store struct {
	client TableDescriber
	table  string
}

func New(client TableDescriber, table string) *Store {
	return &Store{client: client, table: table}
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
