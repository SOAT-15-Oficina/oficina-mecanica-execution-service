package store

import (
	"context"
	"errors"
	"testing"

	"github.com/SOAT-15-Oficina/oficina-mecanica-execution-service/internal/config"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPing(t *testing.T) {
	tests := []struct {
		name    string
		stub    *stubAPI
		wantErr error
	}{
		{"active", &stubAPI{describeOut: &dynamodb.DescribeTableOutput{Table: &types.TableDescription{TableStatus: types.TableStatusActive}}}, nil},
		{"creating", &stubAPI{describeOut: &dynamodb.DescribeTableOutput{Table: &types.TableDescription{TableStatus: types.TableStatusCreating}}}, ErrTableNotActive},
		{"no description", &stubAPI{describeOut: &dynamodb.DescribeTableOutput{}}, ErrTableNotActive},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := New(tt.stub, "tickets")
			err := s.Ping(context.Background())
			if tt.wantErr == nil {
				require.NoError(t, err)
			} else {
				assert.ErrorIs(t, err, tt.wantErr)
			}
			assert.Equal(t, "tickets", tt.stub.describedAs)
			assert.Equal(t, "tickets", s.Table())
		})
	}
}

func TestPingWrapsClientError(t *testing.T) {
	boom := errors.New("boom")
	err := New(&stubAPI{err: boom}, "tickets").Ping(context.Background())
	assert.ErrorIs(t, err, boom)
}

func TestNewClientHonoursEndpoint(t *testing.T) {
	t.Setenv("AWS_ENDPOINT_URL", "")
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")

	client, err := NewClient(context.Background(), &config.AWSConfig{Region: "sa-east-1", Endpoint: "http://localhost:4566"})
	require.NoError(t, err)
	assert.Equal(t, "http://localhost:4566", aws.ToString(client.Options().BaseEndpoint))

	client, err = NewClient(context.Background(), &config.AWSConfig{Region: "sa-east-1"})
	require.NoError(t, err)
	assert.Nil(t, client.Options().BaseEndpoint)
}
