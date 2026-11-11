package store

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/SOAT-15-Oficina/oficina-mecanica-execution-service/internal/config"
	"github.com/SOAT-15-Oficina/oficina-mecanica-execution-service/internal/messaging"
	"github.com/SOAT-15-Oficina/oficina-mecanica-execution-service/internal/ticket"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func NewIntegrationTable(t *testing.T) *Store {
	t.Helper()
	endpoint := os.Getenv("AWS_ENDPOINT_URL")
	if endpoint == "" {
		t.Skip("AWS_ENDPOINT_URL not set; skipping DynamoDB integration test")
	}
	if os.Getenv("AWS_ACCESS_KEY_ID") == "" {
		t.Setenv("AWS_ACCESS_KEY_ID", "test")
		t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	}

	ctx := context.Background()
	client, err := NewClient(ctx, &config.AWSConfig{Region: "sa-east-1", Endpoint: endpoint})
	require.NoError(t, err)

	table := "execution-it-" + uuid.NewString()[:8]
	_, err = client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:   aws.String(table),
		BillingMode: types.BillingModePayPerRequest,
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("PK"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("SK"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("GSI1PK"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("GSI1SK"), AttributeType: types.ScalarAttributeTypeS},
		},
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("PK"), KeyType: types.KeyTypeHash},
			{AttributeName: aws.String("SK"), KeyType: types.KeyTypeRange},
		},
		GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{{
			IndexName: aws.String(QueueIndex),
			KeySchema: []types.KeySchemaElement{
				{AttributeName: aws.String("GSI1PK"), KeyType: types.KeyTypeHash},
				{AttributeName: aws.String("GSI1SK"), KeyType: types.KeyTypeRange},
			},
			Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
		}},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = client.DeleteTable(context.Background(), &dynamodb.DeleteTableInput{TableName: aws.String(table)})
	})

	s := New(client, table)
	require.Eventually(t, func() bool { return s.Ping(ctx) == nil }, 20*time.Second, 200*time.Millisecond)
	return s
}

func TestIntegrationTicketLifecycle(t *testing.T) {
	s := NewIntegrationTable(t)
	ctx := context.Background()

	first, second := sampleTicket(), sampleTicket()
	second.EnqueuedAt = ptr(first.EnqueuedAt.Add(time.Second))
	for _, tk := range []*ticket.Ticket{first, second} {
		inbound := messaging.Envelope{MessageID: uuid.New(), Type: messaging.TypeScheduleDiagnosis, WorkOrderID: tk.WorkOrderID}
		reply, err := messaging.New(messaging.TypeDiagnosisScheduled, inbound, messaging.DiagnosisScheduled{QueuePosition: 1})
		require.NoError(t, err)
		require.NoError(t, s.Commit(ctx, ticket.Change{
			Inbound: &inbound, Ticket: tk, Create: true,
			Events: []ticket.Event{{At: fixedNow, Type: "SCHEDULED", To: tk.Status}},
			Outbox: []messaging.OutboxMessage{{ID: reply.MessageID, Destination: "dest", Envelope: reply}},
		}))

		processed, err := s.IsProcessed(ctx, inbound.MessageID)
		require.NoError(t, err)
		assert.True(t, processed)

		err = s.Commit(ctx, ticket.Change{Inbound: &inbound})
		assert.ErrorIs(t, err, ticket.ErrDuplicateMessage)
	}

	queue, err := s.ListQueue(ctx, "MATRIZ", ticket.PhaseDiagnosis)
	require.NoError(t, err)
	require.Len(t, queue, 2)
	assert.Equal(t, first.WorkOrderID, queue[0].WorkOrderID)

	pos, err := s.Position(ctx, second)
	require.NoError(t, err)
	assert.Equal(t, 2, pos)

	pending, err := s.FetchPending(ctx, 10)
	require.NoError(t, err)
	require.Len(t, pending, 2)
	require.NoError(t, s.MarkPublished(ctx, pending[0].ID))
	pending, err = s.FetchPending(ctx, 10)
	require.NoError(t, err)
	assert.Len(t, pending, 1)

	stale, err := s.GetTicket(ctx, first.WorkOrderID)
	require.NoError(t, err)
	require.NoError(t, first.StartDiagnosis("ana", fixedNow))
	require.NoError(t, s.Commit(ctx, ticket.Change{Ticket: first, Events: []ticket.Event{{At: fixedNow, Type: "STARTED"}}}))
	assert.Equal(t, 2, first.Version)

	require.NoError(t, stale.Cancel("CLIENTE", fixedNow))
	assert.ErrorIs(t, s.Commit(ctx, ticket.Change{Ticket: stale}), ticket.ErrConflict)

	queue, err = s.ListQueue(ctx, "MATRIZ", ticket.PhaseDiagnosis)
	require.NoError(t, err)
	require.Len(t, queue, 1)
	assert.Equal(t, second.WorkOrderID, queue[0].WorkOrderID)

	got, err := s.GetTicket(ctx, first.WorkOrderID)
	require.NoError(t, err)
	assert.Equal(t, ticket.StatusDiagnosing, got.Status)
	assert.Equal(t, "ana", got.DiagnosisTechnician)
	assert.Len(t, got.Timeline, 2)
}

func TestIntegrationBayGuardsRepair(t *testing.T) {
	s := NewIntegrationTable(t)
	ctx := context.Background()

	tk := sampleTicket()
	require.NoError(t, s.Commit(ctx, ticket.Change{Ticket: tk, Create: true}))
	require.NoError(t, tk.StartDiagnosis("ana", fixedNow))
	require.NoError(t, tk.CompleteDiagnosis("", fixedNow))
	require.NoError(t, s.Commit(ctx, ticket.Change{Ticket: tk}))

	require.NoError(t, s.SaveBay(ctx, ticket.Bay{Branch: "MATRIZ", Available: false, UpdatedAt: &fixedNow}))
	bay, err := s.GetBay(ctx, "MATRIZ")
	require.NoError(t, err)
	assert.False(t, bay.Available)

	items := []messaging.RepairItem{{WorkOrderServiceID: uuid.New(), Title: "B"}, {WorkOrderServiceID: uuid.New(), Title: "A"}}
	repair := *tk
	require.NoError(t, repair.EnqueueRepair(items, fixedNow))
	err = s.Commit(ctx, ticket.Change{Ticket: &repair, Items: repair.Items, RequireBayBranch: "MATRIZ"})
	assert.ErrorIs(t, err, ticket.ErrBayUnavailable)

	require.NoError(t, s.SaveBay(ctx, ticket.Bay{Branch: "MATRIZ", Available: true, UpdatedAt: &fixedNow}))
	require.NoError(t, s.Commit(ctx, ticket.Change{Ticket: &repair, Items: repair.Items, RequireBayBranch: "MATRIZ"}))

	got, err := s.GetTicket(ctx, tk.WorkOrderID)
	require.NoError(t, err)
	assert.Equal(t, ticket.StatusRepairQueued, got.Status)
	require.Len(t, got.Items, 2)
	assert.Equal(t, "B", got.Items[0].Title)

	queue, err := s.ListQueue(ctx, "MATRIZ", ticket.PhaseRepair)
	require.NoError(t, err)
	assert.Len(t, queue, 1)
}

func ptr[T any](v T) *T { return &v }
