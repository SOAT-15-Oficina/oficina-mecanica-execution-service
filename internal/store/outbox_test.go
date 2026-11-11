package store

import (
	"context"
	"errors"
	"testing"

	"github.com/SOAT-15-Oficina/oficina-mecanica-execution-service/internal/messaging"
	"github.com/SOAT-15-Oficina/oficina-mecanica-execution-service/internal/ticket"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFetchPendingDecodesOutbox(t *testing.T) {
	env, err := messaging.New(messaging.TypeDiagnosisStarted, messaging.Envelope{SagaID: uuid.New(), WorkOrderID: uuid.New()}, messaging.DiagnosisStarted{Technician: "ana"})
	require.NoError(t, err)
	rec, err := toOutbox(messaging.OutboxMessage{ID: env.MessageID, Destination: "dest", Envelope: env, Carrier: messaging.Carrier{"k": "v"}}, fixedNow)
	require.NoError(t, err)

	stub := &stubAPI{pages: []*dynamodb.QueryOutput{{Items: []map[string]types.AttributeValue{row(rec)}}}}
	got, err := newStubStore(stub).FetchPending(context.Background(), 10)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, env.MessageID, got[0].ID)
	assert.Equal(t, "dest", got[0].Destination)
	assert.Equal(t, env.Type, got[0].Envelope.Type)
	assert.Equal(t, "v", got[0].Carrier["k"])
	assert.Equal(t, int32(10), aws.ToInt32(stub.queries[0].Limit))
	assert.Equal(t, outboxQueue, stub.queries[0].ExpressionAttributeValues[":pk"].(*types.AttributeValueMemberS).Value)
}

func TestFetchPendingErrors(t *testing.T) {
	boom := errors.New("boom")
	_, err := newStubStore(&stubAPI{err: boom}).FetchPending(context.Background(), 1)
	assert.ErrorIs(t, err, boom)

	cases := []map[string]types.AttributeValue{
		{"created_at": &types.AttributeValueMemberN{Value: "1"}},
		{"id": str("nope")},
		{"id": str(uuid.NewString()), "envelope": str("{")},
	}
	for _, c := range cases {
		_, err := newStubStore(&stubAPI{pages: []*dynamodb.QueryOutput{{Items: []map[string]types.AttributeValue{c}}}}).FetchPending(context.Background(), 1)
		assert.Error(t, err)
	}
}

func TestMarkPublishedRemovesFromPendingIndex(t *testing.T) {
	stub := &stubAPI{}
	id := uuid.New()
	require.NoError(t, newStubStore(stub).MarkPublished(context.Background(), id))
	require.Len(t, stub.updates, 1)
	in := stub.updates[0]
	assert.Equal(t, "OUTBOX#"+id.String(), in.Key["PK"].(*types.AttributeValueMemberS).Value)
	assert.Contains(t, aws.ToString(in.UpdateExpression), "REMOVE GSI1PK, GSI1SK")
	assert.Equal(t, "attribute_exists(PK)", aws.ToString(in.ConditionExpression))

	assert.Error(t, newStubStore(&stubAPI{err: errors.New("boom")}).MarkPublished(context.Background(), id))
}

func TestBay(t *testing.T) {
	bay, err := newStubStore(&stubAPI{}).GetBay(context.Background(), "MATRIZ")
	require.NoError(t, err)
	assert.Equal(t, ticket.Bay{Branch: "MATRIZ", Available: true}, bay)

	stub := &stubAPI{}
	s := newStubStore(stub)
	require.NoError(t, s.SaveBay(context.Background(), ticket.Bay{Branch: "FILIAL", Available: false, UpdatedAt: &fixedNow, UpdatedBy: "ana"}))
	require.Len(t, stub.puts, 1)
	var rec bayRecord
	require.NoError(t, attributevalue.UnmarshalMap(stub.puts[0].Item, &rec))
	assert.Equal(t, "BAY#FILIAL", rec.PK)
	assert.False(t, rec.Available)

	stub.getOut = &dynamodb.GetItemOutput{Item: stub.puts[0].Item}
	bay, err = s.GetBay(context.Background(), "FILIAL")
	require.NoError(t, err)
	assert.False(t, bay.Available)
	assert.Equal(t, "ana", bay.UpdatedBy)

	stub.getOut = &dynamodb.GetItemOutput{Item: map[string]types.AttributeValue{"available": str("x")}}
	_, err = s.GetBay(context.Background(), "FILIAL")
	assert.Error(t, err)

	failing := newStubStore(&stubAPI{err: errors.New("boom")})
	_, err = failing.GetBay(context.Background(), "MATRIZ")
	assert.Error(t, err)
	assert.Error(t, failing.SaveBay(context.Background(), ticket.Bay{Branch: "MATRIZ"}))
}
