package store

import (
	"context"
	"errors"
	"testing"
	"time"

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

func sampleTicket() *ticket.Ticket {
	env := messaging.Envelope{MessageID: uuid.New(), SagaID: uuid.New(), WorkOrderID: uuid.New(), CorrelationID: "req"}
	return ticket.NewDiagnosis(env, messaging.ScheduleDiagnosis{
		Branch: "MATRIZ", WorkOrderCode: "OS-1", Title: "Revisao",
		Vehicle: messaging.Vehicle{LicensePlate: "ABC1D23", Brand: "Fiat", Model: "Uno", Year: 2010},
	}, fixedNow)
}

func TestCommitCreatesTicketWithMessageEventsAndOutbox(t *testing.T) {
	stub := &stubAPI{}
	s := newStubStore(stub)
	tk := sampleTicket()
	tk.Items = []ticket.Item{{WorkOrderServiceID: uuid.New(), Title: "A", Status: ticket.ItemPending}}
	inbound := messaging.Envelope{MessageID: uuid.New(), Type: messaging.TypeScheduleDiagnosis, WorkOrderID: tk.WorkOrderID}
	reply, err := messaging.New(messaging.TypeDiagnosisScheduled, inbound, messaging.DiagnosisScheduled{QueuePosition: 1})
	require.NoError(t, err)

	err = s.Commit(context.Background(), ticket.Change{
		Inbound:          &inbound,
		Ticket:           tk,
		Create:           true,
		Items:            tk.Items,
		Events:           []ticket.Event{{At: fixedNow, Type: "SCHEDULED", To: tk.Status, MessageID: &inbound.MessageID, WorkOrderServiceID: &tk.Items[0].WorkOrderServiceID}},
		Outbox:           []messaging.OutboxMessage{{ID: reply.MessageID, Destination: "arn:aws:sns:x", Envelope: reply, Carrier: messaging.Carrier{"a": "b"}}},
		RequireBayBranch: "MATRIZ",
	})
	require.NoError(t, err)
	assert.Equal(t, 1, tk.Version)

	require.Len(t, stub.transact, 1)
	items := stub.transact[0].TransactItems
	require.Len(t, items, 6)

	assert.Equal(t, "attribute_not_exists(PK)", aws.ToString(items[0].Put.ConditionExpression))
	var msg messageRecord
	require.NoError(t, attributevalue.UnmarshalMap(items[0].Put.Item, &msg))
	assert.Equal(t, "MSG#"+inbound.MessageID.String(), msg.PK)
	assert.Equal(t, fixedNow.Add(MessageTTL).Unix(), msg.ExpiresAt)

	assert.Equal(t, "attribute_not_exists(PK)", aws.ToString(items[1].Put.ConditionExpression))
	var meta metaRecord
	require.NoError(t, attributevalue.UnmarshalMap(items[1].Put.Item, &meta))
	assert.Equal(t, "TICKET#"+tk.WorkOrderID.String(), meta.PK)
	assert.Equal(t, "QUEUE#MATRIZ#DIAGNOSTICO", meta.GSI1PK)
	assert.Equal(t, QueueKey(tk), meta.GSI1SK)
	assert.Equal(t, 1, meta.Version)

	var item itemRecord
	require.NoError(t, attributevalue.UnmarshalMap(items[2].Put.Item, &item))
	assert.Equal(t, "ITEM#"+tk.Items[0].WorkOrderServiceID.String(), item.SK)
	assert.Nil(t, items[2].Put.ConditionExpression)

	var event eventRecord
	require.NoError(t, attributevalue.UnmarshalMap(items[3].Put.Item, &event))
	assert.Regexp(t, `^EVENT#2026-11-11T10:00:00\.000000000Z#000#[0-9a-f]{8}$`, event.SK)

	assert.NotNil(t, items[4].ConditionCheck)
	assert.Equal(t, "BAY#MATRIZ", items[4].ConditionCheck.Key["PK"].(*types.AttributeValueMemberS).Value)

	var out outboxRecord
	require.NoError(t, attributevalue.UnmarshalMap(items[5].Put.Item, &out))
	assert.Equal(t, "OUTBOX#PENDING", out.GSI1PK)
	assert.Equal(t, "2026-11-11T10:00:00.000000000Z#"+reply.MessageID.String(), out.GSI1SK)
	assert.Equal(t, "arn:aws:sns:x", out.Destination)
}

func TestCommitUpdateUsesVersionCondition(t *testing.T) {
	stub := &stubAPI{}
	s := newStubStore(stub)
	tk := sampleTicket()
	tk.Version = 4
	require.NoError(t, tk.StartDiagnosis("ana", fixedNow))

	require.NoError(t, s.Commit(context.Background(), ticket.Change{Ticket: tk}))
	put := stub.transact[0].TransactItems[0].Put
	assert.Equal(t, "#version = :version", aws.ToString(put.ConditionExpression))
	assert.Equal(t, "4", put.ExpressionAttributeValues[":version"].(*types.AttributeValueMemberN).Value)
	_, queued := put.Item["GSI1PK"]
	assert.False(t, queued)
	assert.Equal(t, 5, tk.Version)
}

func TestCommitWithoutActionsIsNoop(t *testing.T) {
	stub := &stubAPI{}
	require.NoError(t, newStubStore(stub).Commit(context.Background(), ticket.Change{}))
	assert.Empty(t, stub.transact)
}

func canceled(codes ...string) error {
	reasons := make([]types.CancellationReason, len(codes))
	for i, c := range codes {
		reasons[i] = types.CancellationReason{Code: aws.String(c)}
	}
	return &types.TransactionCanceledException{CancellationReasons: reasons}
}

func TestCommitTranslatesConditionFailures(t *testing.T) {
	inbound := messaging.Envelope{MessageID: uuid.New(), WorkOrderID: uuid.New()}
	tests := []struct {
		name string
		err  error
		want error
	}{
		{"duplicate message", canceled("ConditionalCheckFailed", "None", "None"), ticket.ErrDuplicateMessage},
		{"ticket conflict", canceled("None", "ConditionalCheckFailed", "None"), ticket.ErrConflict},
		{"bay unavailable", canceled("None", "None", "ConditionalCheckFailed"), ticket.ErrBayUnavailable},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tk := sampleTicket()
			stub := &stubAPI{err: tt.err}
			err := newStubStore(stub).Commit(context.Background(), ticket.Change{Inbound: &inbound, Ticket: tk, Create: true, RequireBayBranch: "MATRIZ"})
			assert.ErrorIs(t, err, tt.want)
			assert.Equal(t, 0, tk.Version)
		})
	}

	boom := errors.New("boom")
	err := newStubStore(&stubAPI{err: boom}).Commit(context.Background(), ticket.Change{Inbound: &inbound})
	assert.ErrorIs(t, err, boom)

	err = newStubStore(&stubAPI{err: canceled("TransactionConflict")}).Commit(context.Background(), ticket.Change{Inbound: &inbound})
	var tce *types.TransactionCanceledException
	assert.ErrorAs(t, err, &tce)
}

func TestGetTicketAssemblesMetaItemsAndTimeline(t *testing.T) {
	tk := sampleTicket()
	first, second := uuid.New(), uuid.New()
	tk.Items = []ticket.Item{{WorkOrderServiceID: first, Title: "A"}, {WorkOrderServiceID: second, Title: "B"}}
	msg := uuid.New()
	stub := &stubAPI{pages: []*dynamodb.QueryOutput{
		{Items: []map[string]types.AttributeValue{
			row(toMeta(tk, 3)),
			row(toItem(tk.WorkOrderID, 1, tk.Items[1])),
		}, LastEvaluatedKey: more()},
		{Items: []map[string]types.AttributeValue{
			row(toItem(tk.WorkOrderID, 0, tk.Items[0])),
			row(toEvent(tk.WorkOrderID, 0, ticket.Event{At: fixedNow, Type: "X", MessageID: &msg, WorkOrderServiceID: &first})),
			{"PK": str("x")},
			{"PK": str("x"), "SK": str("OTHER")},
		}},
	}}

	got, err := newStubStore(stub).GetTicket(context.Background(), tk.WorkOrderID)
	require.NoError(t, err)
	assert.Equal(t, 3, got.Version)
	assert.Equal(t, tk.SagaID, got.SagaID)
	assert.Equal(t, "ABC1D23", got.Vehicle.LicensePlate)
	require.Len(t, got.Items, 2)
	assert.Equal(t, first, got.Items[0].WorkOrderServiceID)
	require.Len(t, got.Timeline, 1)
	assert.Equal(t, msg, *got.Timeline[0].MessageID)
	assert.Equal(t, first, *got.Timeline[0].WorkOrderServiceID)
	require.Len(t, stub.queries, 2)
	assert.True(t, aws.ToBool(stub.queries[0].ConsistentRead))
	assert.NotNil(t, stub.queries[1].ExclusiveStartKey)
}

func TestGetTicketErrors(t *testing.T) {
	_, err := newStubStore(&stubAPI{}).GetTicket(context.Background(), uuid.New())
	assert.ErrorIs(t, err, ticket.ErrNotFound)

	boom := errors.New("boom")
	_, err = newStubStore(&stubAPI{err: boom}).GetTicket(context.Background(), uuid.New())
	assert.ErrorIs(t, err, boom)

	bad := []map[string]types.AttributeValue{{"PK": str("TICKET#x"), "SK": str("META"), "work_order_id": str("nope")}}
	_, err = newStubStore(&stubAPI{pages: []*dynamodb.QueryOutput{{Items: bad}}}).GetTicket(context.Background(), uuid.New())
	assert.Error(t, err)

	for _, sk := range []string{"META", "ITEM#1", "EVENT#1"} {
		wrong := []map[string]types.AttributeValue{{"PK": str("TICKET#x"), "SK": str(sk), "version": str("x"), "position": str("x"), "at": &types.AttributeValueMemberN{Value: "1"}}}
		_, err = newStubStore(&stubAPI{pages: []*dynamodb.QueryOutput{{Items: wrong}}}).GetTicket(context.Background(), uuid.New())
		assert.Error(t, err, sk)
	}
}

func TestListQueueAndPosition(t *testing.T) {
	a, b := sampleTicket(), sampleTicket()
	stub := &stubAPI{pages: []*dynamodb.QueryOutput{
		{Items: []map[string]types.AttributeValue{row(toMeta(a, 1))}, LastEvaluatedKey: more()},
		{Items: []map[string]types.AttributeValue{row(toMeta(b, 1))}},
		{Count: 2, LastEvaluatedKey: more()},
		{Count: 1},
	}}
	s := newStubStore(stub)

	queue, err := s.ListQueue(context.Background(), "MATRIZ", ticket.PhaseDiagnosis)
	require.NoError(t, err)
	require.Len(t, queue, 2)
	assert.Equal(t, a.WorkOrderID, queue[0].WorkOrderID)
	assert.Equal(t, QueueIndex, aws.ToString(stub.queries[0].IndexName))
	assert.Equal(t, "QUEUE#MATRIZ#DIAGNOSTICO", stub.queries[0].ExpressionAttributeValues[":pk"].(*types.AttributeValueMemberS).Value)

	pos, err := s.Position(context.Background(), b)
	require.NoError(t, err)
	assert.Equal(t, 4, pos)
	assert.Equal(t, types.SelectCount, stub.queries[2].Select)
	assert.Equal(t, QueueKey(b), stub.queries[2].ExpressionAttributeValues[":sk"].(*types.AttributeValueMemberS).Value)

	boom := errors.New("boom")
	_, err = newStubStore(&stubAPI{err: boom}).ListQueue(context.Background(), "MATRIZ", ticket.PhaseRepair)
	assert.ErrorIs(t, err, boom)
	_, err = newStubStore(&stubAPI{err: boom}).Position(context.Background(), b)
	assert.ErrorIs(t, err, boom)

	bad := []map[string]types.AttributeValue{{"work_order_id": str("nope")}}
	_, err = newStubStore(&stubAPI{pages: []*dynamodb.QueryOutput{{Items: bad}}}).ListQueue(context.Background(), "MATRIZ", ticket.PhaseRepair)
	assert.Error(t, err)
	wrong := []map[string]types.AttributeValue{{"version": str("x")}}
	_, err = newStubStore(&stubAPI{pages: []*dynamodb.QueryOutput{{Items: wrong}}}).ListQueue(context.Background(), "MATRIZ", ticket.PhaseRepair)
	assert.Error(t, err)
}

func TestQueueKeyFallsBackToCreation(t *testing.T) {
	tk := sampleTicket()
	tk.EnqueuedAt = nil
	assert.Contains(t, QueueKey(tk), keyTime(tk.CreatedAt))
	assert.Equal(t, "2026-11-11T10:00:00.000000001Z", keyTime(fixedNow.Add(time.Nanosecond)))
}

func TestIsProcessed(t *testing.T) {
	stub := &stubAPI{getOut: &dynamodb.GetItemOutput{Item: map[string]types.AttributeValue{"PK": str("MSG#x")}}}
	id := uuid.New()
	ok, err := newStubStore(stub).IsProcessed(context.Background(), id)
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, "MSG#"+id.String(), stub.gets[0].Key["PK"].(*types.AttributeValueMemberS).Value)

	ok, err = newStubStore(&stubAPI{}).IsProcessed(context.Background(), id)
	require.NoError(t, err)
	assert.False(t, ok)

	_, err = newStubStore(&stubAPI{err: errors.New("boom")}).IsProcessed(context.Background(), id)
	assert.Error(t, err)
}
