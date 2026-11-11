package execution

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/SOAT-15-Oficina/oficina-mecanica-execution-service/internal/config"
	"github.com/SOAT-15-Oficina/oficina-mecanica-execution-service/internal/messaging"
	"github.com/SOAT-15-Oficina/oficina-mecanica-execution-service/internal/store"
	"github.com/SOAT-15-Oficina/oficina-mecanica-execution-service/internal/ticket"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type stack struct {
	repo     *store.Store
	sqs      *sqs.Client
	inbox    string
	observer string
	topic    string
}

func newStack(t *testing.T) stack {
	t.Helper()
	endpoint := os.Getenv("AWS_ENDPOINT_URL")
	if endpoint == "" {
		t.Skip("AWS_ENDPOINT_URL not set; skipping LocalStack integration test")
	}
	if os.Getenv("AWS_ACCESS_KEY_ID") == "" {
		t.Setenv("AWS_ACCESS_KEY_ID", "test")
		t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	}
	ctx := context.Background()
	suffix := uuid.NewString()[:8]

	db, err := store.NewClient(ctx, &config.AWSConfig{Region: "sa-east-1", Endpoint: endpoint})
	require.NoError(t, err)
	table := "execution-e2e-" + suffix
	_, err = db.CreateTable(ctx, &dynamodb.CreateTableInput{
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
			IndexName: aws.String(store.QueueIndex),
			KeySchema: []types.KeySchemaElement{
				{AttributeName: aws.String("GSI1PK"), KeyType: types.KeyTypeHash},
				{AttributeName: aws.String("GSI1SK"), KeyType: types.KeyTypeRange},
			},
			Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
		}},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = db.DeleteTable(context.Background(), &dynamodb.DeleteTableInput{TableName: aws.String(table)})
	})

	sqsClient, snsClient, err := messaging.NewClients(ctx, "sa-east-1", endpoint)
	require.NoError(t, err)

	fifo := map[string]string{"FifoQueue": "true", "ContentBasedDeduplication": "false"}
	inbox, err := sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("inbox-" + suffix + ".fifo"), Attributes: fifo})
	require.NoError(t, err)
	observer, err := sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("observer-" + suffix + ".fifo"), Attributes: fifo})
	require.NoError(t, err)
	topic, err := snsClient.CreateTopic(ctx, &sns.CreateTopicInput{
		Name:       aws.String("events-" + suffix + ".fifo"),
		Attributes: map[string]string{"FifoTopic": "true", "ContentBasedDeduplication": "false"},
	})
	require.NoError(t, err)
	attrs, err := sqsClient.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{QueueUrl: observer.QueueUrl, AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameQueueArn}})
	require.NoError(t, err)
	_, err = snsClient.Subscribe(ctx, &sns.SubscribeInput{
		TopicArn:   topic.TopicArn,
		Protocol:   aws.String("sqs"),
		Endpoint:   aws.String(attrs.Attributes["QueueArn"]),
		Attributes: map[string]string{"RawMessageDelivery": "true"},
	})
	require.NoError(t, err)

	repo := store.New(db, table)
	require.Eventually(t, func() bool { return repo.Ping(ctx) == nil }, 20*time.Second, 200*time.Millisecond)
	return stack{repo: repo, sqs: sqsClient, inbox: *inbox.QueueUrl, observer: *observer.QueueUrl, topic: *topic.TopicArn}
}

func (s stack) observed(t *testing.T) []messaging.Envelope {
	t.Helper()
	out, err := s.sqs.ReceiveMessage(context.Background(), &sqs.ReceiveMessageInput{QueueUrl: aws.String(s.observer), MaxNumberOfMessages: 10, WaitTimeSeconds: 1})
	require.NoError(t, err)
	var envs []messaging.Envelope
	for _, m := range out.Messages {
		var env messaging.Envelope
		require.NoError(t, json.Unmarshal([]byte(aws.ToString(m.Body)), &env))
		envs = append(envs, env)
		_, err := s.sqs.DeleteMessage(context.Background(), &sqs.DeleteMessageInput{QueueUrl: aws.String(s.observer), ReceiptHandle: m.ReceiptHandle})
		require.NoError(t, err)
	}
	return envs
}

func TestIntegrationSagaRoundTrip(t *testing.T) {
	st := newStack(t)
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	svc := New(st.repo, st.topic, logger)
	publisher := messaging.NewAWSPublisher(st.sqs, nil)
	consumer := messaging.NewConsumer(st.sqs, st.inbox, time.Second, svc.HandleMessage, logger)

	wo := uuid.New()
	cmd := command(messaging.TypeScheduleDiagnosis, wo, messaging.ScheduleDiagnosis{Branch: "MATRIZ", WorkOrderCode: "OS-9", Title: "Ruido"})
	require.NoError(t, publisher.Publish(ctx, st.inbox, cmd, nil))
	require.NoError(t, consumer.Poll(ctx))

	tk, err := svc.Ticket(ctx, wo)
	require.NoError(t, err)
	assert.Equal(t, ticket.StatusDiagnosisQueued, tk.Status)

	_, snsClient, err := messaging.NewClients(ctx, "sa-east-1", os.Getenv("AWS_ENDPOINT_URL"))
	require.NoError(t, err)
	relay := messaging.NewRelay(st.repo, messaging.NewAWSPublisher(st.sqs, snsClient), time.Second, logger)
	n, err := relay.Flush(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, n)

	envs := st.observed(t)
	require.Len(t, envs, 1)
	assert.Equal(t, messaging.TypeDiagnosisScheduled, envs[0].Type)
	assert.Equal(t, cmd.MessageID, *envs[0].CausationID)
	var p messaging.DiagnosisScheduled
	require.NoError(t, envs[0].DecodePayload(&p))
	assert.Equal(t, 1, p.QueuePosition)

	_, err = svc.StartDiagnosis(ctx, wo, "ana", "ana")
	require.NoError(t, err)
	_, err = svc.StartDiagnosis(ctx, wo, "ana", "ana")
	assert.ErrorIs(t, err, ticket.ErrInvalidTransition)
	_, err = svc.CompleteDiagnosis(ctx, wo, "ok", "ana")
	require.NoError(t, err)

	_, err = svc.SetBayAvailability(ctx, "MATRIZ", false, "ana")
	require.NoError(t, err)
	require.NoError(t, publisher.Publish(ctx, st.inbox, command(messaging.TypeEnqueueRepair, wo, messaging.EnqueueRepair{Branch: "MATRIZ"}), nil))
	require.NoError(t, consumer.Poll(ctx))

	_, err = svc.SetBayAvailability(ctx, "MATRIZ", true, "ana")
	require.NoError(t, err)
	item := uuid.New()
	require.NoError(t, publisher.Publish(ctx, st.inbox, command(messaging.TypeEnqueueRepair, wo, messaging.EnqueueRepair{Branch: "MATRIZ", Items: []messaging.RepairItem{{WorkOrderServiceID: item, Title: "Freio"}}}), nil))
	require.NoError(t, consumer.Poll(ctx))

	_, err = svc.StartRepair(ctx, wo, "bia", "bia")
	require.NoError(t, err)
	_, err = svc.StartItem(ctx, wo, item, "bia")
	require.NoError(t, err)
	tk, err = svc.FinishItem(ctx, wo, item, "bia")
	require.NoError(t, err)
	assert.Equal(t, ticket.StatusCompleted, tk.Status)

	n, err = relay.Flush(ctx)
	require.NoError(t, err)
	assert.Equal(t, 8, n)

	var got []string
	for range 5 {
		for _, env := range st.observed(t) {
			got = append(got, env.Type)
		}
		if len(got) >= 8 {
			break
		}
	}
	assert.Equal(t, []string{
		messaging.TypeDiagnosisStarted,
		messaging.TypeDiagnosisCompleted,
		messaging.TypeRepairRejected,
		messaging.TypeRepairEnqueued,
		messaging.TypeRepairStarted,
		messaging.TypeItemStarted,
		messaging.TypeItemFinished,
		messaging.TypeRepairCompleted,
	}, got)

	n, err = relay.Flush(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, n)

	stored, err := svc.Ticket(ctx, wo)
	require.NoError(t, err)
	assert.Len(t, stored.Timeline, 8)
}
