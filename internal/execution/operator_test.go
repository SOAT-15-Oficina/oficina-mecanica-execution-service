package execution

import (
	"context"
	"errors"
	"testing"

	"github.com/SOAT-15-Oficina/oficina-mecanica-execution-service/internal/messaging"
	"github.com/SOAT-15-Oficina/oficina-mecanica-execution-service/internal/ticket"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOperatorFlowPublishesEveryEvent(t *testing.T) {
	repo := newFake()
	s := newService(repo)
	ctx := context.Background()
	wo := uuid.New()
	sched := schedule(t, s, wo)

	tk, err := s.StartDiagnosis(ctx, wo, "ana", "user-ana")
	require.NoError(t, err)
	assert.Equal(t, ticket.StatusDiagnosing, tk.Status)
	assert.Equal(t, "user-ana", tk.Timeline[len(tk.Timeline)-1].Actor)
	started := repo.last().Envelope
	assert.Equal(t, sched.SagaID, started.SagaID)
	assert.Equal(t, "req-1", started.CorrelationID)
	assert.Nil(t, started.CausationID)
	var ds messaging.DiagnosisStarted
	require.NoError(t, started.DecodePayload(&ds))
	assert.Equal(t, "ana", ds.Technician)

	_, err = s.CompleteDiagnosis(ctx, wo, "pastilhas gastas", "user-ana")
	require.NoError(t, err)

	a, b := uuid.New(), uuid.New()
	require.NoError(t, s.HandleMessage(ctx, command(messaging.TypeEnqueueRepair, wo, messaging.EnqueueRepair{Items: []messaging.RepairItem{{WorkOrderServiceID: a}, {WorkOrderServiceID: b}}})))

	_, err = s.StartRepair(ctx, wo, "bia", "user-bia")
	require.NoError(t, err)
	_, err = s.StartItem(ctx, wo, a, "user-bia")
	require.NoError(t, err)
	tk, err = s.FinishItem(ctx, wo, a, "user-bia")
	require.NoError(t, err)
	assert.Equal(t, ticket.StatusRepairing, tk.Status)
	_, err = s.StartItem(ctx, wo, b, "user-bia")
	require.NoError(t, err)
	tk, err = s.FinishItem(ctx, wo, b, "user-bia")
	require.NoError(t, err)
	assert.Equal(t, ticket.StatusCompleted, tk.Status)

	assert.Equal(t, []string{
		messaging.TypeDiagnosisScheduled,
		messaging.TypeDiagnosisStarted,
		messaging.TypeDiagnosisCompleted,
		messaging.TypeRepairEnqueued,
		messaging.TypeRepairStarted,
		messaging.TypeItemStarted,
		messaging.TypeItemFinished,
		messaging.TypeItemStarted,
		messaging.TypeItemFinished,
		messaging.TypeRepairCompleted,
	}, repo.types())

	stored, err := s.Ticket(ctx, wo)
	require.NoError(t, err)
	assert.Len(t, stored.Timeline, 10)
	assert.Equal(t, ticket.StatusCompleted, stored.Timeline[9].To)
}

func TestStartRepairWithoutItemsCompletes(t *testing.T) {
	repo := newFake()
	s := newService(repo)
	wo := diagnosed(t, repo, s)
	require.NoError(t, s.HandleMessage(context.Background(), command(messaging.TypeEnqueueRepair, wo, messaging.EnqueueRepair{})))

	tk, err := s.StartRepair(context.Background(), wo, "bia", "bia")
	require.NoError(t, err)
	assert.Equal(t, ticket.StatusCompleted, tk.Status)
	types := repo.types()
	assert.Equal(t, []string{messaging.TypeRepairStarted, messaging.TypeRepairCompleted}, types[len(types)-2:])
}

func TestOperatorErrors(t *testing.T) {
	repo := newFake()
	s := newService(repo)
	ctx := context.Background()
	wo := uuid.New()

	_, err := s.StartDiagnosis(ctx, wo, "ana", "ana")
	assert.ErrorIs(t, err, ticket.ErrNotFound)

	schedule(t, s, wo)
	_, err = s.CompleteDiagnosis(ctx, wo, "", "ana")
	assert.ErrorIs(t, err, ticket.ErrInvalidTransition)
	_, err = s.StartRepair(ctx, wo, "", "ana")
	assert.ErrorIs(t, err, ticket.ErrInvalidTransition)
	_, err = s.StartItem(ctx, wo, uuid.New(), "ana")
	assert.ErrorIs(t, err, ticket.ErrItemNotFound)
	_, err = s.FinishItem(ctx, wo, uuid.New(), "ana")
	assert.ErrorIs(t, err, ticket.ErrItemNotFound)

	repo.commitErrs = []error{ticket.ErrConflict, ticket.ErrConflict, ticket.ErrConflict}
	_, err = s.StartDiagnosis(ctx, wo, "ana", "ana")
	assert.ErrorIs(t, err, ticket.ErrConflict)

	repo.commitErrs = []error{ticket.ErrConflict}
	tk, err := s.StartDiagnosis(ctx, wo, "ana", "ana")
	require.NoError(t, err)
	assert.Equal(t, ticket.StatusDiagnosing, tk.Status)
}

func TestQueueAndBay(t *testing.T) {
	repo := newFake()
	s := newService(repo)
	ctx := context.Background()
	schedule(t, s, uuid.New())

	queue, err := s.Queue(ctx, DefaultBranch, ticket.PhaseDiagnosis)
	require.NoError(t, err)
	assert.Len(t, queue, 1)

	bay, err := s.Bay(ctx, "FILIAL")
	require.NoError(t, err)
	assert.True(t, bay.Available)

	bay, err = s.SetBayAvailability(ctx, "FILIAL", false, "ana")
	require.NoError(t, err)
	assert.False(t, bay.Available)
	assert.Equal(t, "ana", bay.UpdatedBy)
	assert.Equal(t, clock, *bay.UpdatedAt)

	repo.err = errors.New("boom")
	_, err = s.SetBayAvailability(ctx, "FILIAL", true, "ana")
	assert.Error(t, err)
}
