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

func schedule(t *testing.T, s *Service, wo uuid.UUID) messaging.Envelope {
	t.Helper()
	env := command(messaging.TypeScheduleDiagnosis, wo, messaging.ScheduleDiagnosis{WorkOrderCode: "OS-1", Title: "Revisao"})
	require.NoError(t, s.HandleMessage(context.Background(), env))
	return env
}

func diagnosed(t *testing.T, repo *fakeRepo, s *Service) uuid.UUID {
	t.Helper()
	wo := uuid.New()
	schedule(t, s, wo)
	_, err := s.StartDiagnosis(context.Background(), wo, "ana", "ana")
	require.NoError(t, err)
	_, err = s.CompleteDiagnosis(context.Background(), wo, "ok", "ana")
	require.NoError(t, err)
	return wo
}

func TestScheduleDiagnosisCreatesTicketAndReplies(t *testing.T) {
	repo := newFake()
	repo.position = 3
	s := newService(repo)
	wo := uuid.New()

	env := schedule(t, s, wo)

	tk := repo.tickets[wo]
	require.NotNil(t, tk)
	assert.Equal(t, ticket.StatusDiagnosisQueued, tk.Status)
	assert.Equal(t, DefaultBranch, tk.Branch)
	assert.Equal(t, env.SagaID, tk.SagaID)
	require.Len(t, tk.Timeline, 1)

	reply := repo.last()
	assert.Equal(t, messaging.TypeDiagnosisScheduled, reply.Envelope.Type)
	assert.Equal(t, topic, reply.Destination)
	assert.Equal(t, env.MessageID, *reply.Envelope.CausationID)
	assert.Equal(t, env.SagaID, reply.Envelope.SagaID)
	var p messaging.DiagnosisScheduled
	require.NoError(t, reply.Envelope.DecodePayload(&p))
	assert.Equal(t, 3, p.QueuePosition)

	require.NoError(t, s.HandleMessage(context.Background(), env))
	assert.Len(t, repo.outbox, 1)

	again := command(messaging.TypeScheduleDiagnosis, wo, messaging.ScheduleDiagnosis{Branch: "OUTRA"})
	require.NoError(t, s.HandleMessage(context.Background(), again))
	assert.Equal(t, []string{messaging.TypeDiagnosisScheduled, messaging.TypeDiagnosisScheduled}, repo.types())
	assert.Equal(t, DefaultBranch, repo.tickets[wo].Branch)
}

func TestHandleMessageIgnoresDuplicateDetectedOnCommit(t *testing.T) {
	repo := newFake()
	repo.commitErrs = []error{ticket.ErrDuplicateMessage}
	require.NoError(t, newService(repo).HandleMessage(context.Background(), command(messaging.TypeScheduleDiagnosis, uuid.New(), messaging.ScheduleDiagnosis{})))
	assert.Empty(t, repo.outbox)
}

func TestHandleMessageRetriesOnConflict(t *testing.T) {
	repo := newFake()
	repo.commitErrs = []error{ticket.ErrConflict, ticket.ErrConflict}
	require.NoError(t, newService(repo).HandleMessage(context.Background(), command(messaging.TypeScheduleDiagnosis, uuid.New(), messaging.ScheduleDiagnosis{})))
	assert.Equal(t, 3, repo.commits)

	repo = newFake()
	repo.commitErrs = []error{ticket.ErrConflict, ticket.ErrConflict, ticket.ErrConflict}
	err := newService(repo).HandleMessage(context.Background(), command(messaging.TypeScheduleDiagnosis, uuid.New(), messaging.ScheduleDiagnosis{}))
	assert.ErrorIs(t, err, ticket.ErrConflict)
}

func TestHandleMessageUnknownTypeIsMarkedProcessed(t *testing.T) {
	repo := newFake()
	env := command("execution.something-else", uuid.New(), struct{}{})
	require.NoError(t, newService(repo).HandleMessage(context.Background(), env))
	assert.True(t, repo.processed[env.MessageID])
	assert.Empty(t, repo.outbox)
}

func TestHandleMessageErrors(t *testing.T) {
	boom := errors.New("boom")
	wo := uuid.New()
	bad := command(messaging.TypeScheduleDiagnosis, wo, nil)
	bad.Payload = []byte(`"x"`)

	repo := newFake()
	repo.err = boom
	assert.ErrorIs(t, newService(repo).HandleMessage(context.Background(), bad), boom)

	for _, kind := range []string{messaging.TypeScheduleDiagnosis, messaging.TypeEnqueueRepair, messaging.TypeCancelTicket} {
		env := command(kind, wo, nil)
		env.Payload = []byte(`"x"`)
		assert.Error(t, newService(newFake()).HandleMessage(context.Background(), env), kind)

		repo := newFake()
		repo.getErr = boom
		assert.ErrorIs(t, newService(repo).HandleMessage(context.Background(), command(kind, wo, struct{}{})), boom, kind)
	}

	repo = newFake()
	repo.posErr = boom
	assert.ErrorIs(t, newService(repo).HandleMessage(context.Background(), command(messaging.TypeScheduleDiagnosis, wo, messaging.ScheduleDiagnosis{})), boom)
}

func TestEnqueueRepairAcceptsDiagnosedTicket(t *testing.T) {
	repo := newFake()
	s := newService(repo)
	wo := diagnosed(t, repo, s)
	items := []messaging.RepairItem{{WorkOrderServiceID: uuid.New(), Title: "Freio"}}

	env := command(messaging.TypeEnqueueRepair, wo, messaging.EnqueueRepair{Branch: "MATRIZ", Items: items})
	require.NoError(t, s.HandleMessage(context.Background(), env))

	tk := repo.tickets[wo]
	assert.Equal(t, ticket.StatusRepairQueued, tk.Status)
	require.Len(t, tk.Items, 1)
	assert.Equal(t, ticket.ItemPending, tk.Items[0].Status)
	assert.Equal(t, messaging.TypeRepairEnqueued, repo.last().Envelope.Type)

	again := command(messaging.TypeEnqueueRepair, wo, messaging.EnqueueRepair{Items: items})
	require.NoError(t, s.HandleMessage(context.Background(), again))
	assert.Equal(t, messaging.TypeRepairEnqueued, repo.last().Envelope.Type)
}

func TestEnqueueRepairCreatesMissingTicket(t *testing.T) {
	repo := newFake()
	wo := uuid.New()
	require.NoError(t, newService(repo).HandleMessage(context.Background(), command(messaging.TypeEnqueueRepair, wo, messaging.EnqueueRepair{})))
	assert.Equal(t, ticket.StatusRepairQueued, repo.tickets[wo].Status)
	assert.Equal(t, DefaultBranch, repo.tickets[wo].Branch)
}

func rejection(t *testing.T, repo *fakeRepo) string {
	t.Helper()
	msg := repo.last()
	require.Equal(t, messaging.TypeRepairRejected, msg.Envelope.Type)
	var p messaging.RepairRejected
	require.NoError(t, msg.Envelope.DecodePayload(&p))
	return p.Reason
}

func TestEnqueueRepairRejectsWhenBayUnavailable(t *testing.T) {
	repo := newFake()
	s := newService(repo)
	wo := diagnosed(t, repo, s)
	_, err := s.SetBayAvailability(context.Background(), DefaultBranch, false, "ana")
	require.NoError(t, err)

	require.NoError(t, s.HandleMessage(context.Background(), command(messaging.TypeEnqueueRepair, wo, messaging.EnqueueRepair{})))
	assert.Equal(t, messaging.ReasonBayUnavailable, rejection(t, repo))
	assert.Equal(t, ticket.StatusDiagnosed, repo.tickets[wo].Status)
}

func TestEnqueueRepairRejectsWhenBayClosesDuringCommit(t *testing.T) {
	repo := newFake()
	s := newService(repo)
	wo := diagnosed(t, repo, s)
	repo.commitErrs = []error{ticket.ErrBayUnavailable}

	require.NoError(t, s.HandleMessage(context.Background(), command(messaging.TypeEnqueueRepair, wo, messaging.EnqueueRepair{})))
	assert.Equal(t, messaging.ReasonBayUnavailable, rejection(t, repo))
}

func TestEnqueueRepairRejectsInvalidState(t *testing.T) {
	repo := newFake()
	s := newService(repo)
	wo := uuid.New()
	schedule(t, s, wo)

	require.NoError(t, s.HandleMessage(context.Background(), command(messaging.TypeEnqueueRepair, wo, messaging.EnqueueRepair{})))
	assert.Equal(t, ReasonInvalidState, rejection(t, repo))
}

func TestEnqueueRepairPropagatesErrors(t *testing.T) {
	boom := errors.New("boom")
	repo := newFake()
	s := newService(repo)
	wo := diagnosed(t, repo, s)

	repo.bayErr = boom
	assert.ErrorIs(t, s.HandleMessage(context.Background(), command(messaging.TypeEnqueueRepair, wo, messaging.EnqueueRepair{})), boom)
	repo.bayErr = nil

	repo.posErr = boom
	assert.ErrorIs(t, s.HandleMessage(context.Background(), command(messaging.TypeEnqueueRepair, wo, messaging.EnqueueRepair{})), boom)
	repo.posErr = nil

	require.NoError(t, s.HandleMessage(context.Background(), command(messaging.TypeEnqueueRepair, wo, messaging.EnqueueRepair{})))
	repo.posErr = boom
	assert.ErrorIs(t, s.HandleMessage(context.Background(), command(messaging.TypeEnqueueRepair, wo, messaging.EnqueueRepair{})), boom)
}

func TestCancelTicket(t *testing.T) {
	repo := newFake()
	s := newService(repo)
	wo := uuid.New()
	schedule(t, s, wo)

	require.NoError(t, s.HandleMessage(context.Background(), command(messaging.TypeCancelTicket, wo, messaging.CancelTicket{Reason: messaging.ReasonCustomer})))
	tk := repo.tickets[wo]
	assert.Equal(t, ticket.StatusCanceled, tk.Status)
	assert.Equal(t, messaging.ReasonCustomer, tk.CancelReason)
	assert.Equal(t, messaging.TypeTicketCanceled, repo.last().Envelope.Type)

	require.NoError(t, s.HandleMessage(context.Background(), command(messaging.TypeCancelTicket, wo, messaging.CancelTicket{})))
	assert.Equal(t, messaging.TypeTicketCanceled, repo.last().Envelope.Type)

	missing := uuid.New()
	env := command(messaging.TypeCancelTicket, missing, messaging.CancelTicket{})
	require.NoError(t, s.HandleMessage(context.Background(), env))
	assert.Equal(t, messaging.TypeTicketCanceled, repo.last().Envelope.Type)
	assert.Equal(t, missing, repo.last().Envelope.WorkOrderID)
	assert.True(t, repo.processed[env.MessageID])
	assert.NotContains(t, repo.tickets, missing)
}

func TestCancelTicketDuringRepairStillAcknowledges(t *testing.T) {
	repo := newFake()
	s := newService(repo)
	wo := diagnosed(t, repo, s)
	require.NoError(t, s.HandleMessage(context.Background(), command(messaging.TypeEnqueueRepair, wo, messaging.EnqueueRepair{Items: []messaging.RepairItem{{WorkOrderServiceID: uuid.New()}}})))
	_, err := s.StartRepair(context.Background(), wo, "bia", "bia")
	require.NoError(t, err)

	require.NoError(t, s.HandleMessage(context.Background(), command(messaging.TypeCancelTicket, wo, messaging.CancelTicket{})))
	assert.Equal(t, ticket.StatusRepairing, repo.tickets[wo].Status)
	assert.Equal(t, messaging.TypeTicketCanceled, repo.last().Envelope.Type)
}
