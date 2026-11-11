package ticket

import (
	"testing"
	"time"

	"github.com/SOAT-15-Oficina/oficina-mecanica-execution-service/internal/messaging"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var now = time.Date(2026, 11, 11, 10, 0, 0, 0, time.UTC)

func scheduled(t *testing.T) *Ticket {
	t.Helper()
	env := messaging.Envelope{MessageID: uuid.New(), SagaID: uuid.New(), WorkOrderID: uuid.New(), CorrelationID: "req-1"}
	tk := NewDiagnosis(env, messaging.ScheduleDiagnosis{Branch: "MATRIZ", WorkOrderCode: "OS-1", Title: "Barulho"}, now)
	require.Equal(t, StatusDiagnosisQueued, tk.Status)
	require.Equal(t, PhaseDiagnosis, tk.Phase)
	require.NotNil(t, tk.EnqueuedAt)
	assert.Equal(t, env.SagaID, tk.Cause().SagaID)
	assert.Equal(t, "req-1", tk.Cause().CorrelationID)
	return tk
}

func TestParsePhase(t *testing.T) {
	p, ok := ParsePhase("REPARO")
	assert.True(t, ok)
	assert.Equal(t, PhaseRepair, p)
	_, ok = ParsePhase("x")
	assert.False(t, ok)
}

func TestHappyPath(t *testing.T) {
	tk := scheduled(t)
	require.NoError(t, tk.StartDiagnosis("ana", now))
	assert.Equal(t, StatusDiagnosing, tk.Status)
	assert.False(t, tk.Queued())
	require.NoError(t, tk.CompleteDiagnosis("ok", now))
	assert.Equal(t, StatusDiagnosed, tk.Status)

	a, b := uuid.New(), uuid.New()
	require.NoError(t, tk.EnqueueRepair([]messaging.RepairItem{{WorkOrderServiceID: a, Title: "A"}, {WorkOrderServiceID: b, Title: "B"}}, now))
	assert.Equal(t, StatusRepairQueued, tk.Status)
	assert.Equal(t, PhaseRepair, tk.Phase)
	assert.True(t, tk.Queued())

	done, err := tk.StartRepair("bia", now)
	require.NoError(t, err)
	assert.False(t, done)

	_, _, err = tk.FinishItem(a, now)
	assert.ErrorIs(t, err, ErrInvalidTransition)

	item, err := tk.StartItem(a, now)
	require.NoError(t, err)
	assert.Equal(t, ItemRunning, item.Status)
	_, err = tk.StartItem(a, now)
	assert.ErrorIs(t, err, ErrInvalidTransition)

	_, done, err = tk.FinishItem(a, now)
	require.NoError(t, err)
	assert.False(t, done)

	_, err = tk.StartItem(b, now)
	require.NoError(t, err)
	_, done, err = tk.FinishItem(b, now)
	require.NoError(t, err)
	assert.True(t, done)
	assert.Equal(t, StatusCompleted, tk.Status)
	assert.NotNil(t, tk.CompletedAt)
	assert.False(t, tk.Cancelable())
	assert.ErrorIs(t, tk.Cancel("x", now), ErrInvalidTransition)
}

func TestInvalidTransitions(t *testing.T) {
	tk := scheduled(t)
	assert.ErrorIs(t, tk.CompleteDiagnosis("", now), ErrInvalidTransition)
	assert.ErrorIs(t, tk.EnqueueRepair(nil, now), ErrInvalidTransition)
	_, err := tk.StartRepair("x", now)
	assert.ErrorIs(t, err, ErrInvalidTransition)
	_, err = tk.StartItem(uuid.New(), now)
	assert.ErrorIs(t, err, ErrItemNotFound)
	_, _, err = tk.FinishItem(uuid.New(), now)
	assert.ErrorIs(t, err, ErrItemNotFound)
	require.NoError(t, tk.StartDiagnosis("x", now))
	assert.ErrorIs(t, tk.StartDiagnosis("x", now), ErrInvalidTransition)
}

func TestRepairWithoutItemsCompletesOnStart(t *testing.T) {
	tk := NewRepair(messaging.Envelope{WorkOrderID: uuid.New()}, "MATRIZ", now)
	require.NoError(t, tk.EnqueueRepair(nil, now))
	done, err := tk.StartRepair("x", now)
	require.NoError(t, err)
	assert.True(t, done)
	assert.Equal(t, StatusCompleted, tk.Status)
}

func TestCancelKeepsPhase(t *testing.T) {
	tk := scheduled(t)
	require.NoError(t, tk.Cancel("CLIENTE", now))
	assert.Equal(t, StatusCanceled, tk.Status)
	assert.Equal(t, PhaseDiagnosis, tk.Phase)
	assert.Equal(t, "CLIENTE", tk.CancelReason)
	assert.Equal(t, PhaseDiagnosis, PhaseOf(StatusCanceled))
	assert.Equal(t, PhaseRepair, PhaseOf(StatusRepairing))
}
