package ticket

import (
	"errors"
	"time"

	"github.com/SOAT-15-Oficina/oficina-mecanica-execution-service/internal/messaging"
	"github.com/google/uuid"
)

type Status string

const (
	StatusDiagnosisQueued Status = "DIAGNOSTICO_NA_FILA"
	StatusDiagnosing      Status = "EM_DIAGNOSTICO"
	StatusDiagnosed       Status = "DIAGNOSTICADO"
	StatusRepairQueued    Status = "REPARO_NA_FILA"
	StatusRepairing       Status = "EM_REPARO"
	StatusCompleted       Status = "CONCLUIDO"
	StatusCanceled        Status = "CANCELADO"
)

type Phase string

const (
	PhaseDiagnosis Phase = "DIAGNOSTICO"
	PhaseRepair    Phase = "REPARO"
)

type ItemStatus string

const (
	ItemPending  ItemStatus = "PENDENTE"
	ItemRunning  ItemStatus = "EM_EXECUCAO"
	ItemFinished ItemStatus = "FINALIZADO"
)

var (
	ErrNotFound          = errors.New("ticket not found")
	ErrItemNotFound      = errors.New("ticket item not found")
	ErrInvalidTransition = errors.New("invalid ticket transition")
	ErrConflict          = errors.New("ticket changed concurrently")
	ErrDuplicateMessage  = errors.New("message already processed")
	ErrBayUnavailable    = errors.New("bay unavailable")
)

func ParsePhase(raw string) (Phase, bool) {
	switch Phase(raw) {
	case PhaseDiagnosis, PhaseRepair:
		return Phase(raw), true
	}
	return "", false
}

type Item struct {
	WorkOrderServiceID   uuid.UUID  `json:"work_order_service_id"`
	Title                string     `json:"title"`
	EstimatedTimeMinutes int        `json:"estimated_time_minutes"`
	Status               ItemStatus `json:"status"`
	StartedAt            *time.Time `json:"started_at,omitempty"`
	FinishedAt           *time.Time `json:"finished_at,omitempty"`
}

type Event struct {
	At                 time.Time  `json:"at"`
	Type               string     `json:"type"`
	From               Status     `json:"from,omitempty"`
	To                 Status     `json:"to,omitempty"`
	Actor              string     `json:"actor,omitempty"`
	WorkOrderServiceID *uuid.UUID `json:"work_order_service_id,omitempty"`
	MessageID          *uuid.UUID `json:"message_id,omitempty"`
	Detail             string     `json:"detail,omitempty"`
}

type Ticket struct {
	WorkOrderID          uuid.UUID         `json:"work_order_id"`
	WorkOrderCode        string            `json:"work_order_code,omitempty"`
	Title                string            `json:"title,omitempty"`
	Branch               string            `json:"branch"`
	Vehicle              messaging.Vehicle `json:"vehicle"`
	Status               Status            `json:"status"`
	Phase                Phase             `json:"phase"`
	SagaID               uuid.UUID         `json:"saga_id"`
	CorrelationID        string            `json:"-"`
	DiagnosisTechnician  string            `json:"diagnosis_technician,omitempty"`
	DiagnosisNotes       string            `json:"diagnosis_notes,omitempty"`
	RepairTechnician     string            `json:"repair_technician,omitempty"`
	CancelReason         string            `json:"cancel_reason,omitempty"`
	EnqueuedAt           *time.Time        `json:"enqueued_at,omitempty"`
	DiagnosisStartedAt   *time.Time        `json:"diagnosis_started_at,omitempty"`
	DiagnosisCompletedAt *time.Time        `json:"diagnosis_completed_at,omitempty"`
	RepairStartedAt      *time.Time        `json:"repair_started_at,omitempty"`
	CompletedAt          *time.Time        `json:"completed_at,omitempty"`
	CanceledAt           *time.Time        `json:"canceled_at,omitempty"`
	CreatedAt            time.Time         `json:"created_at"`
	UpdatedAt            time.Time         `json:"updated_at"`
	Version              int               `json:"version"`
	Items                []Item            `json:"items"`
	Timeline             []Event           `json:"timeline"`
}

type Bay struct {
	Branch    string     `json:"branch"`
	Available bool       `json:"available"`
	UpdatedAt *time.Time `json:"updated_at,omitempty"`
	UpdatedBy string     `json:"updated_by,omitempty"`
}

type Change struct {
	Inbound          *messaging.Envelope
	Ticket           *Ticket
	Create           bool
	Items            []Item
	Events           []Event
	Outbox           []messaging.OutboxMessage
	RequireBayBranch string
}

func NewDiagnosis(env messaging.Envelope, payload messaging.ScheduleDiagnosis, now time.Time) *Ticket {
	t := &Ticket{
		WorkOrderID:   env.WorkOrderID,
		WorkOrderCode: payload.WorkOrderCode,
		Title:         payload.Title,
		Branch:        payload.Branch,
		Vehicle:       payload.Vehicle,
		SagaID:        env.SagaID,
		CorrelationID: env.CorrelationID,
		CreatedAt:     now,
	}
	t.moveTo(StatusDiagnosisQueued, now)
	return t
}

func NewRepair(env messaging.Envelope, branch string, now time.Time) *Ticket {
	return &Ticket{
		WorkOrderID:   env.WorkOrderID,
		Branch:        branch,
		SagaID:        env.SagaID,
		CorrelationID: env.CorrelationID,
		Status:        StatusDiagnosed,
		Phase:         PhaseDiagnosis,
		CreatedAt:     now,
		UpdatedAt:     now,
	}
}

func PhaseOf(s Status) Phase {
	switch s {
	case StatusDiagnosisQueued, StatusDiagnosing, StatusDiagnosed, StatusCanceled:
		return PhaseDiagnosis
	}
	return PhaseRepair
}

func (t *Ticket) Queued() bool {
	return t.Status == StatusDiagnosisQueued || t.Status == StatusRepairQueued
}

func (t *Ticket) Cause() messaging.Envelope {
	return messaging.Envelope{SagaID: t.SagaID, WorkOrderID: t.WorkOrderID, CorrelationID: t.CorrelationID}
}

func (t *Ticket) Item(id uuid.UUID) (*Item, bool) {
	for i := range t.Items {
		if t.Items[i].WorkOrderServiceID == id {
			return &t.Items[i], true
		}
	}
	return nil, false
}

func (t *Ticket) StartDiagnosis(technician string, now time.Time) error {
	if t.Status != StatusDiagnosisQueued {
		return ErrInvalidTransition
	}
	t.DiagnosisTechnician = technician
	t.DiagnosisStartedAt = &now
	t.moveTo(StatusDiagnosing, now)
	return nil
}

func (t *Ticket) CompleteDiagnosis(notes string, now time.Time) error {
	if t.Status != StatusDiagnosing {
		return ErrInvalidTransition
	}
	t.DiagnosisNotes = notes
	t.DiagnosisCompletedAt = &now
	t.moveTo(StatusDiagnosed, now)
	return nil
}

func (t *Ticket) EnqueueRepair(items []messaging.RepairItem, now time.Time) error {
	if t.Status != StatusDiagnosed {
		return ErrInvalidTransition
	}
	t.Items = make([]Item, 0, len(items))
	for _, it := range items {
		t.Items = append(t.Items, Item{
			WorkOrderServiceID:   it.WorkOrderServiceID,
			Title:                it.Title,
			EstimatedTimeMinutes: it.EstimatedTimeMinutes,
			Status:               ItemPending,
		})
	}
	t.moveTo(StatusRepairQueued, now)
	return nil
}

func (t *Ticket) StartRepair(technician string, now time.Time) (bool, error) {
	if t.Status != StatusRepairQueued {
		return false, ErrInvalidTransition
	}
	t.RepairTechnician = technician
	t.RepairStartedAt = &now
	t.moveTo(StatusRepairing, now)
	return t.completeIfDone(now), nil
}

func (t *Ticket) StartItem(id uuid.UUID, now time.Time) (*Item, error) {
	item, ok := t.Item(id)
	if !ok {
		return nil, ErrItemNotFound
	}
	if t.Status != StatusRepairing || item.Status != ItemPending {
		return nil, ErrInvalidTransition
	}
	item.Status = ItemRunning
	item.StartedAt = &now
	t.UpdatedAt = now
	return item, nil
}

func (t *Ticket) FinishItem(id uuid.UUID, now time.Time) (*Item, bool, error) {
	item, ok := t.Item(id)
	if !ok {
		return nil, false, ErrItemNotFound
	}
	if t.Status != StatusRepairing || item.Status != ItemRunning {
		return nil, false, ErrInvalidTransition
	}
	item.Status = ItemFinished
	item.FinishedAt = &now
	t.UpdatedAt = now
	return item, t.completeIfDone(now), nil
}

func (t *Ticket) Cancelable() bool {
	return t.Status != StatusRepairing && t.Status != StatusCompleted && t.Status != StatusCanceled
}

func (t *Ticket) Cancel(reason string, now time.Time) error {
	if !t.Cancelable() {
		return ErrInvalidTransition
	}
	t.CancelReason = reason
	t.CanceledAt = &now
	t.moveTo(StatusCanceled, now)
	return nil
}

func (t *Ticket) completeIfDone(now time.Time) bool {
	for _, it := range t.Items {
		if it.Status != ItemFinished {
			return false
		}
	}
	t.CompletedAt = &now
	t.moveTo(StatusCompleted, now)
	return true
}

func (t *Ticket) moveTo(s Status, now time.Time) {
	t.Status = s
	if s != StatusCanceled {
		t.Phase = PhaseOf(s)
	}
	t.UpdatedAt = now
	if t.Queued() {
		t.EnqueuedAt = &now
	}
}
