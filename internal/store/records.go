package store

import (
	"fmt"
	"strings"
	"time"

	"github.com/SOAT-15-Oficina/oficina-mecanica-execution-service/internal/messaging"
	"github.com/SOAT-15-Oficina/oficina-mecanica-execution-service/internal/ticket"
	"github.com/google/uuid"
)

const (
	skMeta       = "META"
	skMessage    = "MSG"
	skOutbox     = "OUTBOX"
	prefixTicket = "TICKET#"
	prefixItem   = "ITEM#"
	prefixEvent  = "EVENT#"
	prefixQueue  = "QUEUE#"
	prefixBay    = "BAY#"
	prefixMsg    = "MSG#"
	prefixOutbox = "OUTBOX#"
	outboxQueue  = "OUTBOX#PENDING"
)

func ticketPK(id uuid.UUID) string  { return prefixTicket + id.String() }
func bayPK(branch string) string    { return prefixBay + branch }
func messagePK(id uuid.UUID) string { return prefixMsg + id.String() }
func outboxPK(id uuid.UUID) string  { return prefixOutbox + id.String() }
func queuePK(branch string, phase ticket.Phase) string {
	return prefixQueue + branch + "#" + string(phase)
}

func QueueKey(t *ticket.Ticket) string {
	at := t.CreatedAt
	if t.EnqueuedAt != nil {
		at = *t.EnqueuedAt
	}
	return keyTime(at) + "#" + t.WorkOrderID.String()
}

type vehicleRecord struct {
	LicensePlate string `dynamodbav:"license_plate,omitempty"`
	Brand        string `dynamodbav:"brand,omitempty"`
	Model        string `dynamodbav:"model,omitempty"`
	Year         int    `dynamodbav:"year,omitempty"`
}

type metaRecord struct {
	PK                   string        `dynamodbav:"PK"`
	SK                   string        `dynamodbav:"SK"`
	GSI1PK               string        `dynamodbav:"GSI1PK,omitempty"`
	GSI1SK               string        `dynamodbav:"GSI1SK,omitempty"`
	WorkOrderID          string        `dynamodbav:"work_order_id"`
	WorkOrderCode        string        `dynamodbav:"work_order_code,omitempty"`
	Title                string        `dynamodbav:"title,omitempty"`
	Branch               string        `dynamodbav:"branch"`
	Vehicle              vehicleRecord `dynamodbav:"vehicle"`
	Status               string        `dynamodbav:"status"`
	Phase                string        `dynamodbav:"phase"`
	SagaID               string        `dynamodbav:"saga_id"`
	CorrelationID        string        `dynamodbav:"correlation_id,omitempty"`
	DiagnosisTechnician  string        `dynamodbav:"diagnosis_technician,omitempty"`
	DiagnosisNotes       string        `dynamodbav:"diagnosis_notes,omitempty"`
	RepairTechnician     string        `dynamodbav:"repair_technician,omitempty"`
	CancelReason         string        `dynamodbav:"cancel_reason,omitempty"`
	EnqueuedAt           *time.Time    `dynamodbav:"enqueued_at,omitempty"`
	DiagnosisStartedAt   *time.Time    `dynamodbav:"diagnosis_started_at,omitempty"`
	DiagnosisCompletedAt *time.Time    `dynamodbav:"diagnosis_completed_at,omitempty"`
	RepairStartedAt      *time.Time    `dynamodbav:"repair_started_at,omitempty"`
	CompletedAt          *time.Time    `dynamodbav:"completed_at,omitempty"`
	CanceledAt           *time.Time    `dynamodbav:"canceled_at,omitempty"`
	CreatedAt            time.Time     `dynamodbav:"created_at"`
	UpdatedAt            time.Time     `dynamodbav:"updated_at"`
	Version              int           `dynamodbav:"version"`
}

type itemRecord struct {
	PK                   string     `dynamodbav:"PK"`
	SK                   string     `dynamodbav:"SK"`
	Position             int        `dynamodbav:"position"`
	WorkOrderServiceID   string     `dynamodbav:"work_order_service_id"`
	Title                string     `dynamodbav:"title,omitempty"`
	EstimatedTimeMinutes int        `dynamodbav:"estimated_time_minutes"`
	Status               string     `dynamodbav:"status"`
	StartedAt            *time.Time `dynamodbav:"started_at,omitempty"`
	FinishedAt           *time.Time `dynamodbav:"finished_at,omitempty"`
}

type eventRecord struct {
	PK                 string    `dynamodbav:"PK"`
	SK                 string    `dynamodbav:"SK"`
	At                 time.Time `dynamodbav:"at"`
	Type               string    `dynamodbav:"type"`
	From               string    `dynamodbav:"from,omitempty"`
	To                 string    `dynamodbav:"to,omitempty"`
	Actor              string    `dynamodbav:"actor,omitempty"`
	WorkOrderServiceID string    `dynamodbav:"work_order_service_id,omitempty"`
	MessageID          string    `dynamodbav:"message_id,omitempty"`
	Detail             string    `dynamodbav:"detail,omitempty"`
}

type messageRecord struct {
	PK          string    `dynamodbav:"PK"`
	SK          string    `dynamodbav:"SK"`
	Type        string    `dynamodbav:"type"`
	WorkOrderID string    `dynamodbav:"work_order_id"`
	ProcessedAt time.Time `dynamodbav:"processed_at"`
	ExpiresAt   int64     `dynamodbav:"expires_at"`
}

type outboxRecord struct {
	PK          string            `dynamodbav:"PK"`
	SK          string            `dynamodbav:"SK"`
	GSI1PK      string            `dynamodbav:"GSI1PK,omitempty"`
	GSI1SK      string            `dynamodbav:"GSI1SK,omitempty"`
	ID          string            `dynamodbav:"id"`
	Destination string            `dynamodbav:"destination"`
	Type        string            `dynamodbav:"type"`
	Envelope    string            `dynamodbav:"envelope"`
	Carrier     map[string]string `dynamodbav:"carrier,omitempty"`
	CreatedAt   time.Time         `dynamodbav:"created_at"`
}

type bayRecord struct {
	PK        string     `dynamodbav:"PK"`
	SK        string     `dynamodbav:"SK"`
	Branch    string     `dynamodbav:"branch"`
	Available bool       `dynamodbav:"available"`
	UpdatedAt *time.Time `dynamodbav:"updated_at,omitempty"`
	UpdatedBy string     `dynamodbav:"updated_by,omitempty"`
}

func toMeta(t *ticket.Ticket, version int) metaRecord {
	r := metaRecord{
		PK:                   ticketPK(t.WorkOrderID),
		SK:                   skMeta,
		WorkOrderID:          t.WorkOrderID.String(),
		WorkOrderCode:        t.WorkOrderCode,
		Title:                t.Title,
		Branch:               t.Branch,
		Vehicle:              vehicleRecord(t.Vehicle),
		Status:               string(t.Status),
		Phase:                string(t.Phase),
		SagaID:               t.SagaID.String(),
		CorrelationID:        t.CorrelationID,
		DiagnosisTechnician:  t.DiagnosisTechnician,
		DiagnosisNotes:       t.DiagnosisNotes,
		RepairTechnician:     t.RepairTechnician,
		CancelReason:         t.CancelReason,
		EnqueuedAt:           t.EnqueuedAt,
		DiagnosisStartedAt:   t.DiagnosisStartedAt,
		DiagnosisCompletedAt: t.DiagnosisCompletedAt,
		RepairStartedAt:      t.RepairStartedAt,
		CompletedAt:          t.CompletedAt,
		CanceledAt:           t.CanceledAt,
		CreatedAt:            t.CreatedAt,
		UpdatedAt:            t.UpdatedAt,
		Version:              version,
	}
	if t.Queued() {
		r.GSI1PK = queuePK(t.Branch, t.Phase)
		r.GSI1SK = QueueKey(t)
	}
	return r
}

func (r metaRecord) toTicket() (*ticket.Ticket, error) {
	wo, err := uuid.Parse(r.WorkOrderID)
	if err != nil {
		return nil, fmt.Errorf("ticket %s: %w", r.PK, err)
	}
	saga, _ := uuid.Parse(r.SagaID)
	return &ticket.Ticket{
		WorkOrderID:          wo,
		WorkOrderCode:        r.WorkOrderCode,
		Title:                r.Title,
		Branch:               r.Branch,
		Vehicle:              messaging.Vehicle(r.Vehicle),
		Status:               ticket.Status(r.Status),
		Phase:                ticket.Phase(r.Phase),
		SagaID:               saga,
		CorrelationID:        r.CorrelationID,
		DiagnosisTechnician:  r.DiagnosisTechnician,
		DiagnosisNotes:       r.DiagnosisNotes,
		RepairTechnician:     r.RepairTechnician,
		CancelReason:         r.CancelReason,
		EnqueuedAt:           r.EnqueuedAt,
		DiagnosisStartedAt:   r.DiagnosisStartedAt,
		DiagnosisCompletedAt: r.DiagnosisCompletedAt,
		RepairStartedAt:      r.RepairStartedAt,
		CompletedAt:          r.CompletedAt,
		CanceledAt:           r.CanceledAt,
		CreatedAt:            r.CreatedAt,
		UpdatedAt:            r.UpdatedAt,
		Version:              r.Version,
		Items:                []ticket.Item{},
		Timeline:             []ticket.Event{},
	}, nil
}

func toItem(wo uuid.UUID, position int, it ticket.Item) itemRecord {
	return itemRecord{
		PK:                   ticketPK(wo),
		SK:                   prefixItem + it.WorkOrderServiceID.String(),
		Position:             position,
		WorkOrderServiceID:   it.WorkOrderServiceID.String(),
		Title:                it.Title,
		EstimatedTimeMinutes: it.EstimatedTimeMinutes,
		Status:               string(it.Status),
		StartedAt:            it.StartedAt,
		FinishedAt:           it.FinishedAt,
	}
}

func (r itemRecord) toItem() ticket.Item {
	id, _ := uuid.Parse(r.WorkOrderServiceID)
	return ticket.Item{
		WorkOrderServiceID:   id,
		Title:                r.Title,
		EstimatedTimeMinutes: r.EstimatedTimeMinutes,
		Status:               ticket.ItemStatus(r.Status),
		StartedAt:            r.StartedAt,
		FinishedAt:           r.FinishedAt,
	}
}

func toEvent(wo uuid.UUID, seq int, e ticket.Event) eventRecord {
	r := eventRecord{
		PK:     ticketPK(wo),
		SK:     fmt.Sprintf("%s%s#%03d#%s", prefixEvent, keyTime(e.At), seq, uuid.NewString()[:8]),
		At:     e.At,
		Type:   e.Type,
		From:   string(e.From),
		To:     string(e.To),
		Actor:  e.Actor,
		Detail: e.Detail,
	}
	if e.WorkOrderServiceID != nil {
		r.WorkOrderServiceID = e.WorkOrderServiceID.String()
	}
	if e.MessageID != nil {
		r.MessageID = e.MessageID.String()
	}
	return r
}

func (r eventRecord) toEvent() ticket.Event {
	e := ticket.Event{
		At:     r.At,
		Type:   r.Type,
		From:   ticket.Status(r.From),
		To:     ticket.Status(r.To),
		Actor:  r.Actor,
		Detail: r.Detail,
	}
	if id, err := uuid.Parse(r.WorkOrderServiceID); err == nil {
		e.WorkOrderServiceID = &id
	}
	if id, err := uuid.Parse(r.MessageID); err == nil {
		e.MessageID = &id
	}
	return e
}

func skKind(sk string) string {
	switch {
	case sk == skMeta:
		return skMeta
	case strings.HasPrefix(sk, prefixItem):
		return prefixItem
	case strings.HasPrefix(sk, prefixEvent):
		return prefixEvent
	}
	return ""
}
