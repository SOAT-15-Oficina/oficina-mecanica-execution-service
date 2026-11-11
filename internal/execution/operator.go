package execution

import (
	"context"
	"time"

	"github.com/SOAT-15-Oficina/oficina-mecanica-execution-service/internal/messaging"
	"github.com/SOAT-15-Oficina/oficina-mecanica-execution-service/internal/ticket"
	"github.com/google/uuid"
)

type effect struct {
	events []ticket.Event
	items  []ticket.Item
	msgs   []outgoing
}

func (e *effect) emit(kind string, from, to ticket.Status, item *uuid.UUID, payload any, at time.Time) {
	e.events = append(e.events, ticket.Event{At: at, Type: kind, From: from, To: to, WorkOrderServiceID: item})
	e.msgs = append(e.msgs, outgoing{kind: kind, payload: payload})
}

func (e *effect) complete(t *ticket.Ticket, at time.Time) {
	e.emit(messaging.TypeRepairCompleted, ticket.StatusRepairing, t.Status, nil, messaging.RepairCompleted{CompletedAt: at}, at)
}

func (s *Service) act(ctx context.Context, workOrderID uuid.UUID, actor string, apply func(t *ticket.Ticket, now time.Time) (effect, error)) (*ticket.Ticket, error) {
	var result *ticket.Ticket
	err := retry(func() error {
		t, err := s.repo.GetTicket(ctx, workOrderID)
		if err != nil {
			return err
		}
		eff, err := apply(t, s.now())
		if err != nil {
			return err
		}
		for i := range eff.events {
			eff.events[i].Actor = actor
		}
		outbox, err := s.outbox(ctx, t.Cause(), eff.msgs...)
		if err != nil {
			return err
		}
		if err := s.repo.Commit(ctx, ticket.Change{Ticket: t, Items: eff.items, Events: eff.events, Outbox: outbox}); err != nil {
			return err
		}
		t.Timeline = append(t.Timeline, eff.events...)
		result = t
		return nil
	})
	return result, err
}

func (s *Service) StartDiagnosis(ctx context.Context, workOrderID uuid.UUID, technician, actor string) (*ticket.Ticket, error) {
	return s.act(ctx, workOrderID, actor, func(t *ticket.Ticket, now time.Time) (effect, error) {
		var eff effect
		from := t.Status
		if err := t.StartDiagnosis(technician, now); err != nil {
			return eff, err
		}
		eff.emit(messaging.TypeDiagnosisStarted, from, t.Status, nil, messaging.DiagnosisStarted{Technician: technician, StartedAt: now}, now)
		return eff, nil
	})
}

func (s *Service) CompleteDiagnosis(ctx context.Context, workOrderID uuid.UUID, notes, actor string) (*ticket.Ticket, error) {
	return s.act(ctx, workOrderID, actor, func(t *ticket.Ticket, now time.Time) (effect, error) {
		var eff effect
		from := t.Status
		if err := t.CompleteDiagnosis(notes, now); err != nil {
			return eff, err
		}
		eff.emit(messaging.TypeDiagnosisCompleted, from, t.Status, nil, messaging.DiagnosisCompleted{CompletedAt: now, Notes: notes}, now)
		return eff, nil
	})
}

func (s *Service) StartRepair(ctx context.Context, workOrderID uuid.UUID, technician, actor string) (*ticket.Ticket, error) {
	return s.act(ctx, workOrderID, actor, func(t *ticket.Ticket, now time.Time) (effect, error) {
		var eff effect
		from := t.Status
		done, err := t.StartRepair(technician, now)
		if err != nil {
			return eff, err
		}
		eff.emit(messaging.TypeRepairStarted, from, ticket.StatusRepairing, nil, messaging.RepairStarted{Technician: technician, StartedAt: now}, now)
		if done {
			eff.complete(t, now)
		}
		return eff, nil
	})
}

func (s *Service) StartItem(ctx context.Context, workOrderID, itemID uuid.UUID, actor string) (*ticket.Ticket, error) {
	return s.act(ctx, workOrderID, actor, func(t *ticket.Ticket, now time.Time) (effect, error) {
		var eff effect
		item, err := t.StartItem(itemID, now)
		if err != nil {
			return eff, err
		}
		eff.items = []ticket.Item{*item}
		eff.emit(messaging.TypeItemStarted, "", "", &itemID, messaging.ItemStarted{WorkOrderServiceID: itemID, StartedAt: now}, now)
		return eff, nil
	})
}

func (s *Service) FinishItem(ctx context.Context, workOrderID, itemID uuid.UUID, actor string) (*ticket.Ticket, error) {
	return s.act(ctx, workOrderID, actor, func(t *ticket.Ticket, now time.Time) (effect, error) {
		var eff effect
		item, done, err := t.FinishItem(itemID, now)
		if err != nil {
			return eff, err
		}
		eff.items = []ticket.Item{*item}
		eff.emit(messaging.TypeItemFinished, "", "", &itemID, messaging.ItemFinished{WorkOrderServiceID: itemID, FinishedAt: now}, now)
		if done {
			eff.complete(t, now)
		}
		return eff, nil
	})
}
