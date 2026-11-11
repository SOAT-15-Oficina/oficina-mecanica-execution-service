package execution

import (
	"context"
	"errors"
	"log/slog"

	"github.com/SOAT-15-Oficina/oficina-mecanica-execution-service/internal/messaging"
	"github.com/SOAT-15-Oficina/oficina-mecanica-execution-service/internal/ticket"
)

func (s *Service) HandleMessage(ctx context.Context, env messaging.Envelope) error {
	processed, err := s.repo.IsProcessed(ctx, env.MessageID)
	if err != nil {
		return err
	}
	if processed {
		s.ignored(ctx, env, "duplicate")
		return nil
	}

	switch env.Type {
	case messaging.TypeScheduleDiagnosis:
		err = retry(func() error { return s.scheduleDiagnosis(ctx, env) })
	case messaging.TypeEnqueueRepair:
		err = retry(func() error { return s.enqueueRepair(ctx, env) })
	case messaging.TypeCancelTicket:
		err = retry(func() error { return s.cancelTicket(ctx, env) })
	default:
		err = s.repo.Commit(ctx, ticket.Change{Inbound: &env})
		if err == nil {
			s.ignored(ctx, env, "unknown_type")
		}
	}

	if errors.Is(err, ticket.ErrDuplicateMessage) {
		s.ignored(ctx, env, "duplicate")
		return nil
	}
	return err
}

func (s *Service) ignored(ctx context.Context, env messaging.Envelope, reason string) {
	s.logger.WarnContext(ctx, "execution: message ignored",
		slog.String("event", "execution.message_ignored"),
		slog.String("reason", reason),
		slog.String("message_id", env.MessageID.String()),
		slog.String("message_type", env.Type),
		slog.String("work_order_id", env.WorkOrderID.String()),
	)
}

func (s *Service) reply(ctx context.Context, env messaging.Envelope, change ticket.Change, kind string, payload any) error {
	outbox, err := s.outbox(ctx, env, outgoing{kind: kind, payload: payload})
	if err != nil {
		return err
	}
	change.Inbound = &env
	change.Outbox = outbox
	return s.repo.Commit(ctx, change)
}

func (s *Service) position(ctx context.Context, t *ticket.Ticket) (int, error) {
	if !t.Queued() {
		return 0, nil
	}
	return s.repo.Position(ctx, t)
}

func branchOr(branch string) string {
	if branch == "" {
		return DefaultBranch
	}
	return branch
}

func (s *Service) scheduleDiagnosis(ctx context.Context, env messaging.Envelope) error {
	var p messaging.ScheduleDiagnosis
	if err := env.DecodePayload(&p); err != nil {
		return err
	}
	p.Branch = branchOr(p.Branch)

	change := ticket.Change{}
	t, err := s.repo.GetTicket(ctx, env.WorkOrderID)
	switch {
	case errors.Is(err, ticket.ErrNotFound):
		now := s.now()
		t = ticket.NewDiagnosis(env, p, now)
		change = ticket.Change{
			Ticket: t,
			Create: true,
			Events: []ticket.Event{{At: now, Type: env.Type, To: t.Status, MessageID: &env.MessageID}},
		}
	case err != nil:
		return err
	}

	pos, err := s.position(ctx, t)
	if err != nil {
		return err
	}
	return s.reply(ctx, env, change, messaging.TypeDiagnosisScheduled, messaging.DiagnosisScheduled{QueuePosition: pos})
}

func (s *Service) enqueueRepair(ctx context.Context, env messaging.Envelope) error {
	var p messaging.EnqueueRepair
	if err := env.DecodePayload(&p); err != nil {
		return err
	}

	now := s.now()
	create := false
	t, err := s.repo.GetTicket(ctx, env.WorkOrderID)
	switch {
	case errors.Is(err, ticket.ErrNotFound):
		t = ticket.NewRepair(env, branchOr(p.Branch), now)
		create = true
	case err != nil:
		return err
	}

	switch t.Status {
	case ticket.StatusDiagnosed:
	case ticket.StatusRepairQueued, ticket.StatusRepairing, ticket.StatusCompleted:
		pos, err := s.position(ctx, t)
		if err != nil {
			return err
		}
		return s.reply(ctx, env, ticket.Change{}, messaging.TypeRepairEnqueued, messaging.RepairEnqueued{QueuePosition: pos})
	default:
		return s.reject(ctx, env, ReasonInvalidState)
	}

	bay, err := s.repo.GetBay(ctx, t.Branch)
	if err != nil {
		return err
	}
	if !bay.Available {
		return s.reject(ctx, env, messaging.ReasonBayUnavailable)
	}

	from := t.Status
	if err := t.EnqueueRepair(p.Items, now); err != nil {
		return err
	}
	pos, err := s.position(ctx, t)
	if err != nil {
		return err
	}
	err = s.reply(ctx, env, ticket.Change{
		Ticket:           t,
		Create:           create,
		Items:            t.Items,
		Events:           []ticket.Event{{At: now, Type: env.Type, From: from, To: t.Status, MessageID: &env.MessageID}},
		RequireBayBranch: t.Branch,
	}, messaging.TypeRepairEnqueued, messaging.RepairEnqueued{QueuePosition: pos})
	if errors.Is(err, ticket.ErrBayUnavailable) {
		return s.reject(ctx, env, messaging.ReasonBayUnavailable)
	}
	return err
}

func (s *Service) reject(ctx context.Context, env messaging.Envelope, reason string) error {
	s.logger.WarnContext(ctx, "execution: repair rejected",
		slog.String("event", "execution.repair_rejected"),
		slog.String("reason", reason),
		slog.String("work_order_id", env.WorkOrderID.String()),
	)
	return s.reply(ctx, env, ticket.Change{}, messaging.TypeRepairRejected, messaging.RepairRejected{Reason: reason})
}

func (s *Service) cancelTicket(ctx context.Context, env messaging.Envelope) error {
	var p messaging.CancelTicket
	if err := env.DecodePayload(&p); err != nil {
		return err
	}

	change := ticket.Change{}
	t, err := s.repo.GetTicket(ctx, env.WorkOrderID)
	switch {
	case errors.Is(err, ticket.ErrNotFound):
	case err != nil:
		return err
	case t.Cancelable():
		now := s.now()
		from := t.Status
		if err := t.Cancel(p.Reason, now); err != nil {
			return err
		}
		change = ticket.Change{
			Ticket: t,
			Events: []ticket.Event{{At: now, Type: env.Type, From: from, To: t.Status, MessageID: &env.MessageID, Detail: p.Reason}},
		}
	default:
		s.logger.WarnContext(ctx, "execution: ticket not cancelable, acknowledging anyway",
			slog.String("event", "execution.cancel_ignored"),
			slog.String("status", string(t.Status)),
			slog.String("work_order_id", env.WorkOrderID.String()),
		)
	}
	return s.reply(ctx, env, change, messaging.TypeTicketCanceled, messaging.TicketCanceled{})
}
