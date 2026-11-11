package execution

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/SOAT-15-Oficina/oficina-mecanica-execution-service/internal/messaging"
	"github.com/SOAT-15-Oficina/oficina-mecanica-execution-service/internal/ticket"
	"github.com/google/uuid"
)

const (
	DefaultBranch      = "MATRIZ"
	ReasonInvalidState = "ESTADO_INVALIDO"
	maxAttempts        = 3
)

type Repository interface {
	GetTicket(ctx context.Context, workOrderID uuid.UUID) (*ticket.Ticket, error)
	ListQueue(ctx context.Context, branch string, phase ticket.Phase) ([]ticket.Ticket, error)
	Position(ctx context.Context, t *ticket.Ticket) (int, error)
	IsProcessed(ctx context.Context, messageID uuid.UUID) (bool, error)
	GetBay(ctx context.Context, branch string) (ticket.Bay, error)
	SaveBay(ctx context.Context, bay ticket.Bay) error
	Commit(ctx context.Context, change ticket.Change) error
}

type Service struct {
	repo   Repository
	events string
	now    func() time.Time
	logger *slog.Logger
}

func New(repo Repository, eventsTopic string, logger *slog.Logger) *Service {
	return &Service{
		repo:   repo,
		events: eventsTopic,
		now:    func() time.Time { return time.Now().UTC() },
		logger: logger,
	}
}

type outgoing struct {
	kind    string
	payload any
}

func (s *Service) outbox(ctx context.Context, cause messaging.Envelope, msgs ...outgoing) ([]messaging.OutboxMessage, error) {
	carrier := messaging.InjectTrace(ctx)
	out := make([]messaging.OutboxMessage, 0, len(msgs))
	for _, m := range msgs {
		env, err := messaging.New(m.kind, cause, m.payload)
		if err != nil {
			return nil, err
		}
		out = append(out, messaging.OutboxMessage{ID: env.MessageID, Destination: s.events, Envelope: env, Carrier: carrier})
	}
	return out, nil
}

func retry(fn func() error) error {
	var err error
	for range maxAttempts {
		if err = fn(); !errors.Is(err, ticket.ErrConflict) {
			return err
		}
	}
	return err
}

func (s *Service) Ticket(ctx context.Context, workOrderID uuid.UUID) (*ticket.Ticket, error) {
	return s.repo.GetTicket(ctx, workOrderID)
}

func (s *Service) Queue(ctx context.Context, branch string, phase ticket.Phase) ([]ticket.Ticket, error) {
	return s.repo.ListQueue(ctx, branch, phase)
}

func (s *Service) Bay(ctx context.Context, branch string) (ticket.Bay, error) {
	return s.repo.GetBay(ctx, branch)
}

func (s *Service) SetBayAvailability(ctx context.Context, branch string, available bool, actor string) (ticket.Bay, error) {
	now := s.now()
	bay := ticket.Bay{Branch: branch, Available: available, UpdatedAt: &now, UpdatedBy: actor}
	if err := s.repo.SaveBay(ctx, bay); err != nil {
		return ticket.Bay{}, err
	}
	s.logger.InfoContext(ctx, "execution: bay availability changed",
		slog.String("event", "bay.availability_changed"),
		slog.String("branch", branch),
		slog.Bool("available", available),
	)
	return bay, nil
}
