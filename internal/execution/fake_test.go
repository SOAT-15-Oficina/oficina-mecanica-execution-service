package execution

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/SOAT-15-Oficina/oficina-mecanica-execution-service/internal/messaging"
	"github.com/SOAT-15-Oficina/oficina-mecanica-execution-service/internal/ticket"
	"github.com/google/uuid"
)

const topic = "arn:aws:sns:sa-east-1:000000000000:execution-events.fifo"

var clock = time.Date(2026, 11, 11, 10, 0, 0, 0, time.UTC)

type fakeRepo struct {
	mu         sync.Mutex
	tickets    map[uuid.UUID]*ticket.Ticket
	processed  map[uuid.UUID]bool
	bays       map[string]ticket.Bay
	outbox     []messaging.OutboxMessage
	position   int
	commitErrs []error
	err        error
	getErr     error
	posErr     error
	bayErr     error
	commits    int
}

func newFake() *fakeRepo {
	return &fakeRepo{
		tickets:   map[uuid.UUID]*ticket.Ticket{},
		processed: map[uuid.UUID]bool{},
		bays:      map[string]ticket.Bay{},
		position:  1,
	}
}

func clone(t *ticket.Ticket) *ticket.Ticket {
	cp := *t
	cp.Items = append([]ticket.Item(nil), t.Items...)
	cp.Timeline = append([]ticket.Event(nil), t.Timeline...)
	return &cp
}

func (f *fakeRepo) GetTicket(_ context.Context, id uuid.UUID) (*ticket.Ticket, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.getErr != nil {
		return nil, f.getErr
	}
	t, ok := f.tickets[id]
	if !ok {
		return nil, ticket.ErrNotFound
	}
	return clone(t), nil
}

func (f *fakeRepo) ListQueue(_ context.Context, branch string, phase ticket.Phase) ([]ticket.Ticket, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []ticket.Ticket
	for _, t := range f.tickets {
		if t.Queued() && t.Branch == branch && t.Phase == phase {
			out = append(out, *clone(t))
		}
	}
	return out, f.err
}

func (f *fakeRepo) Position(context.Context, *ticket.Ticket) (int, error) {
	return f.position, f.posErr
}

func (f *fakeRepo) IsProcessed(_ context.Context, id uuid.UUID) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.processed[id], f.err
}

func (f *fakeRepo) GetBay(_ context.Context, branch string) (ticket.Bay, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if bay, ok := f.bays[branch]; ok {
		return bay, f.bayErr
	}
	return ticket.Bay{Branch: branch, Available: true}, f.bayErr
}

func (f *fakeRepo) SaveBay(_ context.Context, bay ticket.Bay) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	f.bays[bay.Branch] = bay
	return nil
}

func (f *fakeRepo) Commit(_ context.Context, c ticket.Change) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.commits++
	if len(f.commitErrs) > 0 {
		err := f.commitErrs[0]
		f.commitErrs = f.commitErrs[1:]
		if err != nil {
			return err
		}
	}
	if c.Inbound != nil && f.processed[c.Inbound.MessageID] {
		return ticket.ErrDuplicateMessage
	}
	if c.Ticket != nil {
		stored, exists := f.tickets[c.Ticket.WorkOrderID]
		if c.Create == exists || (exists && stored.Version != c.Ticket.Version) {
			return ticket.ErrConflict
		}
	}
	if c.RequireBayBranch != "" {
		if bay, ok := f.bays[c.RequireBayBranch]; ok && !bay.Available {
			return ticket.ErrBayUnavailable
		}
	}
	if c.Inbound != nil {
		f.processed[c.Inbound.MessageID] = true
	}
	if c.Ticket != nil {
		c.Ticket.Version++
		stored := clone(c.Ticket)
		if prev, ok := f.tickets[c.Ticket.WorkOrderID]; ok {
			stored.Timeline = append(append([]ticket.Event(nil), prev.Timeline...), c.Events...)
		} else {
			stored.Timeline = append([]ticket.Event(nil), c.Events...)
		}
		f.tickets[c.Ticket.WorkOrderID] = stored
	}
	f.outbox = append(f.outbox, c.Outbox...)
	return nil
}

func (f *fakeRepo) types() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, len(f.outbox))
	for i, m := range f.outbox {
		out[i] = m.Envelope.Type
	}
	return out
}

func (f *fakeRepo) last() messaging.OutboxMessage {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.outbox[len(f.outbox)-1]
}

func newService(repo Repository) *Service {
	s := New(repo, topic, slog.New(slog.NewTextHandler(io.Discard, nil)))
	s.now = func() time.Time { return clock }
	return s
}

func command(kind string, wo uuid.UUID, payload any) messaging.Envelope {
	env, err := messaging.NewSagaStart(kind, uuid.New(), wo, "req-1", payload)
	if err != nil {
		panic(err)
	}
	return env
}
