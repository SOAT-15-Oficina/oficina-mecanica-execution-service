package routes

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/SOAT-15-Oficina/oficina-mecanica-execution-service/internal/auth"
	"github.com/SOAT-15-Oficina/oficina-mecanica-execution-service/internal/ticket"
	"github.com/gofiber/fiber/v3"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const secret = "test-secret"

type call struct {
	method    string
	wo, item  uuid.UUID
	text      string
	actor     string
	branch    string
	phase     ticket.Phase
	available bool
}

type stubService struct {
	calls []call
	err   error
	queue []ticket.Ticket
}

func (s *stubService) record(c call) (*ticket.Ticket, error) {
	s.calls = append(s.calls, c)
	if s.err != nil {
		return nil, s.err
	}
	return &ticket.Ticket{WorkOrderID: c.wo, Status: ticket.StatusDiagnosing, Items: []ticket.Item{}, Timeline: []ticket.Event{}}, nil
}

func (s *stubService) Queue(_ context.Context, branch string, phase ticket.Phase) ([]ticket.Ticket, error) {
	s.calls = append(s.calls, call{method: "queue", branch: branch, phase: phase})
	return s.queue, s.err
}

func (s *stubService) Ticket(_ context.Context, wo uuid.UUID) (*ticket.Ticket, error) {
	return s.record(call{method: "ticket", wo: wo})
}

func (s *stubService) StartDiagnosis(_ context.Context, wo uuid.UUID, technician, actor string) (*ticket.Ticket, error) {
	return s.record(call{method: "startDiagnosis", wo: wo, text: technician, actor: actor})
}

func (s *stubService) CompleteDiagnosis(_ context.Context, wo uuid.UUID, notes, actor string) (*ticket.Ticket, error) {
	return s.record(call{method: "completeDiagnosis", wo: wo, text: notes, actor: actor})
}

func (s *stubService) StartRepair(_ context.Context, wo uuid.UUID, technician, actor string) (*ticket.Ticket, error) {
	return s.record(call{method: "startRepair", wo: wo, text: technician, actor: actor})
}

func (s *stubService) StartItem(_ context.Context, wo, item uuid.UUID, actor string) (*ticket.Ticket, error) {
	return s.record(call{method: "startItem", wo: wo, item: item, actor: actor})
}

func (s *stubService) FinishItem(_ context.Context, wo, item uuid.UUID, actor string) (*ticket.Ticket, error) {
	return s.record(call{method: "finishItem", wo: wo, item: item, actor: actor})
}

func (s *stubService) Bay(_ context.Context, branch string) (ticket.Bay, error) {
	s.calls = append(s.calls, call{method: "bay", branch: branch})
	return ticket.Bay{Branch: branch, Available: true}, s.err
}

func (s *stubService) SetBayAvailability(_ context.Context, branch string, available bool, actor string) (ticket.Bay, error) {
	s.calls = append(s.calls, call{method: "setBay", branch: branch, available: available, actor: actor})
	return ticket.Bay{Branch: branch, Available: available}, s.err
}

func newTicketApp(svc *stubService) *fiber.App {
	app := fiber.New()
	Register(app, nil, svc, secret)
	return app
}

func request(t *testing.T, app *fiber.App, method, path, body, role string) (*http.Response, map[string]any) {
	t.Helper()
	var reader io.Reader
	if body != "" {
		reader = strings.NewReader(body)
	}
	req := httptest.NewRequest(method, path, reader)
	req.Header.Set("Content-Type", "application/json")
	if role != "" {
		token, err := auth.GenerateToken("operador", role, secret)
		require.NoError(t, err)
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := app.Test(req)
	require.NoError(t, err)
	raw, _ := io.ReadAll(resp.Body)
	var out map[string]any
	_ = json.Unmarshal(raw, &out)
	return resp, out
}

func TestTicketRoutesRequireOperatorJWT(t *testing.T) {
	app := newTicketApp(&stubService{})
	resp, _ := request(t, app, http.MethodGet, "/execution/queue?branch=MATRIZ&phase=REPARO", "", "")
	assert.Equal(t, fiber.StatusUnauthorized, resp.StatusCode)
	resp, _ = request(t, app, http.MethodGet, "/execution/queue?branch=MATRIZ&phase=REPARO", "", "customer")
	assert.Equal(t, fiber.StatusForbidden, resp.StatusCode)
	resp, _ = request(t, app, http.MethodGet, "/execution/queue?branch=MATRIZ&phase=REPARO", "", "employee")
	assert.Equal(t, fiber.StatusOK, resp.StatusCode)
	resp, _ = request(t, app, http.MethodGet, "/execution/swagger", "", "")
	assert.Equal(t, fiber.StatusOK, resp.StatusCode)
}

func TestQueueRoute(t *testing.T) {
	svc := &stubService{queue: []ticket.Ticket{{WorkOrderID: uuid.New(), Status: ticket.StatusRepairQueued}, {WorkOrderID: uuid.New()}}}
	app := newTicketApp(svc)

	resp, body := request(t, app, http.MethodGet, "/execution/queue?branch=MATRIZ&phase=REPARO", "", "admin")
	assert.Equal(t, fiber.StatusOK, resp.StatusCode)
	entries := body["entries"].([]any)
	require.Len(t, entries, 2)
	assert.EqualValues(t, 2, entries[1].(map[string]any)["position"])
	assert.Equal(t, "REPARO_NA_FILA", entries[0].(map[string]any)["status"])
	assert.Equal(t, ticket.PhaseRepair, svc.calls[0].phase)

	resp, _ = request(t, app, http.MethodGet, "/execution/queue?phase=REPARO", "", "admin")
	assert.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	resp, _ = request(t, app, http.MethodGet, "/execution/queue?branch=MATRIZ&phase=X", "", "admin")
	assert.Equal(t, fiber.StatusBadRequest, resp.StatusCode)

	svc.err = errors.New("boom")
	resp, _ = request(t, app, http.MethodGet, "/execution/queue?branch=MATRIZ&phase=DIAGNOSTICO", "", "admin")
	assert.Equal(t, fiber.StatusInternalServerError, resp.StatusCode)
}

func TestTicketActionRoutes(t *testing.T) {
	wo, item := uuid.New(), uuid.New()
	base := "/execution/tickets/" + wo.String()
	tests := []struct {
		method, path, body, want, text string
	}{
		{http.MethodGet, base, "", "ticket", ""},
		{http.MethodPost, base + "/diagnosis/start", `{"technician":" Joao "}`, "startDiagnosis", "Joao"},
		{http.MethodPost, base + "/diagnosis/complete", `{"notes":"ok"}`, "completeDiagnosis", "ok"},
		{http.MethodPost, base + "/diagnosis/complete", "", "completeDiagnosis", ""},
		{http.MethodPost, base + "/repair/start", `{"technician":"Bia"}`, "startRepair", "Bia"},
		{http.MethodPost, base + "/items/" + item.String() + "/start", "", "startItem", ""},
		{http.MethodPost, base + "/items/" + item.String() + "/finish", "", "finishItem", ""},
	}
	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			svc := &stubService{}
			resp, body := request(t, newTicketApp(svc), tt.method, tt.path, tt.body, "employee")
			assert.Equal(t, fiber.StatusOK, resp.StatusCode)
			assert.Equal(t, wo.String(), body["work_order_id"])
			require.Len(t, svc.calls, 1)
			got := svc.calls[0]
			assert.Equal(t, tt.want, got.method)
			assert.Equal(t, wo, got.wo)
			assert.Equal(t, tt.text, got.text)
			if tt.method == http.MethodPost {
				assert.Equal(t, "operador", got.actor)
			}
			if strings.Contains(tt.path, "/items/") {
				assert.Equal(t, item, got.item)
			}
		})
	}
}

func TestTicketActionValidation(t *testing.T) {
	wo := uuid.New().String()
	tests := []struct {
		method, path, body string
	}{
		{http.MethodGet, "/execution/tickets/nope", ""},
		{http.MethodPost, "/execution/tickets/nope/diagnosis/start", `{"technician":"x"}`},
		{http.MethodPost, "/execution/tickets/" + wo + "/diagnosis/start", `{"technician":"  "}`},
		{http.MethodPost, "/execution/tickets/" + wo + "/diagnosis/start", `{`},
		{http.MethodPost, "/execution/tickets/" + wo + "/repair/start", ""},
		{http.MethodPost, "/execution/tickets/nope/diagnosis/complete", ""},
		{http.MethodPost, "/execution/tickets/" + wo + "/diagnosis/complete", `[`},
		{http.MethodPost, "/execution/tickets/" + wo + "/diagnosis/complete", `{"notes":"` + strings.Repeat("a", maxTextLength+1) + `"}`},
		{http.MethodPost, "/execution/tickets/nope/items/" + wo + "/start", ""},
		{http.MethodPost, "/execution/tickets/" + wo + "/items/nope/finish", ""},
		{http.MethodGet, "/execution/bays/a%20b", ""},
		{http.MethodPut, "/execution/bays/a%20b/availability", `{"available":true}`},
		{http.MethodPut, "/execution/bays/MATRIZ/availability", ""},
		{http.MethodPut, "/execution/bays/MATRIZ/availability", `{}`},
		{http.MethodPut, "/execution/bays/MATRIZ/availability", `{"available":"x"}`},
	}
	for _, tt := range tests {
		svc := &stubService{}
		resp, body := request(t, newTicketApp(svc), tt.method, tt.path, tt.body, "admin")
		assert.Equal(t, fiber.StatusBadRequest, resp.StatusCode, tt.path+" "+tt.body)
		assert.NotEmpty(t, body["error"])
		assert.Empty(t, svc.calls)
	}
}

func TestTicketErrorMapping(t *testing.T) {
	tests := []struct {
		err    error
		status int
	}{
		{ticket.ErrNotFound, fiber.StatusNotFound},
		{ticket.ErrItemNotFound, fiber.StatusNotFound},
		{ticket.ErrInvalidTransition, fiber.StatusConflict},
		{ticket.ErrConflict, fiber.StatusConflict},
		{errors.New("boom"), fiber.StatusInternalServerError},
	}
	wo, item := uuid.New().String(), uuid.New().String()
	paths := []struct{ method, path, body string }{
		{http.MethodGet, "/execution/tickets/" + wo, ""},
		{http.MethodPost, "/execution/tickets/" + wo + "/diagnosis/start", `{"technician":"x"}`},
		{http.MethodPost, "/execution/tickets/" + wo + "/diagnosis/complete", ""},
		{http.MethodPost, "/execution/tickets/" + wo + "/items/" + item + "/finish", ""},
	}
	for _, tt := range tests {
		for _, p := range paths {
			resp, _ := request(t, newTicketApp(&stubService{err: tt.err}), p.method, p.path, p.body, "admin")
			assert.Equal(t, tt.status, resp.StatusCode, p.path)
		}
	}
}

func TestBayRoutes(t *testing.T) {
	svc := &stubService{}
	app := newTicketApp(svc)

	resp, body := request(t, app, http.MethodGet, "/execution/bays/MATRIZ", "", "admin")
	assert.Equal(t, fiber.StatusOK, resp.StatusCode)
	assert.Equal(t, true, body["available"])

	resp, body = request(t, app, http.MethodPut, "/execution/bays/MATRIZ/availability", `{"available":false}`, "admin")
	assert.Equal(t, fiber.StatusOK, resp.StatusCode)
	assert.Equal(t, false, body["available"])
	assert.Equal(t, call{method: "setBay", branch: "MATRIZ", actor: "operador"}, svc.calls[1])

	svc.err = errors.New("boom")
	resp, _ = request(t, app, http.MethodGet, "/execution/bays/MATRIZ", "", "admin")
	assert.Equal(t, fiber.StatusInternalServerError, resp.StatusCode)
	resp, _ = request(t, app, http.MethodPut, "/execution/bays/MATRIZ/availability", `{"available":true}`, "admin")
	assert.Equal(t, fiber.StatusInternalServerError, resp.StatusCode)
}
