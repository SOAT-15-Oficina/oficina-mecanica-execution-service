package routes

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"regexp"
	"strings"
	"time"

	"github.com/SOAT-15-Oficina/oficina-mecanica-execution-service/internal/auth"
	"github.com/SOAT-15-Oficina/oficina-mecanica-execution-service/internal/messaging"
	"github.com/SOAT-15-Oficina/oficina-mecanica-execution-service/internal/routes/middlewares"
	"github.com/SOAT-15-Oficina/oficina-mecanica-execution-service/internal/ticket"
	"github.com/gofiber/fiber/v3"
	"github.com/google/uuid"
)

const maxTextLength = 2000

var branchPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,40}$`)

type ExecutionService interface {
	Queue(ctx context.Context, branch string, phase ticket.Phase) ([]ticket.Ticket, error)
	Ticket(ctx context.Context, workOrderID uuid.UUID) (*ticket.Ticket, error)
	StartDiagnosis(ctx context.Context, workOrderID uuid.UUID, technician, actor string) (*ticket.Ticket, error)
	CompleteDiagnosis(ctx context.Context, workOrderID uuid.UUID, notes, actor string) (*ticket.Ticket, error)
	StartRepair(ctx context.Context, workOrderID uuid.UUID, technician, actor string) (*ticket.Ticket, error)
	StartItem(ctx context.Context, workOrderID, itemID uuid.UUID, actor string) (*ticket.Ticket, error)
	FinishItem(ctx context.Context, workOrderID, itemID uuid.UUID, actor string) (*ticket.Ticket, error)
	Bay(ctx context.Context, branch string) (ticket.Bay, error)
	SetBayAvailability(ctx context.Context, branch string, available bool, actor string) (ticket.Bay, error)
}

type ticketHandler struct {
	svc ExecutionService
}

type queueEntry struct {
	Position      int               `json:"position"`
	WorkOrderID   uuid.UUID         `json:"work_order_id"`
	WorkOrderCode string            `json:"work_order_code,omitempty"`
	Title         string            `json:"title,omitempty"`
	Status        ticket.Status     `json:"status"`
	Vehicle       messaging.Vehicle `json:"vehicle"`
	EnqueuedAt    *time.Time        `json:"enqueued_at,omitempty"`
}

type technicianRequest struct {
	Technician string `json:"technician"`
}

type notesRequest struct {
	Notes string `json:"notes"`
}

type availabilityRequest struct {
	Available *bool `json:"available"`
}

func registerTickets(group fiber.Router, svc ExecutionService, jwtSecret string) {
	h := &ticketHandler{svc: svc}
	authn := middlewares.Auth(jwtSecret)
	roles := middlewares.RequireRoles(middlewares.RoleAdmin, middlewares.RoleEmployee)

	group.Get("/queue", authn, roles, h.queue)
	group.Get("/tickets/:workOrderId", authn, roles, h.get)
	group.Post("/tickets/:workOrderId/diagnosis/start", authn, roles, h.startDiagnosis)
	group.Post("/tickets/:workOrderId/diagnosis/complete", authn, roles, h.completeDiagnosis)
	group.Post("/tickets/:workOrderId/repair/start", authn, roles, h.startRepair)
	group.Post("/tickets/:workOrderId/items/:workOrderServiceId/start", authn, roles, h.startItem)
	group.Post("/tickets/:workOrderId/items/:workOrderServiceId/finish", authn, roles, h.finishItem)
	group.Get("/bays/:branch", authn, roles, h.bay)
	group.Put("/bays/:branch/availability", authn, roles, h.setAvailability)
}

func badRequest(c fiber.Ctx, msg string) error {
	return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": msg})
}

func fail(c fiber.Ctx, err error) error {
	switch {
	case errors.Is(err, ticket.ErrNotFound):
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"error": "ticket not found"})
	case errors.Is(err, ticket.ErrItemNotFound):
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"error": "item not found"})
	case errors.Is(err, ticket.ErrInvalidTransition):
		return c.Status(fiber.StatusConflict).JSON(fiber.Map{"error": "transição inválida para o status atual da ficha"})
	case errors.Is(err, ticket.ErrConflict):
		return c.Status(fiber.StatusConflict).JSON(fiber.Map{"error": "a ficha foi alterada por outra operação, tente novamente"})
	}
	slog.ErrorContext(c.Context(), "execution: request failed", slog.String("error", err.Error()), slog.String("route", c.Path()))
	return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "internal error"})
}

func actor(c fiber.Ctx) string {
	if claims, ok := c.Locals("token").(*auth.AppClaims); ok {
		return claims.User
	}
	return ""
}

func uuidParam(c fiber.Ctx, name string) (uuid.UUID, bool) {
	id, err := uuid.Parse(c.Params(name))
	return id, err == nil
}

func decode(c fiber.Ctx, target any) bool {
	body := c.Body()
	if len(body) == 0 {
		return true
	}
	return json.Unmarshal(body, target) == nil
}

func (h *ticketHandler) queue(c fiber.Ctx) error {
	branch := c.Query("branch")
	if !branchPattern.MatchString(branch) {
		return badRequest(c, "branch is required")
	}
	phase, ok := ticket.ParsePhase(c.Query("phase"))
	if !ok {
		return badRequest(c, "phase must be DIAGNOSTICO or REPARO")
	}

	tickets, err := h.svc.Queue(c.Context(), branch, phase)
	if err != nil {
		return fail(c, err)
	}
	entries := make([]queueEntry, 0, len(tickets))
	for i, t := range tickets {
		entries = append(entries, queueEntry{
			Position:      i + 1,
			WorkOrderID:   t.WorkOrderID,
			WorkOrderCode: t.WorkOrderCode,
			Title:         t.Title,
			Status:        t.Status,
			Vehicle:       t.Vehicle,
			EnqueuedAt:    t.EnqueuedAt,
		})
	}
	return c.JSON(fiber.Map{"branch": branch, "phase": phase, "entries": entries})
}

func (h *ticketHandler) get(c fiber.Ctx) error {
	wo, ok := uuidParam(c, "workOrderId")
	if !ok {
		return badRequest(c, "invalid work order id")
	}
	t, err := h.svc.Ticket(c.Context(), wo)
	if err != nil {
		return fail(c, err)
	}
	return c.JSON(t)
}

func (h *ticketHandler) withTechnician(c fiber.Ctx, run func(context.Context, uuid.UUID, string, string) (*ticket.Ticket, error)) error {
	wo, ok := uuidParam(c, "workOrderId")
	if !ok {
		return badRequest(c, "invalid work order id")
	}
	var req technicianRequest
	if !decode(c, &req) {
		return badRequest(c, "invalid body")
	}
	technician := strings.TrimSpace(req.Technician)
	if technician == "" || len(technician) > 120 {
		return badRequest(c, "technician is required")
	}
	t, err := run(c.Context(), wo, technician, actor(c))
	if err != nil {
		return fail(c, err)
	}
	return c.JSON(t)
}

func (h *ticketHandler) startDiagnosis(c fiber.Ctx) error {
	return h.withTechnician(c, h.svc.StartDiagnosis)
}

func (h *ticketHandler) startRepair(c fiber.Ctx) error {
	return h.withTechnician(c, h.svc.StartRepair)
}

func (h *ticketHandler) completeDiagnosis(c fiber.Ctx) error {
	wo, ok := uuidParam(c, "workOrderId")
	if !ok {
		return badRequest(c, "invalid work order id")
	}
	var req notesRequest
	if !decode(c, &req) {
		return badRequest(c, "invalid body")
	}
	notes := strings.TrimSpace(req.Notes)
	if len(notes) > maxTextLength {
		return badRequest(c, "notes too long")
	}
	t, err := h.svc.CompleteDiagnosis(c.Context(), wo, notes, actor(c))
	if err != nil {
		return fail(c, err)
	}
	return c.JSON(t)
}

func (h *ticketHandler) withItem(c fiber.Ctx, run func(context.Context, uuid.UUID, uuid.UUID, string) (*ticket.Ticket, error)) error {
	wo, ok := uuidParam(c, "workOrderId")
	if !ok {
		return badRequest(c, "invalid work order id")
	}
	item, ok := uuidParam(c, "workOrderServiceId")
	if !ok {
		return badRequest(c, "invalid work order service id")
	}
	t, err := run(c.Context(), wo, item, actor(c))
	if err != nil {
		return fail(c, err)
	}
	return c.JSON(t)
}

func (h *ticketHandler) startItem(c fiber.Ctx) error {
	return h.withItem(c, h.svc.StartItem)
}

func (h *ticketHandler) finishItem(c fiber.Ctx) error {
	return h.withItem(c, h.svc.FinishItem)
}

func (h *ticketHandler) bay(c fiber.Ctx) error {
	branch := c.Params("branch")
	if !branchPattern.MatchString(branch) {
		return badRequest(c, "invalid branch")
	}
	bay, err := h.svc.Bay(c.Context(), branch)
	if err != nil {
		return fail(c, err)
	}
	return c.JSON(bay)
}

func (h *ticketHandler) setAvailability(c fiber.Ctx) error {
	branch := c.Params("branch")
	if !branchPattern.MatchString(branch) {
		return badRequest(c, "invalid branch")
	}
	var req availabilityRequest
	if len(c.Body()) == 0 || !decode(c, &req) || req.Available == nil {
		return badRequest(c, "available is required")
	}
	bay, err := h.svc.SetBayAvailability(c.Context(), branch, *req.Available, actor(c))
	if err != nil {
		return fail(c, err)
	}
	return c.JSON(bay)
}
