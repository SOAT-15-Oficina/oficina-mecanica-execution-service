package routes

import (
	"context"
	"time"

	"github.com/SOAT-15-Oficina/oficina-mecanica-execution-service/docs"
	"github.com/gofiber/fiber/v3"
)

const Prefix = "/execution"

type DatabasePinger interface {
	Ping(context.Context) error
}

func Register(app *fiber.App, db DatabasePinger, svc ExecutionService, jwtSecret string) {
	registerHealth(app, db)
	group := app.Group(Prefix)
	registerSwagger(group)
	registerTickets(group, svc, jwtSecret)
}

func registerHealth(app *fiber.App, db DatabasePinger) {
	app.Get("/ping", func(c fiber.Ctx) error {
		return c.SendString("Pong")
	})

	app.Get("/ready", func(c fiber.Ctx) error {
		if db == nil {
			return c.SendStatus(fiber.StatusServiceUnavailable)
		}

		ctx, cancel := context.WithTimeout(c.Context(), 2*time.Second)
		defer cancel()
		if err := db.Ping(ctx); err != nil {
			return c.SendStatus(fiber.StatusServiceUnavailable)
		}
		return c.SendStatus(fiber.StatusOK)
	})
}

func registerSwagger(group fiber.Router) {
	group.Get("/docs/swagger.yaml", func(c fiber.Ctx) error {
		c.Set("Content-Type", "text/yaml")
		return c.Send(docs.Swagger)
	})

	group.Get("/swagger", func(c fiber.Ctx) error {
		c.Set("Content-Type", "text/html")
		return c.SendString(swaggerHTML)
	})
}

const swaggerHTML = `<!DOCTYPE html>
<html lang="pt-BR">
<head>
  <meta charset="UTF-8">
  <title>Swagger UI - Execution Service</title>
  <link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5.17.14/swagger-ui.css">
</head>
<body>
  <div id="swagger-ui"></div>
  <script src="https://unpkg.com/swagger-ui-dist@5.17.14/swagger-ui-bundle.js"></script>
  <script>
    SwaggerUIBundle({ url: "/api/execution/docs/swagger.yaml", dom_id: "#swagger-ui", deepLinking: true });
  </script>
</body>
</html>`
