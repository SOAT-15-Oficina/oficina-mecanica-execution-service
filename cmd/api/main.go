package main

import (
	"context"
	"log/slog"
	"os"

	"github.com/SOAT-15-Oficina/oficina-mecanica-execution-service/internal/config"
	"github.com/SOAT-15-Oficina/oficina-mecanica-execution-service/internal/observability"
	"github.com/SOAT-15-Oficina/oficina-mecanica-execution-service/internal/routes"
	"github.com/SOAT-15-Oficina/oficina-mecanica-execution-service/internal/store"
	"github.com/gofiber/fiber/v3"
)

func main() {
	observability.Setup()

	cfg, err := config.Load()
	if err != nil {
		exit(err, "failed to load configuration")
	}

	stopTracer := observability.StartTracer(slog.Default())
	defer stopTracer()

	client, err := store.NewClient(context.Background(), cfg.AWS)
	if err != nil {
		exit(err, "failed to create dynamodb client", observability.Integration(observability.IntegrationDynamoDB))
	}

	app := fiber.New()
	routes.Register(app, store.New(client, cfg.Store.TableName))

	if err := app.Listen(":" + cfg.Server.Port); err != nil {
		exit(err, "failed to start server")
	}
}

func exit(err error, message string, attrs ...slog.Attr) {
	slog.LogAttrs(context.Background(), slog.LevelError, message, append(attrs, observability.Err(err))...)
	os.Exit(1)
}
