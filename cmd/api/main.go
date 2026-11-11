package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/SOAT-15-Oficina/oficina-mecanica-execution-service/internal/config"
	"github.com/SOAT-15-Oficina/oficina-mecanica-execution-service/internal/execution"
	"github.com/SOAT-15-Oficina/oficina-mecanica-execution-service/internal/messaging"
	"github.com/SOAT-15-Oficina/oficina-mecanica-execution-service/internal/observability"
	"github.com/SOAT-15-Oficina/oficina-mecanica-execution-service/internal/routes"
	"github.com/SOAT-15-Oficina/oficina-mecanica-execution-service/internal/store"
	"github.com/gofiber/fiber/v3"
)

const shutdownTimeout = 10 * time.Second

func main() {
	logger := observability.Setup()

	cfg, err := config.Load()
	if err != nil {
		exit(err, "failed to load configuration")
	}

	stopTracer := observability.StartTracer(logger)
	defer stopTracer()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	client, err := store.NewClient(ctx, cfg.AWS)
	if err != nil {
		exit(err, "failed to create dynamodb client", observability.Integration(observability.IntegrationDynamoDB))
	}
	repo := store.New(client, cfg.Store.TableName)
	svc := execution.New(repo, cfg.Messaging.EventsTopicARN, logger)

	sqsClient, snsClient, err := messaging.NewClients(ctx, cfg.AWS.Region, cfg.AWS.Endpoint)
	if err != nil {
		exit(err, "failed to create messaging clients", observability.Integration(observability.IntegrationSQS))
	}

	workers, stopWorkers := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	if cfg.Messaging.InboxQueueURL != "" {
		consumer := messaging.NewConsumer(sqsClient, cfg.Messaging.InboxQueueURL, cfg.Messaging.PollWait, svc.HandleMessage, logger)
		wg.Go(func() { consumer.Run(workers) })
	} else {
		logger.Warn("MESSAGING_INBOX_QUEUE_URL is empty, inbox consumer disabled")
	}
	if cfg.Messaging.EventsTopicARN == "" {
		logger.Warn("MESSAGING_EVENTS_TOPIC_ARN is empty, outbox messages will fail to publish")
	}
	relay := messaging.NewRelay(repo, messaging.NewAWSPublisher(sqsClient, snsClient), cfg.Messaging.OutboxInterval, logger)
	wg.Go(func() { relay.Run(workers) })

	app := fiber.New()
	routes.Register(app, repo, svc, cfg.Auth.JWTSecretKey)

	go func() {
		<-ctx.Done()
		logger.Info("shutting down", observability.Event("server.shutdown"))
		if err := app.ShutdownWithTimeout(shutdownTimeout); err != nil {
			logger.Error("http shutdown failed", observability.Err(err))
		}
	}()

	listenErr := app.Listen(":" + cfg.Server.Port)
	stopWorkers()
	wg.Wait()
	if listenErr != nil {
		exit(listenErr, "failed to start server")
	}
}

func exit(err error, message string, attrs ...slog.Attr) {
	slog.LogAttrs(context.Background(), slog.LevelError, message, append(attrs, observability.Err(err))...)
	os.Exit(1)
}
