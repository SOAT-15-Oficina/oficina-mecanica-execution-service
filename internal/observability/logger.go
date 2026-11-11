package observability

import (
	"context"
	"io"
	"log/slog"
	"os"

	slogtrace "github.com/DataDog/dd-trace-go/contrib/log/slog/v2"
)

const DefaultService = "execution-service"

var version = "dev"

const (
	KeyService     = "service"
	KeyEnv         = "env"
	KeyVersion     = "version"
	KeyRequestID   = "request_id"
	KeyRoute       = "route"
	KeyMethod      = "method"
	KeyStatus      = "status"
	KeyDurationMS  = "duration_ms"
	KeyEvent       = "event"
	KeyIntegration = "integration"
	KeyError       = "error"

	KeySagaID      = "saga_id"
	KeyWorkOrderID = "work_order_id"
	KeyMessageID   = "message_id"
	KeyMessageType = "message_type"
	KeyTicketID    = "ticket_id"
	KeyBranch      = "branch"
)

const (
	IntegrationDynamoDB = "dynamodb"
	IntegrationSQS      = "sqs"
	IntegrationSNS      = "sns"
)

const EnvLocal = "local"

func Setup() *slog.Logger {
	logger := New(os.Stdout)
	slog.SetDefault(logger)
	return logger
}

func New(w io.Writer) *slog.Logger {
	env := Env()

	var handler slog.Handler
	if env == EnvLocal {
		handler = slog.NewTextHandler(w, &slog.HandlerOptions{Level: slog.LevelDebug})
	} else {
		handler = slogtrace.WrapHandler(slog.NewJSONHandler(w, &slog.HandlerOptions{Level: slog.LevelInfo}))
	}

	return slog.New(handler).With(
		slog.String(KeyService, envOr("DD_SERVICE", DefaultService)),
		slog.String(KeyEnv, env),
		slog.String(KeyVersion, version),
	)
}

func Env() string {
	if v := os.Getenv("DD_ENV"); v != "" {
		return v
	}
	return envOr("SERVER_ENVIRONMENT", EnvLocal)
}

func Version() string { return version }

type loggerKey struct{}

func WithLogger(ctx context.Context, logger *slog.Logger) context.Context {
	return context.WithValue(ctx, loggerKey{}, logger)
}

func FromContext(ctx context.Context) *slog.Logger {
	if logger, ok := ctx.Value(loggerKey{}).(*slog.Logger); ok && logger != nil {
		return logger
	}
	return slog.Default()
}

func Event(name string) slog.Attr { return slog.String(KeyEvent, name) }

func Integration(name string) slog.Attr { return slog.String(KeyIntegration, name) }

func Err(err error) slog.Attr {
	if err == nil {
		return slog.Attr{}
	}
	return slog.String(KeyError, err.Error())
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
