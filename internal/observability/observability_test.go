package observability

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewUsesTextHandlerLocally(t *testing.T) {
	t.Setenv("DD_ENV", "")
	t.Setenv("SERVER_ENVIRONMENT", "")

	var buf bytes.Buffer
	New(&buf).Info("hello")

	assert.Contains(t, buf.String(), "service="+DefaultService)
	assert.Contains(t, buf.String(), "env=local")
}

func TestNewUsesJSONOutsideLocal(t *testing.T) {
	t.Setenv("DD_ENV", "homolog")
	t.Setenv("DD_SERVICE", "execution")

	var buf bytes.Buffer
	New(&buf).Info("hello", Event("ticket.enqueued"), Integration(IntegrationSQS))

	var line map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &line))
	assert.Equal(t, "execution", line[KeyService])
	assert.Equal(t, "homolog", line[KeyEnv])
	assert.Equal(t, "ticket.enqueued", line[KeyEvent])
	assert.Equal(t, IntegrationSQS, line[KeyIntegration])
}

func TestEnvPrefersDatadogVariable(t *testing.T) {
	t.Setenv("DD_ENV", "prod")
	t.Setenv("SERVER_ENVIRONMENT", "development")
	assert.Equal(t, "prod", Env())

	t.Setenv("DD_ENV", "")
	assert.Equal(t, "development", Env())
}

func TestLoggerTravelsInContext(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))
	ctx := WithLogger(context.Background(), logger)

	assert.Same(t, logger, FromContext(ctx))
	assert.Same(t, slog.Default(), FromContext(context.Background()))
}

func TestErr(t *testing.T) {
	assert.Equal(t, slog.Attr{}, Err(nil))
	assert.Equal(t, "boom", Err(errors.New("boom")).Value.String())
}

func TestSetupReplacesDefaultLogger(t *testing.T) {
	previous := slog.Default()
	t.Cleanup(func() { slog.SetDefault(previous) })

	logger := Setup()
	assert.Same(t, logger, slog.Default())
	assert.Equal(t, "dev", Version())
}

func TestStartTracerDisabled(t *testing.T) {
	t.Setenv("DD_TRACE_ENABLED", "false")
	stop := StartTracer(slog.Default())
	assert.NotPanics(t, stop)
}
