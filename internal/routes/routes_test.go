package routes

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/SOAT-15-Oficina/oficina-mecanica-execution-service/docs"
	"github.com/gofiber/fiber/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

type stubPinger struct{ err error }

func (s stubPinger) Ping(context.Context) error { return s.err }

func do(t *testing.T, app *fiber.App, path string) *http.Response {
	t.Helper()
	resp, err := app.Test(httptest.NewRequest(http.MethodGet, path, nil))
	require.NoError(t, err)
	return resp
}

func TestPing(t *testing.T) {
	app := fiber.New()
	Register(app, nil)

	resp := do(t, app, "/ping")
	body, _ := io.ReadAll(resp.Body)
	assert.Equal(t, fiber.StatusOK, resp.StatusCode)
	assert.Equal(t, "Pong", string(body))
}

func TestReadinessReflectsDatabase(t *testing.T) {
	tests := []struct {
		name   string
		pinger DatabasePinger
		status int
	}{
		{"available", stubPinger{}, fiber.StatusOK},
		{"unavailable", stubPinger{err: errors.New("down")}, fiber.StatusServiceUnavailable},
		{"missing", nil, fiber.StatusServiceUnavailable},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			app := fiber.New()
			Register(app, tt.pinger)
			assert.Equal(t, tt.status, do(t, app, "/ready").StatusCode)
		})
	}
}

func TestSwaggerIsServedUnderPrefix(t *testing.T) {
	app := fiber.New()
	Register(app, nil)

	resp := do(t, app, Prefix+"/docs/swagger.yaml")
	body, _ := io.ReadAll(resp.Body)
	assert.Equal(t, fiber.StatusOK, resp.StatusCode)
	assert.Equal(t, docs.Swagger, body)

	resp = do(t, app, Prefix+"/swagger")
	assert.Equal(t, fiber.StatusOK, resp.StatusCode)
	assert.Contains(t, resp.Header.Get("Content-Type"), "text/html")
}

func TestContractMatchesRegisteredRoutes(t *testing.T) {
	var contract struct {
		Paths map[string]map[string]any `yaml:"paths"`
	}
	require.NoError(t, yaml.Unmarshal(docs.Swagger, &contract))

	app := fiber.New()
	Register(app, nil)

	registered := map[string]bool{}
	for _, r := range app.GetRoutes(true) {
		if r.Method == http.MethodHead {
			continue
		}
		registered[strings.ToLower(r.Method)+" "+fiberToOpenAPI(r.Path)] = true
	}

	documented := map[string]bool{}
	for path, ops := range contract.Paths {
		for method := range ops {
			documented[method+" "+path] = true
		}
	}

	assert.Equal(t, documented, registered)
}

func fiberToOpenAPI(path string) string {
	parts := strings.Split(path, "/")
	for i, p := range parts {
		if strings.HasPrefix(p, ":") {
			parts[i] = "{" + strings.TrimPrefix(p, ":") + "}"
		}
	}
	return strings.Join(parts, "/")
}
