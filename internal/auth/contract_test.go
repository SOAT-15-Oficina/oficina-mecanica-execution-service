package auth

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	contractSecret   = "contract-test-secret"
	contractUser     = "contract-user"
	contractRole     = "admin"
	contractGoldenAt = "testdata/token.golden"
)

func readGoldenToken(t *testing.T) string {
	t.Helper()

	raw, err := os.ReadFile(contractGoldenAt)
	require.NoError(t, err, "fixture de contrato ausente; copie de oficina-mecanica-serverless")

	return strings.TrimSpace(string(raw))
}

func TestContract_ParsesTokenIssuedByServerless(t *testing.T) {
	claims, err := ParseToken(readGoldenToken(t), contractSecret)

	require.NoError(t, err, "o token emitido pela Lambda deixou de ser aceito aqui")
	assert.Equal(t, contractUser, claims.User, `claim "user" divergiu entre os repos`)
	assert.Equal(t, contractRole, claims.Role, `claim "role" divergiu entre os repos`)
}

func TestContract_GoldenTokenIsHS256(t *testing.T) {
	parts := strings.Split(readGoldenToken(t), ".")
	require.Len(t, parts, 3, "token golden malformado")
	assert.Equal(t, "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9", parts[0])
}
