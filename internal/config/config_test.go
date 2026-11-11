package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadAppliesDefaults(t *testing.T) {
	cfg, err := Load()
	require.NoError(t, err)

	assert.Equal(t, "8080", cfg.Server.Port)
	assert.Equal(t, "sa-east-1", cfg.AWS.Region)
	assert.Equal(t, "execution-tickets", cfg.Store.TableName)
	assert.Equal(t, 20*time.Second, cfg.Messaging.PollWait)
}

func TestLoadReadsEnvironment(t *testing.T) {
	t.Setenv("SERVER_PORT", "9090")
	t.Setenv("AWS_ENDPOINT_URL", "http://localstack:4566")
	t.Setenv("DYNAMODB_TABLE_NAME", "oficina-homolog-execution")
	t.Setenv("MESSAGING_INBOX_QUEUE_URL", "http://localstack:4566/000000000000/execution-inbox.fifo")
	t.Setenv("MESSAGING_POLL_WAIT", "1s")

	cfg, err := Load()
	require.NoError(t, err)

	assert.Equal(t, "9090", cfg.Server.Port)
	assert.Equal(t, "http://localstack:4566", cfg.AWS.Endpoint)
	assert.Equal(t, "oficina-homolog-execution", cfg.Store.TableName)
	assert.Equal(t, "http://localstack:4566/000000000000/execution-inbox.fifo", cfg.Messaging.InboxQueueURL)
	assert.Equal(t, time.Second, cfg.Messaging.PollWait)
}
