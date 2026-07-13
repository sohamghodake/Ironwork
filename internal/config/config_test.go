package config_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sohamghodake/ironwork/internal/config"
)

func TestLoadDefaults(t *testing.T) {
	cfg, err := config.Load("scheduler")
	require.NoError(t, err)

	assert.Equal(t, "scheduler", cfg.Component)
	assert.Equal(t, "scheduler", cfg.Instance)
	assert.Equal(t, ":9443", cfg.GRPCAddr)
	assert.Equal(t, ":8080", cfg.HTTPAddr)
	assert.Empty(t, cfg.Targets)
	assert.Equal(t, "certs/ca.pem", cfg.TLS.CAFile)
}

func TestLoadFromEnv(t *testing.T) {
	t.Setenv("IRONWORK_INSTANCE", "scheduler-2")
	t.Setenv("IRONWORK_GRPC_ADDR", ":7777")
	t.Setenv("IRONWORK_TARGETS", "scheduler-1=scheduler-1:9443, worker-1=worker-1:9443")
	t.Setenv("IRONWORK_TLS_CERT_FILE", "/certs/service.pem")
	t.Setenv("IRONWORK_LOG_LEVEL", "debug")

	cfg, err := config.Load("scheduler")
	require.NoError(t, err)

	assert.Equal(t, "scheduler-2", cfg.Instance)
	assert.Equal(t, ":7777", cfg.GRPCAddr)
	assert.Equal(t, map[string]string{
		"scheduler-1": "scheduler-1:9443",
		"worker-1":    "worker-1:9443",
	}, cfg.Targets)
	assert.Equal(t, "/certs/service.pem", cfg.TLS.CertFile)
	assert.Equal(t, "debug", cfg.LogLevel)
}

func TestLoadRejectsMalformedTargets(t *testing.T) {
	t.Setenv("IRONWORK_TARGETS", "scheduler-1")

	_, err := config.Load("scheduler")
	assert.ErrorContains(t, err, "IRONWORK_TARGETS")
}
