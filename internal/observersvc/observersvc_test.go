package observersvc_test

import (
	"context"
	"errors"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ironworkv1 "github.com/sohamghodake/ironwork/gen/ironwork/v1"
	"github.com/sohamghodake/ironwork/internal/observersvc"
)

type pinger struct{ err error }

func (p pinger) Ping(context.Context) error { return p.err }

func TestClusterCheckAllHealthy(t *testing.T) {
	targets := map[string]string{
		"scheduler-1": "scheduler-1:9443",
		"worker-1":    "worker-1:9443",
	}
	check := func(context.Context, string) error { return nil }
	svc := observersvc.New("observer", targets, pinger{}, check, zerolog.Nop())

	resp, err := svc.ClusterCheck(context.Background(), &ironworkv1.ClusterCheckRequest{})
	require.NoError(t, err)

	assert.Equal(t, ironworkv1.ServingStatus_SERVING_STATUS_SERVING, resp.Status)
	require.Len(t, resp.Components, 3) // 2 targets + postgres
	require.NotNil(t, resp.CheckedAt)

	// Components are sorted by name for stable output.
	names := make([]string, 0, len(resp.Components))
	for _, c := range resp.Components {
		names = append(names, c.Name)
		assert.Equal(t, ironworkv1.ServingStatus_SERVING_STATUS_SERVING, c.Status)
	}
	assert.Equal(t, []string{"postgres-primary", "scheduler-1", "worker-1"}, names)
}

func TestClusterCheckReportsFailingTarget(t *testing.T) {
	targets := map[string]string{
		"scheduler-1": "scheduler-1:9443",
		"worker-1":    "worker-1:9443",
	}
	check := func(_ context.Context, addr string) error {
		if addr == "worker-1:9443" {
			return errors.New("connection refused")
		}
		return nil
	}
	svc := observersvc.New("observer", targets, pinger{}, check, zerolog.Nop())

	resp, err := svc.ClusterCheck(context.Background(), &ironworkv1.ClusterCheckRequest{})
	require.NoError(t, err)

	assert.Equal(t, ironworkv1.ServingStatus_SERVING_STATUS_NOT_SERVING, resp.Status)
	byName := map[string]*ironworkv1.ComponentHealth{}
	for _, c := range resp.Components {
		byName[c.Name] = c
	}
	assert.Equal(t, ironworkv1.ServingStatus_SERVING_STATUS_NOT_SERVING, byName["worker-1"].Status)
	assert.Contains(t, byName["worker-1"].Message, "connection refused")
	assert.Equal(t, ironworkv1.ServingStatus_SERVING_STATUS_SERVING, byName["scheduler-1"].Status)
}

func TestClusterCheckReportsDatabaseFailure(t *testing.T) {
	svc := observersvc.New("observer", nil, pinger{err: errors.New("dial tcp: refused")}, nil, zerolog.Nop())

	resp, err := svc.ClusterCheck(context.Background(), &ironworkv1.ClusterCheckRequest{})
	require.NoError(t, err)

	assert.Equal(t, ironworkv1.ServingStatus_SERVING_STATUS_NOT_SERVING, resp.Status)
	require.Len(t, resp.Components, 1)
	assert.Equal(t, "postgres-primary", resp.Components[0].Name)
	assert.Contains(t, resp.Components[0].Message, "refused")
}

func TestCheckIdentifiesSelf(t *testing.T) {
	svc := observersvc.New("observer-1", nil, nil, nil, zerolog.Nop())

	resp, err := svc.Check(context.Background(), &ironworkv1.CheckRequest{})
	require.NoError(t, err)

	assert.Equal(t, "observer", resp.Component)
	assert.Equal(t, "observer-1", resp.Instance)
	assert.Equal(t, ironworkv1.ServingStatus_SERVING_STATUS_SERVING, resp.Status)
}
