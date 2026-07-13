package gatewayhttp_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"

	ironworkv1 "github.com/sohamghodake/ironwork/gen/ironwork/v1"
	"github.com/sohamghodake/ironwork/internal/gatewayhttp"
)

type fakeHealthClient struct {
	resp *ironworkv1.ClusterCheckResponse
	err  error
}

func (f *fakeHealthClient) Check(context.Context, *ironworkv1.CheckRequest, ...grpc.CallOption) (*ironworkv1.CheckResponse, error) {
	return nil, errors.New("not used by the gateway")
}

func (f *fakeHealthClient) ClusterCheck(context.Context, *ironworkv1.ClusterCheckRequest, ...grpc.CallOption) (*ironworkv1.ClusterCheckResponse, error) {
	return f.resp, f.err
}

func get(t *testing.T, client ironworkv1.HealthServiceClient, path string) *httptest.ResponseRecorder {
	t.Helper()
	router := gatewayhttp.NewRouter(client, zerolog.Nop())
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	return rec
}

func TestHealthzAlwaysOK(t *testing.T) {
	rec := get(t, &fakeHealthClient{err: errors.New("observer down")}, "/healthz")
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHealthServing(t *testing.T) {
	rec := get(t, &fakeHealthClient{resp: &ironworkv1.ClusterCheckResponse{
		Status: ironworkv1.ServingStatus_SERVING_STATUS_SERVING,
		Components: []*ironworkv1.ComponentHealth{
			{Name: "scheduler-1", Status: ironworkv1.ServingStatus_SERVING_STATUS_SERVING},
		},
		CheckedAt: timestamppb.Now(),
	}}, "/health")

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))

	var body map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	assert.Equal(t, "SERVING", body["status"])
	require.Len(t, body["components"], 1)
}

func TestHealthNotServingIs503(t *testing.T) {
	rec := get(t, &fakeHealthClient{resp: &ironworkv1.ClusterCheckResponse{
		Status: ironworkv1.ServingStatus_SERVING_STATUS_NOT_SERVING,
		Components: []*ironworkv1.ComponentHealth{
			{Name: "worker-1", Status: ironworkv1.ServingStatus_SERVING_STATUS_NOT_SERVING, Message: "connection refused"},
		},
		CheckedAt: timestamppb.Now(),
	}}, "/health")

	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)

	var body map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	assert.Equal(t, "NOT_SERVING", body["status"])
}

func TestHealthObserverUnreachableIs502(t *testing.T) {
	rec := get(t, &fakeHealthClient{err: errors.New("connection refused")}, "/health")

	assert.Equal(t, http.StatusBadGateway, rec.Code)

	var body map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	assert.Equal(t, "UNREACHABLE", body["status"])
	assert.Contains(t, body["error"], "connection refused")
}
