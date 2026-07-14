package gatewayhttp_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"

	ironworkv1 "github.com/sohamghodake/ironwork/gen/ironwork/v1"
	"github.com/sohamghodake/ironwork/internal/gatewayhttp"
	"github.com/sohamghodake/ironwork/internal/store"
)

const testJobID = "3f6f2be8-7c1e-4b3a-9f10-6a4a1c2d3e4f"

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

// fakeJobStore is an in-memory JobStore.
type fakeJobStore struct {
	jobs      map[string]*store.Job
	createErr error
}

func newFakeJobStore() *fakeJobStore {
	return &fakeJobStore{jobs: map[string]*store.Job{}}
}

func (f *fakeJobStore) CreateJob(_ context.Context, name string, payload []byte) (*store.Job, error) {
	if f.createErr != nil {
		return nil, f.createErr
	}
	j := &store.Job{ID: testJobID, Name: name, Payload: payload,
		Status: store.StatusPending, CreatedAt: time.Now(), UpdatedAt: time.Now()}
	f.jobs[j.ID] = j
	return j, nil
}

func (f *fakeJobStore) GetJob(_ context.Context, id string) (*store.Job, error) {
	j, ok := f.jobs[id]
	if !ok {
		return nil, store.ErrNotFound
	}
	return j, nil
}

func (f *fakeJobStore) ListJobs(_ context.Context, status string, _ int) ([]*store.Job, error) {
	out := []*store.Job{}
	for _, j := range f.jobs {
		if status == "" || j.Status == status {
			out = append(out, j)
		}
	}
	return out, nil
}

func (f *fakeJobStore) MarkFinished(_ context.Context, id string, succeeded bool, errMsg string) error {
	j, ok := f.jobs[id]
	if !ok {
		return store.ErrNotFound
	}
	j.Status = store.StatusFailed
	if succeeded {
		j.Status = store.StatusSucceeded
	}
	j.Error = errMsg
	return nil
}

// fakeDispatcher mimics a worker: on success it records the scheduled state
// the way a real worker does before acking.
type fakeDispatcher struct {
	st     *fakeJobStore
	err    error
	called int
}

func (f *fakeDispatcher) Dispatch(_ context.Context, job *store.Job) (string, error) {
	f.called++
	if f.err != nil {
		return "", f.err
	}
	if j, ok := f.st.jobs[job.ID]; ok {
		j.Status = store.StatusScheduled
		j.AssignedWorker = "worker-1"
	}
	return "worker-1", nil
}

func newRouter(health ironworkv1.HealthServiceClient, st *fakeJobStore, disp gatewayhttp.JobDispatcher) http.Handler {
	return gatewayhttp.NewRouter(gatewayhttp.Deps{Health: health, Jobs: st, Disp: disp, Log: zerolog.Nop()})
}

func doJSON(t *testing.T, h http.Handler, method, path, body string) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	var reader *strings.Reader
	if body == "" {
		reader = strings.NewReader("")
	} else {
		reader = strings.NewReader(body)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(method, path, reader))
	out := map[string]any{}
	if rec.Body.Len() > 0 {
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	}
	return rec, out
}

// --- job API ---

func TestCreateJobDispatches(t *testing.T) {
	st := newFakeJobStore()
	disp := &fakeDispatcher{st: st}
	h := newRouter(&fakeHealthClient{}, st, disp)

	rec, body := doJSON(t, h, http.MethodPost, "/jobs", `{"name":"sleep","payload":{"duration_ms":500}}`)

	assert.Equal(t, http.StatusCreated, rec.Code)
	assert.Equal(t, "/jobs/"+testJobID, rec.Header().Get("Location"))
	assert.Equal(t, 1, disp.called)
	assert.Equal(t, store.StatusScheduled, body["status"])
	assert.Equal(t, "worker-1", body["assigned_worker"])
	assert.Equal(t, map[string]any{"duration_ms": float64(500)}, body["payload"])
}

func TestCreateJobDispatchFailureMarksJobFailed(t *testing.T) {
	st := newFakeJobStore()
	h := newRouter(&fakeHealthClient{}, st, &fakeDispatcher{st: st, err: errors.New("all workers down")})

	rec, body := doJSON(t, h, http.MethodPost, "/jobs", `{"name":"sleep"}`)

	assert.Equal(t, http.StatusCreated, rec.Code)
	assert.Equal(t, store.StatusFailed, body["status"])
	assert.Contains(t, body["error"], "all workers down")
}

func TestCreateJobValidation(t *testing.T) {
	st := newFakeJobStore()
	h := newRouter(&fakeHealthClient{}, st, &fakeDispatcher{st: st})

	rec, _ := doJSON(t, h, http.MethodPost, "/jobs", `{"payload":{}}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code, "missing name")

	rec, _ = doJSON(t, h, http.MethodPost, "/jobs", `{not json`)
	assert.Equal(t, http.StatusBadRequest, rec.Code, "malformed body")
}

func TestGetJob(t *testing.T) {
	st := newFakeJobStore()
	h := newRouter(&fakeHealthClient{}, st, &fakeDispatcher{st: st})
	_, _ = doJSON(t, h, http.MethodPost, "/jobs", `{"name":"sleep"}`)

	rec, body := doJSON(t, h, http.MethodGet, "/jobs/"+testJobID, "")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "sleep", body["name"])

	rec, _ = doJSON(t, h, http.MethodGet, "/jobs/00000000-0000-0000-0000-000000000000", "")
	assert.Equal(t, http.StatusNotFound, rec.Code)

	rec, _ = doJSON(t, h, http.MethodGet, "/jobs/not-a-uuid", "")
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestListJobs(t *testing.T) {
	st := newFakeJobStore()
	h := newRouter(&fakeHealthClient{}, st, &fakeDispatcher{st: st})
	_, _ = doJSON(t, h, http.MethodPost, "/jobs", `{"name":"sleep"}`)

	rec, body := doJSON(t, h, http.MethodGet, "/jobs?status=scheduled", "")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Len(t, body["jobs"], 1)

	rec, _ = doJSON(t, h, http.MethodGet, "/jobs?status=bogus", "")
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	rec, _ = doJSON(t, h, http.MethodGet, "/jobs?limit=zap", "")
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// --- health ---

func getHealth(t *testing.T, client ironworkv1.HealthServiceClient, path string) *httptest.ResponseRecorder {
	t.Helper()
	router := newRouter(client, newFakeJobStore(), &fakeDispatcher{})
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	return rec
}

func TestHealthzAlwaysOK(t *testing.T) {
	rec := getHealth(t, &fakeHealthClient{err: errors.New("observer down")}, "/healthz")
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHealthServing(t *testing.T) {
	rec := getHealth(t, &fakeHealthClient{resp: &ironworkv1.ClusterCheckResponse{
		Status: ironworkv1.ServingStatus_SERVING_STATUS_SERVING,
		Components: []*ironworkv1.ComponentHealth{
			{Name: "scheduler-1", Status: ironworkv1.ServingStatus_SERVING_STATUS_SERVING},
		},
		CheckedAt: timestamppb.Now(),
	}}, "/health")

	assert.Equal(t, http.StatusOK, rec.Code)

	var body map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	assert.Equal(t, "SERVING", body["status"])
	require.Len(t, body["components"], 1)
}

func TestHealthNotServingIs503(t *testing.T) {
	rec := getHealth(t, &fakeHealthClient{resp: &ironworkv1.ClusterCheckResponse{
		Status:    ironworkv1.ServingStatus_SERVING_STATUS_NOT_SERVING,
		CheckedAt: timestamppb.Now(),
	}}, "/health")

	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
}

func TestHealthObserverUnreachableIs502(t *testing.T) {
	rec := getHealth(t, &fakeHealthClient{err: errors.New("connection refused")}, "/health")

	assert.Equal(t, http.StatusBadGateway, rec.Code)

	var body map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	assert.Equal(t, "UNREACHABLE", body["status"])
}
