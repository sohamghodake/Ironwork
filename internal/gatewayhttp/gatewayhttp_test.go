package gatewayhttp_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	ironworkv1 "github.com/sohamghodake/ironwork/gen/ironwork/v1"
	"github.com/sohamghodake/ironwork/internal/crdtview"
	"github.com/sohamghodake/ironwork/internal/gatewayhttp"
	"github.com/sohamghodake/ironwork/internal/leaderclient"
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

// fakeJobsClient scripts the scheduler's answers.
type fakeJobsClient struct {
	submitResp *ironworkv1.SubmitJobResponse
	submitErr  error
	getResp    *ironworkv1.GetJobResponse
	getErr     error
	listResp   *ironworkv1.ListJobsResponse
	listErr    error

	lastSubmit *ironworkv1.SubmitJobRequest
	lastList   *ironworkv1.ListJobsRequest
}

func (f *fakeJobsClient) SubmitJob(_ context.Context, in *ironworkv1.SubmitJobRequest, _ ...grpc.CallOption) (*ironworkv1.SubmitJobResponse, error) {
	f.lastSubmit = in
	return f.submitResp, f.submitErr
}

func (f *fakeJobsClient) GetJob(_ context.Context, in *ironworkv1.GetJobRequest, _ ...grpc.CallOption) (*ironworkv1.GetJobResponse, error) {
	return f.getResp, f.getErr
}

func (f *fakeJobsClient) ListJobs(_ context.Context, in *ironworkv1.ListJobsRequest, _ ...grpc.CallOption) (*ironworkv1.ListJobsResponse, error) {
	f.lastList = in
	return f.listResp, f.listErr
}

func protoJob(status ironworkv1.JobStatus) *ironworkv1.Job {
	return &ironworkv1.Job{
		Id:               testJobID,
		Name:             "sleep",
		Payload:          []byte(`{"duration_ms":500}`),
		Status:           status,
		AssignedWorkerId: "worker-1",
		Attempts:         1,
		CreatedAt:        timestamppb.Now(),
		UpdatedAt:        timestamppb.Now(),
	}
}

func newRouter(health ironworkv1.HealthServiceClient, jobs ironworkv1.JobServiceClient) http.Handler {
	return gatewayhttp.NewRouter(gatewayhttp.Deps{Health: health, Jobs: jobs, Log: zerolog.Nop()})
}

func doJSON(t *testing.T, h http.Handler, method, path, body string) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(method, path, strings.NewReader(body)))
	out := map[string]any{}
	if rec.Body.Len() > 0 {
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	}
	return rec, out
}

// --- job API ---

func TestCreateJobTranslatesToScheduler(t *testing.T) {
	jobs := &fakeJobsClient{submitResp: &ironworkv1.SubmitJobResponse{
		Job: protoJob(ironworkv1.JobStatus_JOB_STATUS_SCHEDULED),
	}}
	h := newRouter(&fakeHealthClient{}, jobs)

	rec, body := doJSON(t, h, http.MethodPost, "/jobs", `{"name":"sleep","payload":{"duration_ms":500}}`)

	assert.Equal(t, http.StatusCreated, rec.Code)
	assert.Equal(t, "/jobs/"+testJobID, rec.Header().Get("Location"))
	assert.Equal(t, "scheduled", body["status"])
	assert.Equal(t, "worker-1", body["assigned_worker"])
	assert.Equal(t, map[string]any{"duration_ms": float64(500)}, body["payload"])

	require.NotNil(t, jobs.lastSubmit)
	assert.Equal(t, "sleep", jobs.lastSubmit.Name)
	assert.JSONEq(t, `{"duration_ms":500}`, string(jobs.lastSubmit.Payload))
}

func TestCreateJobSchedulerValidationIs400(t *testing.T) {
	jobs := &fakeJobsClient{submitErr: status.Error(codes.InvalidArgument, "name is required")}
	h := newRouter(&fakeHealthClient{}, jobs)

	rec, body := doJSON(t, h, http.MethodPost, "/jobs", `{"payload":{}}`)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, body["error"], "name is required")
}

func TestCreateJobSchedulerDownIs502(t *testing.T) {
	jobs := &fakeJobsClient{submitErr: status.Error(codes.Unavailable, "connection refused")}
	h := newRouter(&fakeHealthClient{}, jobs)

	rec, body := doJSON(t, h, http.MethodPost, "/jobs", `{"name":"sleep"}`)

	assert.Equal(t, http.StatusBadGateway, rec.Code)
	assert.Contains(t, body["error"], "scheduler unavailable")
}

func TestCreateJobMalformedBodyIs400(t *testing.T) {
	h := newRouter(&fakeHealthClient{}, &fakeJobsClient{})

	rec, _ := doJSON(t, h, http.MethodPost, "/jobs", `{not json`)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestGetJob(t *testing.T) {
	jobs := &fakeJobsClient{getResp: &ironworkv1.GetJobResponse{
		Job: protoJob(ironworkv1.JobStatus_JOB_STATUS_SUCCEEDED),
	}}
	h := newRouter(&fakeHealthClient{}, jobs)

	rec, body := doJSON(t, h, http.MethodGet, "/jobs/"+testJobID, "")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "succeeded", body["status"])

	jobs.getResp, jobs.getErr = nil, status.Error(codes.NotFound, "job not found")
	rec, _ = doJSON(t, h, http.MethodGet, "/jobs/"+testJobID, "")
	assert.Equal(t, http.StatusNotFound, rec.Code)

	rec, _ = doJSON(t, h, http.MethodGet, "/jobs/not-a-uuid", "")
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestListJobs(t *testing.T) {
	jobs := &fakeJobsClient{listResp: &ironworkv1.ListJobsResponse{
		Jobs: []*ironworkv1.Job{protoJob(ironworkv1.JobStatus_JOB_STATUS_RUNNING)},
	}}
	h := newRouter(&fakeHealthClient{}, jobs)

	rec, body := doJSON(t, h, http.MethodGet, "/jobs?status=running&limit=20", "")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Len(t, body["jobs"], 1)
	require.NotNil(t, jobs.lastList)
	assert.Equal(t, ironworkv1.JobStatus_JOB_STATUS_RUNNING, jobs.lastList.StatusFilter)
	assert.Equal(t, uint32(20), jobs.lastList.PageSize)

	rec, _ = doJSON(t, h, http.MethodGet, "/jobs?status=bogus", "")
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	rec, _ = doJSON(t, h, http.MethodGet, "/jobs?limit=zap", "")
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// --- raft view ---

type fakeRaft struct{ nodes []leaderclient.NodeStatus }

func (f *fakeRaft) RaftStatus(context.Context) []leaderclient.NodeStatus { return f.nodes }

func TestRaftEndpointReportsMajorityLeader(t *testing.T) {
	h := gatewayhttp.NewRouter(gatewayhttp.Deps{
		Health: &fakeHealthClient{}, Jobs: &fakeJobsClient{}, Log: zerolog.Nop(),
		Raft: &fakeRaft{nodes: []leaderclient.NodeStatus{
			{Name: "scheduler-1", Reachable: true, State: "Follower", LeaderID: "scheduler-2", Term: 5},
			{Name: "scheduler-2", Reachable: true, State: "Leader", LeaderID: "scheduler-2", Term: 5},
			{Name: "scheduler-3", Reachable: false, Error: "stopped"},
		}},
	})

	rec, body := doJSON(t, h, http.MethodGet, "/raft", "")

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "scheduler-2", body["leader"])
	assert.Len(t, body["nodes"], 3)
}

func TestRaftEndpointNoMajorityDuringElection(t *testing.T) {
	h := gatewayhttp.NewRouter(gatewayhttp.Deps{
		Health: &fakeHealthClient{}, Jobs: &fakeJobsClient{}, Log: zerolog.Nop(),
		Raft: &fakeRaft{nodes: []leaderclient.NodeStatus{
			{Name: "scheduler-1", Reachable: true, State: "Candidate", LeaderID: ""},
			{Name: "scheduler-2", Reachable: true, State: "Candidate", LeaderID: ""},
			{Name: "scheduler-3", Reachable: false, Error: "stopped"},
		}},
	})

	rec, body := doJSON(t, h, http.MethodGet, "/raft", "")

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "", body["leader"])
}

func TestWorkersEndpointReportsRegistryViews(t *testing.T) {
	h := gatewayhttp.NewRouter(gatewayhttp.Deps{
		Health: &fakeHealthClient{}, Jobs: &fakeJobsClient{}, Log: zerolog.Nop(),
		Raft: &fakeRaft{nodes: []leaderclient.NodeStatus{
			{Name: "scheduler-1", Reachable: true, Workers: []leaderclient.WorkerView{
				{Instance: "worker-1", Alive: true, Capacity: 2, Inflight: 2},
				{Instance: "worker-2", Alive: false, SecondsSinceHeartbeat: 9.5, Capacity: 2},
			}},
			{Name: "scheduler-2", Reachable: false, Error: "stopped"},
		}},
	})

	rec, body := doJSON(t, h, http.MethodGet, "/workers", "")

	assert.Equal(t, http.StatusOK, rec.Code)
	nodes := body["nodes"].([]any)
	require.Len(t, nodes, 2)
	first := nodes[0].(map[string]any)
	workers := first["workers"].([]any)
	require.Len(t, workers, 2)
	assert.Equal(t, false, workers[1].(map[string]any)["alive"])
}

// --- crdt view ---

type fakeCRDT struct {
	view       crdtview.View
	gossipSets []bool
	err        error
}

func (f *fakeCRDT) Fetch(context.Context) crdtview.View { return f.view }
func (f *fakeCRDT) SetGossip(_ context.Context, enabled bool) error {
	f.gossipSets = append(f.gossipSets, enabled)
	return f.err
}

func TestCRDTStateAndPartitionControls(t *testing.T) {
	crdt := &fakeCRDT{view: crdtview.View{
		Converged: false,
		Replicas: []crdtview.ReplicaView{
			{Name: "statemanager-1", Reachable: true, GossipEnabled: false,
				ByStatus: map[string]crdtview.CounterView{"succeeded": {Total: 4, Shards: map[string]uint64{"statemanager-1": 4}}}},
			{Name: "statemanager-2", Reachable: true, GossipEnabled: false},
		},
	}}
	h := gatewayhttp.NewRouter(gatewayhttp.Deps{
		Health: &fakeHealthClient{}, Jobs: &fakeJobsClient{}, CRDT: crdt, Log: zerolog.Nop(),
	})

	rec, body := doJSON(t, h, http.MethodGet, "/crdt/state", "")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, false, body["converged"])
	assert.Len(t, body["replicas"], 2)

	rec, _ = doJSON(t, h, http.MethodPost, "/crdt/partition", "")
	assert.Equal(t, http.StatusOK, rec.Code)
	rec, _ = doJSON(t, h, http.MethodPost, "/crdt/heal", "")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, []bool{false, true}, crdt.gossipSets)

	// The dashboard itself is served inline.
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/crdt", nil))
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Header().Get("Content-Type"), "text/html")
	assert.Contains(t, rec.Body.String(), "CRDT convergence")
}

// --- health ---

func getHealth(t *testing.T, client ironworkv1.HealthServiceClient, path string) *httptest.ResponseRecorder {
	t.Helper()
	router := newRouter(client, &fakeJobsClient{})
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
