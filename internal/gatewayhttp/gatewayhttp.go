// Package gatewayhttp serves the gateway's REST API. Phase 2: the gateway is
// a pure edge — no database, no placement. The job API translates REST to the
// scheduler's JobService over mTLS gRPC; cluster health is backed by the
// observer's ClusterCheck. The REST contract is unchanged from Phase 1.
package gatewayhttp

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	ironworkv1 "github.com/sohamghodake/ironwork/gen/ironwork/v1"
	"github.com/sohamghodake/ironwork/internal/leaderclient"
	"github.com/sohamghodake/ironwork/internal/protoconv"
	"github.com/sohamghodake/ironwork/internal/store"
)

// clusterCheckTimeout bounds the observer round trip; the observer's own
// per-target timeout is shorter, so a healthy observer always answers in time.
const clusterCheckTimeout = 10 * time.Second

// submitTimeout bounds POST /jobs: the scheduler may walk every worker with
// its own per-attempt timeout before answering.
const submitTimeout = 15 * time.Second

const readTimeout = 5 * time.Second

const (
	maxBodyBytes = 1 << 20
	defaultLimit = 50
	maxLimit     = 200
)

// RaftStatusProvider aggregates every scheduler's consensus view.
type RaftStatusProvider interface {
	RaftStatus(ctx context.Context) []leaderclient.NodeStatus
}

// Deps wires the router's collaborators.
type Deps struct {
	Health ironworkv1.HealthServiceClient
	Jobs   ironworkv1.JobServiceClient
	Raft   RaftStatusProvider
	Log    zerolog.Logger
}

// jobJSON is the REST rendering of a job — same shape since Phase 1.
type jobJSON struct {
	ID             string          `json:"id"`
	Name           string          `json:"name"`
	Status         string          `json:"status"`
	AssignedWorker string          `json:"assigned_worker,omitempty"`
	Attempts       uint32          `json:"attempts"`
	Error          string          `json:"error,omitempty"`
	CreatedAt      time.Time       `json:"created_at"`
	UpdatedAt      time.Time       `json:"updated_at"`
	StartedAt      *time.Time      `json:"started_at,omitempty"`
	FinishedAt     *time.Time      `json:"finished_at,omitempty"`
	Payload        json.RawMessage `json:"payload,omitempty"`
}

func renderJob(j *ironworkv1.Job) jobJSON {
	out := jobJSON{
		ID:             j.Id,
		Name:           j.Name,
		Status:         protoconv.StatusFromProto(j.Status),
		AssignedWorker: j.AssignedWorkerId,
		Attempts:       j.Attempts,
		Error:          j.Error,
		CreatedAt:      j.CreatedAt.AsTime(),
		UpdatedAt:      j.UpdatedAt.AsTime(),
	}
	if j.StartedAt != nil {
		t := j.StartedAt.AsTime()
		out.StartedAt = &t
	}
	if j.FinishedAt != nil {
		t := j.FinishedAt.AsTime()
		out.FinishedAt = &t
	}
	if json.Valid(j.Payload) {
		out.Payload = j.Payload
	}
	return out
}

var validStatuses = map[string]bool{
	store.StatusPending: true, store.StatusScheduled: true, store.StatusRunning: true,
	store.StatusSucceeded: true, store.StatusFailed: true,
}

// NewRouter builds the gateway's HTTP handler.
func NewRouter(d Deps) http.Handler {
	r := chi.NewRouter()

	// Liveness of the gateway process itself (compose healthcheck).
	r.Get("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})

	r.Get("/health", d.handleHealth)
	r.Get("/raft", d.handleRaft)
	r.Get("/workers", d.handleWorkers)

	r.Route("/jobs", func(r chi.Router) {
		r.Post("/", d.handleCreateJob)
		r.Get("/", d.handleListJobs)
		r.Get("/{id}", d.handleGetJob)
	})

	return r
}

// writeRPCError maps a JobService error onto the REST surface.
func (d Deps) writeRPCError(w http.ResponseWriter, err error) {
	st := status.Convert(err)
	switch st.Code() {
	case codes.InvalidArgument:
		writeError(w, http.StatusBadRequest, st.Message())
	case codes.NotFound:
		writeError(w, http.StatusNotFound, st.Message())
	default:
		d.Log.Error().Err(err).Msg("scheduler call failed")
		writeError(w, http.StatusBadGateway, "scheduler unavailable: "+st.Message())
	}
}

func (d Deps) handleCreateJob(w http.ResponseWriter, req *http.Request) {
	var body struct {
		Name    string          `json:"name"`
		Payload json.RawMessage `json:"payload"`
	}
	req.Body = http.MaxBytesReader(w, req.Body, maxBodyBytes)
	if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}

	ctx, cancel := context.WithTimeout(req.Context(), submitTimeout)
	defer cancel()

	resp, err := d.Jobs.SubmitJob(ctx, &ironworkv1.SubmitJobRequest{
		Name:    body.Name,
		Payload: body.Payload,
	})
	if err != nil {
		d.writeRPCError(w, err)
		return
	}

	w.Header().Set("Location", "/jobs/"+resp.Job.Id)
	writeJSON(w, http.StatusCreated, renderJob(resp.Job))
}

func (d Deps) handleGetJob(w http.ResponseWriter, req *http.Request) {
	id := chi.URLParam(req, "id")
	if _, err := uuid.Parse(id); err != nil {
		writeError(w, http.StatusBadRequest, "id must be a UUID")
		return
	}

	ctx, cancel := context.WithTimeout(req.Context(), readTimeout)
	defer cancel()

	resp, err := d.Jobs.GetJob(ctx, &ironworkv1.GetJobRequest{Id: id})
	if err != nil {
		d.writeRPCError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, renderJob(resp.Job))
}

func (d Deps) handleListJobs(w http.ResponseWriter, req *http.Request) {
	filter := req.URL.Query().Get("status")
	if filter != "" && !validStatuses[filter] {
		writeError(w, http.StatusBadRequest, "unknown status filter")
		return
	}

	limit := defaultLimit
	if raw := req.URL.Query().Get("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil {
			writeError(w, http.StatusBadRequest, "limit must be an integer")
			return
		}
		limit = n
	}
	if limit < 1 || limit > maxLimit {
		limit = defaultLimit
	}

	ctx, cancel := context.WithTimeout(req.Context(), readTimeout)
	defer cancel()

	resp, err := d.Jobs.ListJobs(ctx, &ironworkv1.ListJobsRequest{
		StatusFilter: protoconv.StatusToProto(filter),
		PageSize:     uint32(limit), //nolint:gosec // limit is bounded above
	})
	if err != nil {
		d.writeRPCError(w, err)
		return
	}

	rendered := make([]jobJSON, 0, len(resp.Jobs))
	for _, j := range resp.Jobs {
		rendered = append(rendered, renderJob(j))
	}
	writeJSON(w, http.StatusOK, map[string]any{"jobs": rendered})
}

// handleRaft reports every scheduler's consensus view side by side — the
// live window into leader election and log replication. "leader" is the
// leader_id a majority of nodes agree on ("" mid-election).
func (d Deps) handleRaft(w http.ResponseWriter, req *http.Request) {
	ctx, cancel := context.WithTimeout(req.Context(), clusterCheckTimeout)
	defer cancel()

	nodes := d.Raft.RaftStatus(ctx)

	votes := map[string]int{}
	for _, n := range nodes {
		if n.Reachable && n.LeaderID != "" {
			votes[n.LeaderID]++
		}
	}
	leader := ""
	for id, count := range votes {
		if count > len(nodes)/2 {
			leader = id
		}
	}

	writeJSON(w, http.StatusOK, map[string]any{"leader": leader, "nodes": nodes})
}

// handleWorkers reports each scheduler's heartbeat view of the worker pool —
// liveness, capacity, and inflight load (the backpressure signal) side by
// side across all registries.
func (d Deps) handleWorkers(w http.ResponseWriter, req *http.Request) {
	ctx, cancel := context.WithTimeout(req.Context(), clusterCheckTimeout)
	defer cancel()

	type nodeWorkers struct {
		Name      string                    `json:"name"`
		Reachable bool                      `json:"reachable"`
		Error     string                    `json:"error,omitempty"`
		Workers   []leaderclient.WorkerView `json:"workers"`
	}
	nodes := d.Raft.RaftStatus(ctx)
	out := make([]nodeWorkers, 0, len(nodes))
	for _, n := range nodes {
		out = append(out, nodeWorkers{Name: n.Name, Reachable: n.Reachable, Error: n.Error, Workers: n.Workers})
	}
	writeJSON(w, http.StatusOK, map[string]any{"nodes": out})
}

// healthResponse is the JSON shape of GET /health.
type healthResponse struct {
	Status     string            `json:"status"`
	CheckedAt  time.Time         `json:"checked_at"`
	Components []componentHealth `json:"components,omitempty"`
	Error      string            `json:"error,omitempty"`
}

type componentHealth struct {
	Name    string `json:"name"`
	Status  string `json:"status"`
	Message string `json:"message,omitempty"`
}

// handleHealth reports aggregated cluster health: gateway -> observer ->
// every component + DB.
func (d Deps) handleHealth(w http.ResponseWriter, req *http.Request) {
	ctx, cancel := context.WithTimeout(req.Context(), clusterCheckTimeout)
	defer cancel()

	resp, err := d.Health.ClusterCheck(ctx, &ironworkv1.ClusterCheckRequest{})
	if err != nil {
		d.Log.Error().Err(err).Msg("observer cluster check failed")
		writeJSON(w, http.StatusBadGateway, healthResponse{
			Status:    "UNREACHABLE",
			CheckedAt: time.Now().UTC(),
			Error:     err.Error(),
		})
		return
	}

	body := healthResponse{
		Status:    statusString(resp.Status),
		CheckedAt: resp.CheckedAt.AsTime(),
	}
	for _, c := range resp.Components {
		body.Components = append(body.Components, componentHealth{
			Name:    c.Name,
			Status:  statusString(c.Status),
			Message: c.Message,
		})
	}

	code := http.StatusOK
	if resp.Status != ironworkv1.ServingStatus_SERVING_STATUS_SERVING {
		code = http.StatusServiceUnavailable
	}
	writeJSON(w, code, body)
}

func statusString(s ironworkv1.ServingStatus) string {
	switch s {
	case ironworkv1.ServingStatus_SERVING_STATUS_SERVING:
		return "SERVING"
	case ironworkv1.ServingStatus_SERVING_STATUS_NOT_SERVING:
		return "NOT_SERVING"
	default:
		return "UNKNOWN"
	}
}

func writeJSON(w http.ResponseWriter, code int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(body)
}

func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}
