// Package gatewayhttp serves the gateway's REST API: the job API (create,
// get, list — Phase 1) and cluster health (backed by the observer's
// ClusterCheck over mTLS gRPC) plus a local liveness probe.
package gatewayhttp

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/rs/zerolog"

	ironworkv1 "github.com/sohamghodake/ironwork/gen/ironwork/v1"
	"github.com/sohamghodake/ironwork/internal/store"
)

// clusterCheckTimeout bounds the observer round trip; the observer's own
// per-target timeout is shorter, so a healthy observer always answers in time.
const clusterCheckTimeout = 10 * time.Second

const (
	maxBodyBytes  = 1 << 20
	maxNameLength = 200
	defaultLimit  = 50
	maxLimit      = 200
)

// JobStore is the slice of store.Store the gateway needs.
type JobStore interface {
	CreateJob(ctx context.Context, name string, payload []byte) (*store.Job, error)
	GetJob(ctx context.Context, id string) (*store.Job, error)
	ListJobs(ctx context.Context, status string, limit int) ([]*store.Job, error)
	MarkFinished(ctx context.Context, id string, succeeded bool, errMsg string) error
}

// JobDispatcher places an accepted job on a worker (Phase 1: round-robin).
type JobDispatcher interface {
	Dispatch(ctx context.Context, job *store.Job) (workerInstance string, err error)
}

// Deps wires the router's collaborators.
type Deps struct {
	Health ironworkv1.HealthServiceClient
	Jobs   JobStore
	Disp   JobDispatcher
	Log    zerolog.Logger
}

// jobJSON renders a store.Job with its payload inlined when it is valid JSON.
type jobJSON struct {
	*store.Job
	Payload json.RawMessage `json:"payload,omitempty"`
}

func renderJob(j *store.Job) jobJSON {
	out := jobJSON{Job: j}
	if json.Valid(j.Payload) {
		out.Payload = json.RawMessage(j.Payload)
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

	r.Route("/jobs", func(r chi.Router) {
		r.Post("/", d.handleCreateJob)
		r.Get("/", d.handleListJobs)
		r.Get("/{id}", d.handleGetJob)
	})

	return r
}

// handleCreateJob persists a job and dispatches it to a worker. The job
// resource is created either way (201); a failed dispatch surfaces as the job
// in status "failed" with the dispatch error recorded.
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
	if body.Name == "" || len(body.Name) > maxNameLength {
		writeError(w, http.StatusBadRequest, "name is required (max 200 chars)")
		return
	}

	ctx := req.Context()
	job, err := d.Jobs.CreateJob(ctx, body.Name, body.Payload)
	if err != nil {
		d.Log.Error().Err(err).Msg("create job")
		writeError(w, http.StatusInternalServerError, "create job")
		return
	}

	if _, err := d.Disp.Dispatch(ctx, job); err != nil {
		d.Log.Warn().Err(err).Str("job_id", job.ID).Msg("dispatch failed")
		if merr := d.Jobs.MarkFinished(ctx, job.ID, false, err.Error()); merr != nil {
			d.Log.Error().Err(merr).Str("job_id", job.ID).Msg("mark dispatch failure")
		}
	}

	// Re-read: the accepting worker has already recorded at least "scheduled".
	if fresh, err := d.Jobs.GetJob(ctx, job.ID); err == nil {
		job = fresh
	}

	w.Header().Set("Location", "/jobs/"+job.ID)
	writeJSON(w, http.StatusCreated, renderJob(job))
}

func (d Deps) handleGetJob(w http.ResponseWriter, req *http.Request) {
	id := chi.URLParam(req, "id")
	if _, err := uuid.Parse(id); err != nil {
		writeError(w, http.StatusBadRequest, "id must be a UUID")
		return
	}

	job, err := d.Jobs.GetJob(req.Context(), id)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "job not found")
		return
	}
	if err != nil {
		d.Log.Error().Err(err).Str("job_id", id).Msg("get job")
		writeError(w, http.StatusInternalServerError, "get job")
		return
	}
	writeJSON(w, http.StatusOK, renderJob(job))
}

func (d Deps) handleListJobs(w http.ResponseWriter, req *http.Request) {
	status := req.URL.Query().Get("status")
	if status != "" && !validStatuses[status] {
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

	jobs, err := d.Jobs.ListJobs(req.Context(), status, limit)
	if err != nil {
		d.Log.Error().Err(err).Msg("list jobs")
		writeError(w, http.StatusInternalServerError, "list jobs")
		return
	}

	rendered := make([]jobJSON, 0, len(jobs))
	for _, j := range jobs {
		rendered = append(rendered, renderJob(j))
	}
	writeJSON(w, http.StatusOK, map[string]any{"jobs": rendered})
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
