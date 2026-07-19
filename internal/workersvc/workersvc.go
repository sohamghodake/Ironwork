// Package workersvc implements the worker's WorkerService: ExecuteJob accepts
// a dispatched job, records acceptance in Postgres, and executes it
// asynchronously under a fixed concurrency cap. Phase 1 execution is the
// documented stub: parse the payload, sleep, succeed or fail on request.
package workersvc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	ironworkv1 "github.com/sohamghodake/ironwork/gen/ironwork/v1"
	"github.com/sohamghodake/ironwork/internal/store"
)

// maxSleep caps requested job duration so a typo cannot wedge a slot for long.
const maxSleep = 60 * time.Second

// defaultSleep is used when the payload does not specify duration_ms.
const defaultSleep = 1 * time.Second

// JobStore is the slice of store.Store the worker needs.
type JobStore interface {
	MarkScheduled(ctx context.Context, id, workerInstance string) error
	MarkRunning(ctx context.Context, id string) error
	MarkFinished(ctx context.Context, id string, succeeded bool, errMsg string) error
}

// Reporter forwards terminal job outcomes to the statemanager's
// eventually-consistent statistics view. May be nil (reporting disabled).
type Reporter interface {
	ReportJobEvent(ctx context.Context, jobID, worker string, succeeded bool)
}

// payload is the Phase 1 stub job contract.
type payload struct {
	DurationMS int  `json:"duration_ms"`
	Fail       bool `json:"fail"`
}

// Server implements WorkerService for a worker instance.
type Server struct {
	ironworkv1.UnimplementedWorkerServiceServer

	store     JobStore
	reporter  Reporter
	instance  string
	capacity  int32
	inflightN atomic.Int32
	inflight  sync.WaitGroup
	log       zerolog.Logger
}

// New builds a worker service executing at most capacity jobs concurrently.
// reporter may be nil.
func New(instance string, st JobStore, capacity int, reporter Reporter, log zerolog.Logger) *Server {
	if capacity < 1 {
		capacity = 1
	}
	return &Server{
		store:    st,
		reporter: reporter,
		instance: instance,
		capacity: int32(capacity), //nolint:gosec // capacity is a small config value
		log:      log,
	}
}

// Inflight reports jobs currently accepted and not yet finished; heartbeats
// carry it so schedulers can steer placement (backpressure).
func (s *Server) Inflight() int { return int(s.inflightN.Load()) }

// Capacity reports the concurrency cap.
func (s *Server) Capacity() int { return int(s.capacity) }

// ExecuteJob rejects at capacity (backpressure: the placer falls through to
// another worker), otherwise records acceptance synchronously (status ->
// scheduled), then executes in the background and acks.
func (s *Server) ExecuteJob(ctx context.Context, req *ironworkv1.ExecuteJobRequest) (*ironworkv1.ExecuteJobResponse, error) {
	if req.JobId == "" {
		return nil, status.Error(codes.InvalidArgument, "job_id is required")
	}

	// Reserve a slot before touching the database; give it back on any
	// rejection path.
	for {
		cur := s.inflightN.Load()
		if cur >= s.capacity {
			return nil, status.Errorf(codes.ResourceExhausted, "worker %s at capacity (%d/%d)", s.instance, cur, s.capacity)
		}
		if s.inflightN.CompareAndSwap(cur, cur+1) {
			break
		}
	}

	if err := s.store.MarkScheduled(ctx, req.JobId, s.instance); err != nil {
		s.inflightN.Add(-1)
		if errors.Is(err, store.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "job %s not found", req.JobId)
		}
		return nil, status.Errorf(codes.Internal, "record acceptance: %v", err)
	}

	s.inflight.Add(1)
	go s.run(req.JobId, req.Payload)

	s.log.Info().Str("job_id", req.JobId).Str("name", req.Name).Msg("job accepted")
	return &ironworkv1.ExecuteJobResponse{WorkerInstance: s.instance}, nil
}

func (s *Server) run(jobID string, rawPayload []byte) {
	defer s.inflight.Done()
	defer s.inflightN.Add(-1)

	// Detached from the RPC context: execution outlives the ExecuteJob call.
	ctx, cancel := context.WithTimeout(context.Background(), 2*maxSleep)
	defer cancel()

	if err := s.store.MarkRunning(ctx, jobID); err != nil {
		s.log.Error().Err(err).Str("job_id", jobID).Msg("mark running")
		return
	}

	execErr := s.execute(jobID, rawPayload)

	succeeded, errMsg := true, ""
	if execErr != nil {
		succeeded, errMsg = false, execErr.Error()
	}
	if err := s.store.MarkFinished(ctx, jobID, succeeded, errMsg); err != nil {
		s.log.Error().Err(err).Str("job_id", jobID).Msg("mark finished")
		return
	}
	if s.reporter != nil {
		s.reporter.ReportJobEvent(ctx, jobID, s.instance, succeeded)
	}
	s.log.Info().Str("job_id", jobID).Bool("succeeded", succeeded).Msg("job finished")
}

// execute is the Phase 1 stub: sleep for payload.duration_ms (default 1s,
// capped), then honor the fail flag. Real execution arrives in later phases.
func (s *Server) execute(jobID string, rawPayload []byte) error {
	var p payload
	if len(rawPayload) > 0 {
		if err := json.Unmarshal(rawPayload, &p); err != nil {
			return fmt.Errorf("invalid payload: %w", err)
		}
	}
	if p.DurationMS < 0 {
		return fmt.Errorf("invalid payload: duration_ms must be >= 0")
	}

	sleep := defaultSleep
	if p.DurationMS > 0 {
		sleep = time.Duration(p.DurationMS) * time.Millisecond
	}
	if sleep > maxSleep {
		s.log.Warn().Str("job_id", jobID).Dur("requested", sleep).Dur("capped_to", maxSleep).Msg("duration capped")
		sleep = maxSleep
	}
	time.Sleep(sleep)

	if p.Fail {
		return errors.New("job requested failure")
	}
	return nil
}

// Drain waits up to timeout for in-flight jobs to finish; call after the gRPC
// server has stopped accepting new work.
func (s *Server) Drain(timeout time.Duration) {
	done := make(chan struct{})
	go func() {
		s.inflight.Wait()
		close(done)
	}()
	select {
	case <-done:
		s.log.Info().Msg("all jobs drained")
	case <-time.After(timeout):
		s.log.Warn().Dur("timeout", timeout).Msg("drain timed out with jobs still running")
	}
}
