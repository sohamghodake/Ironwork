// Package schedulersvc implements the scheduler's JobService. From Phase 6,
// SubmitJob persists the job and its dispatch command in one transaction (the
// transactional outbox) and wakes the leader's relay — it never dispatches
// inline. GetJob/ListJobs serve the gateway's reads; GetOutboxStats reports
// the dispatch backlog. Raft leadership gates submission from Phase 3 on.
package schedulersvc

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	ironworkv1 "github.com/sohamghodake/ironwork/gen/ironwork/v1"
	"github.com/sohamghodake/ironwork/internal/protoconv"
	"github.com/sohamghodake/ironwork/internal/store"
)

const (
	maxNameLength = 200
	defaultLimit  = 50
	maxLimit      = 200
)

// JobStore is the slice of store.Store the scheduler needs.
type JobStore interface {
	CreateJobWithOutbox(ctx context.Context, name string, payload []byte) (*store.Job, error)
	GetJob(ctx context.Context, id string) (*store.Job, error)
	ListJobs(ctx context.Context, status string, limit int) ([]*store.Job, error)
	OutboxStats(ctx context.Context) (store.OutboxStats, error)
}

// Leadership reports this scheduler's role in the Raft consensus group.
type Leadership interface {
	IsLeader() bool
	LeaderID() string
}

// Waker nudges the outbox relay to dispatch a freshly enqueued command
// without waiting for its next tick.
type Waker interface {
	Wake()
}

// Server implements JobService for the scheduler.
type Server struct {
	ironworkv1.UnimplementedJobServiceServer

	store JobStore
	lead  Leadership
	waker Waker
	log   zerolog.Logger
}

// New builds the scheduler's job service.
func New(st JobStore, lead Leadership, waker Waker, log zerolog.Logger) *Server {
	return &Server{store: st, lead: lead, waker: waker, log: log}
}

// SubmitJob persists the job and places it. Only the Raft leader accepts
// submissions; followers reject fast so the gateway can route onward. The job
// resource and its dispatch command are committed together; the job returns
// as "pending" and the relay places it moments later.
func (s *Server) SubmitJob(ctx context.Context, req *ironworkv1.SubmitJobRequest) (*ironworkv1.SubmitJobResponse, error) {
	if !s.lead.IsLeader() {
		return nil, status.Errorf(codes.FailedPrecondition, "not leader (leader: %s)", s.lead.LeaderID())
	}
	if req.Name == "" || len(req.Name) > maxNameLength {
		return nil, status.Error(codes.InvalidArgument, "name is required (max 200 chars)")
	}

	// One transaction: the job and its dispatch command land together, so the
	// intent to place the job is durable the instant SubmitJob returns — no
	// dual write, nothing to lose if the leader crashes before dispatching.
	job, err := s.store.CreateJobWithOutbox(ctx, req.Name, req.Payload)
	if err != nil {
		s.log.Error().Err(err).Msg("create job with outbox")
		return nil, status.Error(codes.Internal, "create job")
	}
	s.log.Info().Str("job_id", job.ID).Msg("job enqueued for dispatch")

	// Nudge the relay so placement happens now rather than on its next tick.
	s.waker.Wake()

	return &ironworkv1.SubmitJobResponse{Job: protoconv.JobToProto(job)}, nil
}

// GetOutboxStats reports the current dispatch backlog.
func (s *Server) GetOutboxStats(ctx context.Context, _ *ironworkv1.GetOutboxStatsRequest) (*ironworkv1.GetOutboxStatsResponse, error) {
	stats, err := s.store.OutboxStats(ctx)
	if err != nil {
		s.log.Error().Err(err).Msg("outbox stats")
		return nil, status.Error(codes.Internal, "outbox stats")
	}
	return &ironworkv1.GetOutboxStatsResponse{
		Pending:              stats.Pending,
		Dispatched:           stats.Dispatched,
		Failed:               stats.Failed,
		OldestPendingSeconds: stats.OldestPending.Seconds(),
	}, nil
}

// GetJob fetches one job by id.
func (s *Server) GetJob(ctx context.Context, req *ironworkv1.GetJobRequest) (*ironworkv1.GetJobResponse, error) {
	if _, err := uuid.Parse(req.Id); err != nil {
		return nil, status.Error(codes.InvalidArgument, "id must be a UUID")
	}
	job, err := s.store.GetJob(ctx, req.Id)
	if errors.Is(err, store.ErrNotFound) {
		return nil, status.Errorf(codes.NotFound, "job %s not found", req.Id)
	}
	if err != nil {
		s.log.Error().Err(err).Str("job_id", req.Id).Msg("get job")
		return nil, status.Error(codes.Internal, "get job")
	}
	return &ironworkv1.GetJobResponse{Job: protoconv.JobToProto(job)}, nil
}

// ListJobs returns the newest jobs, optionally filtered by status.
func (s *Server) ListJobs(ctx context.Context, req *ironworkv1.ListJobsRequest) (*ironworkv1.ListJobsResponse, error) {
	filter := protoconv.StatusFromProto(req.StatusFilter)
	limit := int(req.PageSize)
	if limit < 1 || limit > maxLimit {
		limit = defaultLimit
	}

	jobs, err := s.store.ListJobs(ctx, filter, limit)
	if err != nil {
		s.log.Error().Err(err).Msg("list jobs")
		return nil, status.Error(codes.Internal, "list jobs")
	}

	out := &ironworkv1.ListJobsResponse{Jobs: make([]*ironworkv1.Job, 0, len(jobs))}
	for _, j := range jobs {
		out.Jobs = append(out.Jobs, protoconv.JobToProto(j))
	}
	return out, nil
}
