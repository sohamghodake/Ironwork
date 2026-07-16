// Package schedulersvc implements the scheduler's JobService: SubmitJob
// persists a job and places it on a worker (Phase 2: round-robin with
// fall-through — the same dumb placement the gateway used in Phase 1, now
// behind the scheduler seam); GetJob/ListJobs serve the gateway's reads.
// Raft leadership gates placement from Phase 3 on.
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
	CreateJob(ctx context.Context, name string, payload []byte) (*store.Job, error)
	GetJob(ctx context.Context, id string) (*store.Job, error)
	ListJobs(ctx context.Context, status string, limit int) ([]*store.Job, error)
	MarkFinished(ctx context.Context, id string, succeeded bool, errMsg string) error
}

// JobDispatcher places an accepted job on a worker.
type JobDispatcher interface {
	Dispatch(ctx context.Context, job *store.Job) (workerInstance string, err error)
}

// Leadership reports this scheduler's role in the Raft consensus group.
type Leadership interface {
	IsLeader() bool
	LeaderID() string
}

// PlacementLog replicates placement decisions to the consensus group.
type PlacementLog interface {
	ApplyPlacement(jobID, worker string) error
}

// Server implements JobService for the scheduler.
type Server struct {
	ironworkv1.UnimplementedJobServiceServer

	store JobStore
	disp  JobDispatcher
	lead  Leadership
	plog  PlacementLog
	log   zerolog.Logger
}

// New builds the scheduler's job service.
func New(st JobStore, disp JobDispatcher, lead Leadership, plog PlacementLog, log zerolog.Logger) *Server {
	return &Server{store: st, disp: disp, lead: lead, plog: plog, log: log}
}

// SubmitJob persists the job and places it. Only the Raft leader accepts
// submissions; followers reject fast so the gateway can route onward. The job
// resource is created even when placement fails — the failure is recorded on
// the job itself.
func (s *Server) SubmitJob(ctx context.Context, req *ironworkv1.SubmitJobRequest) (*ironworkv1.SubmitJobResponse, error) {
	if !s.lead.IsLeader() {
		return nil, status.Errorf(codes.FailedPrecondition, "not leader (leader: %s)", s.lead.LeaderID())
	}
	if req.Name == "" || len(req.Name) > maxNameLength {
		return nil, status.Error(codes.InvalidArgument, "name is required (max 200 chars)")
	}

	job, err := s.store.CreateJob(ctx, req.Name, req.Payload)
	if err != nil {
		s.log.Error().Err(err).Msg("create job")
		return nil, status.Error(codes.Internal, "create job")
	}

	if worker, err := s.disp.Dispatch(ctx, job); err != nil {
		s.log.Warn().Err(err).Str("job_id", job.ID).Msg("placement failed")
		if merr := s.store.MarkFinished(ctx, job.ID, false, err.Error()); merr != nil {
			s.log.Error().Err(merr).Str("job_id", job.ID).Msg("mark placement failure")
		}
	} else {
		s.log.Info().Str("job_id", job.ID).Str("worker", worker).Msg("job placed")
		// Replicate the decision through the Raft log. The job is already
		// placed — this is the authority trail, not the placement mechanism —
		// so a failed apply (e.g. leadership lost mid-flight) only warns.
		if aerr := s.plog.ApplyPlacement(job.ID, worker); aerr != nil {
			s.log.Warn().Err(aerr).Str("job_id", job.ID).Msg("placement log apply failed")
		}
	}

	// Re-read: the accepting worker has already recorded at least "scheduled".
	if fresh, err := s.store.GetJob(ctx, job.ID); err == nil {
		job = fresh
	}
	return &ironworkv1.SubmitJobResponse{Job: protoconv.JobToProto(job)}, nil
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
