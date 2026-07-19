package schedulersvc

import (
	"context"
	"time"

	"github.com/rs/zerolog"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	ironworkv1 "github.com/sohamghodake/ironwork/gen/ironwork/v1"
)

// WorkerRegistry records worker heartbeats.
type WorkerRegistry interface {
	Record(instance string, capacity, inflight int)
}

// WorkerServer is the scheduler-served half of WorkerService: workers
// register and heartbeat here (they fan out to every scheduler, keeping all
// registries warm across leader failovers). ExecuteJob stays unimplemented
// on this side — that half is served by the workers.
type WorkerServer struct {
	ironworkv1.UnimplementedWorkerServiceServer

	registry WorkerRegistry
	interval time.Duration
	log      zerolog.Logger
}

// NewWorkerServer builds the scheduler's worker-facing service.
func NewWorkerServer(reg WorkerRegistry, interval time.Duration, log zerolog.Logger) *WorkerServer {
	return &WorkerServer{registry: reg, interval: interval, log: log}
}

// RegisterWorker records the worker and advertises the heartbeat interval.
func (s *WorkerServer) RegisterWorker(_ context.Context, req *ironworkv1.RegisterWorkerRequest) (*ironworkv1.RegisterWorkerResponse, error) {
	if req.WorkerId == "" {
		return nil, status.Error(codes.InvalidArgument, "worker_id is required")
	}
	s.registry.Record(req.WorkerId, int(req.Capacity), 0)
	s.log.Info().Str("worker", req.WorkerId).Uint32("capacity", req.Capacity).Msg("worker registered")
	return &ironworkv1.RegisterWorkerResponse{
		HeartbeatIntervalSeconds: uint32(s.interval.Seconds()), //nolint:gosec // small constant
	}, nil
}

// Heartbeat refreshes the worker's liveness and load.
func (s *WorkerServer) Heartbeat(_ context.Context, req *ironworkv1.HeartbeatRequest) (*ironworkv1.HeartbeatResponse, error) {
	if req.WorkerId == "" {
		return nil, status.Error(codes.InvalidArgument, "worker_id is required")
	}
	s.registry.Record(req.WorkerId, int(req.Capacity), int(req.Inflight))
	return &ironworkv1.HeartbeatResponse{}, nil
}
