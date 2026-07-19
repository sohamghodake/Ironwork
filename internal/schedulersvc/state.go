package schedulersvc

import (
	"context"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	ironworkv1 "github.com/sohamghodake/ironwork/gen/ironwork/v1"
	"github.com/sohamghodake/ironwork/internal/raftnode"
	"github.com/sohamghodake/ironwork/internal/registry"
)

// RaftInfo provides the consensus view surfaced via StateService.
type RaftInfo interface {
	Info() raftnode.Info
}

// WorkerView provides the heartbeat registry's worker table.
type WorkerView interface {
	Snapshot() []registry.WorkerState
}

// StateServer implements StateService for a scheduler: GetClusterState
// reports this node's Raft view, the replicated placement history, and its
// heartbeat registry's worker table. The gateway's /raft and /workers
// endpoints fan this out across all schedulers.
type StateServer struct {
	ironworkv1.UnimplementedStateServiceServer

	instance string
	raft     RaftInfo
	workers  WorkerView
}

// NewStateServer builds the scheduler's state service.
func NewStateServer(instance string, raft RaftInfo, workers WorkerView) *StateServer {
	return &StateServer{instance: instance, raft: raft, workers: workers}
}

// GetClusterState returns this scheduler's point-in-time consensus view.
func (s *StateServer) GetClusterState(context.Context, *ironworkv1.GetClusterStateRequest) (*ironworkv1.GetClusterStateResponse, error) {
	info := s.raft.Info()

	rs := &ironworkv1.RaftStatus{
		State:        info.State,
		LeaderId:     info.LeaderID,
		Term:         info.Term,
		LastLogIndex: info.LastLogIndex,
		AppliedIndex: info.AppliedIndex,
		Peers:        info.Peers,
	}
	for _, p := range info.RecentPlacements {
		rs.RecentPlacements = append(rs.RecentPlacements, &ironworkv1.PlacementRecord{
			JobId:  p.JobID,
			Worker: p.Worker,
			At:     timestamppb.New(p.At),
		})
	}

	var workers []*ironworkv1.WorkerStatus
	for _, w := range s.workers.Snapshot() {
		workers = append(workers, &ironworkv1.WorkerStatus{
			Instance:              w.Instance,
			Alive:                 w.Alive,
			SecondsSinceHeartbeat: time.Since(w.LastSeen).Seconds(),
			Capacity:              uint32(w.Capacity), //nolint:gosec // small config value
			Inflight:              uint32(w.Inflight), //nolint:gosec // bounded by capacity
		})
	}

	return &ironworkv1.GetClusterStateResponse{
		State: &ironworkv1.ClusterState{
			ReportingNode: s.instance,
			AsOf:          timestamppb.Now(),
			Raft:          rs,
			Workers:       workers,
		},
	}, nil
}
