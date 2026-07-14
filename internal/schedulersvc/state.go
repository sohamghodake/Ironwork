package schedulersvc

import (
	"context"

	"google.golang.org/protobuf/types/known/timestamppb"

	ironworkv1 "github.com/sohamghodake/ironwork/gen/ironwork/v1"
	"github.com/sohamghodake/ironwork/internal/raftnode"
)

// RaftInfo provides the consensus view surfaced via StateService.
type RaftInfo interface {
	Info() raftnode.Info
}

// StateServer implements StateService for a scheduler: GetClusterState
// reports this node's Raft view plus the replicated placement history. The
// gateway's /raft endpoint fans this out across all schedulers.
type StateServer struct {
	ironworkv1.UnimplementedStateServiceServer

	instance string
	raft     RaftInfo
}

// NewStateServer builds the scheduler's state service.
func NewStateServer(instance string, raft RaftInfo) *StateServer {
	return &StateServer{instance: instance, raft: raft}
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

	return &ironworkv1.GetClusterStateResponse{
		State: &ironworkv1.ClusterState{
			ReportingNode: s.instance,
			AsOf:          timestamppb.Now(),
			Raft:          rs,
		},
	}, nil
}
