package leaderclient

import (
	"context"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	ironworkv1 "github.com/sohamghodake/ironwork/gen/ironwork/v1"
)

type fakeJobs struct {
	name  string
	err   error
	calls int
}

func (f *fakeJobs) SubmitJob(context.Context, *ironworkv1.SubmitJobRequest, ...grpc.CallOption) (*ironworkv1.SubmitJobResponse, error) {
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	return &ironworkv1.SubmitJobResponse{Job: &ironworkv1.Job{Id: "job-1"}}, nil
}

func (f *fakeJobs) GetJob(context.Context, *ironworkv1.GetJobRequest, ...grpc.CallOption) (*ironworkv1.GetJobResponse, error) {
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	return &ironworkv1.GetJobResponse{}, nil
}

func (f *fakeJobs) ListJobs(context.Context, *ironworkv1.ListJobsRequest, ...grpc.CallOption) (*ironworkv1.ListJobsResponse, error) {
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	return &ironworkv1.ListJobsResponse{}, nil
}

func testRouter(clients ...*fakeJobs) *Router {
	r := &Router{log: zerolog.Nop()}
	for _, c := range clients {
		r.members = append(r.members, member{name: c.name, jobs: c})
	}
	return r
}

var notLeader = status.Error(codes.FailedPrecondition, "not leader")

func TestRouteDiscoversAndCachesLeader(t *testing.T) {
	follower := &fakeJobs{name: "scheduler-1", err: notLeader}
	leader := &fakeJobs{name: "scheduler-2"}
	r := testRouter(follower, leader, &fakeJobs{name: "scheduler-3", err: notLeader})

	_, err := r.SubmitJob(context.Background(), &ironworkv1.SubmitJobRequest{Name: "a"})
	require.NoError(t, err)
	assert.Equal(t, 1, follower.calls)
	assert.Equal(t, 1, leader.calls)

	// Cached: the next call goes straight to the leader.
	_, err = r.SubmitJob(context.Background(), &ironworkv1.SubmitJobRequest{Name: "b"})
	require.NoError(t, err)
	assert.Equal(t, 1, follower.calls, "follower should not be retried once the leader is cached")
	assert.Equal(t, 2, leader.calls)
}

func TestRouteFailsOverWhenCachedLeaderDies(t *testing.T) {
	a := &fakeJobs{name: "scheduler-1"}
	b := &fakeJobs{name: "scheduler-2", err: notLeader}
	r := testRouter(a, b)

	_, err := r.SubmitJob(context.Background(), &ironworkv1.SubmitJobRequest{Name: "a"})
	require.NoError(t, err) // cached leader: a

	a.err = status.Error(codes.Unavailable, "connection refused")
	b.err = nil // b took over
	_, err = r.SubmitJob(context.Background(), &ironworkv1.SubmitJobRequest{Name: "b"})
	require.NoError(t, err)
	assert.Equal(t, int64(1), r.leader.Load(), "new leader should be cached")
}

func TestRouteNonRetryableReturnsImmediately(t *testing.T) {
	bad := &fakeJobs{name: "scheduler-1", err: status.Error(codes.InvalidArgument, "name is required")}
	other := &fakeJobs{name: "scheduler-2"}
	r := testRouter(bad, other)

	_, err := r.SubmitJob(context.Background(), &ironworkv1.SubmitJobRequest{})
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
	assert.Zero(t, other.calls, "a definitive answer must not be retried elsewhere")
}

func TestRouteAllDownExhaustsRounds(t *testing.T) {
	a := &fakeJobs{name: "scheduler-1", err: status.Error(codes.Unavailable, "down")}
	b := &fakeJobs{name: "scheduler-2", err: status.Error(codes.Unavailable, "down")}
	r := testRouter(a, b)

	start := time.Now()
	_, err := r.SubmitJob(context.Background(), &ironworkv1.SubmitJobRequest{Name: "a"})
	require.Error(t, err)
	assert.Equal(t, codes.Unavailable, status.Code(err))
	assert.Equal(t, retryRounds, a.calls)
	assert.Equal(t, retryRounds, b.calls)
	assert.GreaterOrEqual(t, time.Since(start), (retryRounds-1)*roundPause, "rounds should pause to ride out elections")
}

// --- RaftStatus fan-out ---

type fakeStates struct {
	resp *ironworkv1.GetClusterStateResponse
	err  error
}

func (f *fakeStates) GetClusterState(context.Context, *ironworkv1.GetClusterStateRequest, ...grpc.CallOption) (*ironworkv1.GetClusterStateResponse, error) {
	return f.resp, f.err
}

func TestRaftStatusFanOut(t *testing.T) {
	leaderView := &ironworkv1.GetClusterStateResponse{State: &ironworkv1.ClusterState{
		ReportingNode: "scheduler-1",
		Raft: &ironworkv1.RaftStatus{
			State: "Leader", LeaderId: "scheduler-1", Term: 3, LastLogIndex: 12, AppliedIndex: 12,
			RecentPlacements: []*ironworkv1.PlacementRecord{
				{JobId: "job-9", Worker: "worker-2", At: timestamppb.Now()},
			},
		},
	}}

	r := &Router{log: zerolog.Nop()}
	r.members = []member{
		{name: "scheduler-1", states: &fakeStates{resp: leaderView}},
		{name: "scheduler-2", states: &fakeStates{err: status.Error(codes.Unavailable, "stopped")}},
	}

	nodes := r.RaftStatus(context.Background())
	require.Len(t, nodes, 2)

	assert.True(t, nodes[0].Reachable)
	assert.Equal(t, "Leader", nodes[0].State)
	assert.Equal(t, uint64(3), nodes[0].Term)
	require.Len(t, nodes[0].Placements, 1)
	assert.Equal(t, "job-9", nodes[0].Placements[0].JobID)

	assert.False(t, nodes[1].Reachable)
	assert.Contains(t, nodes[1].Error, "stopped")
}
