package statesvc_test

import (
	"context"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	ironworkv1 "github.com/sohamghodake/ironwork/gen/ironwork/v1"
	"github.com/sohamghodake/ironwork/internal/statesvc"
)

func report(t *testing.T, s *statesvc.Server, jobID, worker, outcome string) {
	t.Helper()
	_, err := s.ReportJobEvent(context.Background(), &ironworkv1.ReportJobEventRequest{
		JobId: jobID, Worker: worker, Outcome: outcome,
	})
	require.NoError(t, err)
}

func state(t *testing.T, s *statesvc.Server) *ironworkv1.GetCRDTStateResponse {
	t.Helper()
	resp, err := s.GetCRDTState(context.Background(), &ironworkv1.GetCRDTStateRequest{})
	require.NoError(t, err)
	return resp
}

// exchange runs one push-pull round from a to b (a pushes, merges b's reply).
func exchange(t *testing.T, a, b *statesvc.Server) {
	t.Helper()
	resp, err := b.SyncState(context.Background(), &ironworkv1.SyncStateRequest{State: state(t, a).State})
	require.NoError(t, err)
	// a merges the pull half by syncing the response back through itself.
	_, err = a.SyncState(context.Background(), &ironworkv1.SyncStateRequest{State: resp.State})
	require.NoError(t, err)
}

func TestReportAccumulatesInOwnShard(t *testing.T) {
	s := statesvc.New("statemanager-1", zerolog.Nop())
	report(t, s, "job-1", "worker-1", statesvc.OutcomeSucceeded)
	report(t, s, "job-2", "worker-2", statesvc.OutcomeSucceeded)
	report(t, s, "job-3", "worker-1", statesvc.OutcomeFailed)

	st := state(t, s).State
	assert.Equal(t, uint64(2), st.ByStatus["succeeded"].Shards["statemanager-1"])
	assert.Equal(t, uint64(1), st.ByStatus["failed"].Shards["statemanager-1"])
	assert.Equal(t, uint64(2), st.ByWorker["worker-1"].Shards["statemanager-1"])
	assert.Len(t, st.Recent, 3)
}

func TestReportValidatesOutcome(t *testing.T) {
	s := statesvc.New("statemanager-1", zerolog.Nop())
	_, err := s.ReportJobEvent(context.Background(), &ironworkv1.ReportJobEventRequest{
		JobId: "j", Worker: "w", Outcome: "exploded",
	})
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestPushPullConverges(t *testing.T) {
	sm1 := statesvc.New("statemanager-1", zerolog.Nop())
	sm2 := statesvc.New("statemanager-2", zerolog.Nop())

	// Concurrent independent writes on both replicas.
	report(t, sm1, "job-1", "worker-1", statesvc.OutcomeSucceeded)
	report(t, sm1, "job-2", "worker-1", statesvc.OutcomeSucceeded)
	report(t, sm2, "job-3", "worker-2", statesvc.OutcomeSucceeded)
	report(t, sm2, "job-4", "worker-2", statesvc.OutcomeFailed)

	exchange(t, sm1, sm2)

	st1, st2 := state(t, sm1).State, state(t, sm2).State
	assert.Equal(t, st1.ByStatus["succeeded"].Shards, st2.ByStatus["succeeded"].Shards)
	assert.Equal(t, uint64(2)+uint64(1), st1.ByStatus["succeeded"].Shards["statemanager-1"]+st1.ByStatus["succeeded"].Shards["statemanager-2"])
	assert.Len(t, st1.Recent, 4)
	assert.Len(t, st2.Recent, 4)

	// Idempotence end to end: another exchange changes nothing.
	before := state(t, sm1).State.String()
	exchange(t, sm1, sm2)
	assert.Equal(t, before, state(t, sm1).State.String())
}

func TestPartitionedReplicaRefusesGossip(t *testing.T) {
	s := statesvc.New("statemanager-1", zerolog.Nop())
	_, err := s.SetGossip(context.Background(), &ironworkv1.SetGossipRequest{Enabled: false})
	require.NoError(t, err)

	_, err = s.SyncState(context.Background(), &ironworkv1.SyncStateRequest{})
	assert.Equal(t, codes.Unavailable, status.Code(err))
	assert.False(t, state(t, s).GossipEnabled)

	// Writes still land while partitioned — availability under partition is
	// the whole point.
	report(t, s, "job-1", "worker-1", statesvc.OutcomeSucceeded)
	assert.Equal(t, uint64(1), state(t, s).State.ByStatus["succeeded"].Shards["statemanager-1"])
}
