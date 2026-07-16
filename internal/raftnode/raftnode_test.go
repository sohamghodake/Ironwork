package raftnode

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTestCluster builds a fully connected in-process 3-node cluster with
// aggressive timeouts so elections settle in tens of milliseconds.
func newTestCluster(t *testing.T) []*Node {
	t.Helper()

	const n = 3
	nodes := make([]*Node, n)
	addrs := make([]raft.ServerAddress, n)
	transports := make([]*raft.InmemTransport, n)
	servers := make([]raft.Server, n)

	for i := 0; i < n; i++ {
		id := fmt.Sprintf("node-%d", i)
		addrs[i], transports[i] = raft.NewInmemTransport(raft.ServerAddress(id))
		servers[i] = raft.Server{ID: raft.ServerID(id), Address: addrs[i]}
	}
	for i := 0; i < n; i++ {
		for j := 0; j < n; j++ {
			if i != j {
				transports[i].Connect(addrs[j], transports[j])
			}
		}
	}

	for i := 0; i < n; i++ {
		rcfg := raft.DefaultConfig()
		rcfg.LocalID = servers[i].ID
		rcfg.HeartbeatTimeout = 50 * time.Millisecond
		rcfg.ElectionTimeout = 50 * time.Millisecond
		rcfg.LeaderLeaseTimeout = 50 * time.Millisecond
		rcfg.CommitTimeout = 5 * time.Millisecond
		rcfg.LogLevel = "ERROR"

		node, err := newWithBackends(string(servers[i].ID), rcfg,
			raft.NewInmemStore(), raft.NewInmemStore(), raft.NewInmemSnapshotStore(),
			transports[i], zerolog.Nop())
		require.NoError(t, err)
		require.NoError(t, node.raft.BootstrapCluster(raft.Configuration{Servers: servers}).Error())
		nodes[i] = node
		t.Cleanup(func() { _ = node.raft.Shutdown().Error() })
	}
	return nodes
}

// waitForLeader polls until exactly one of the given nodes reports leadership.
func waitForLeader(t *testing.T, nodes []*Node, within time.Duration) *Node {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		var leaders []*Node
		for _, n := range nodes {
			if n.IsLeader() {
				leaders = append(leaders, n)
			}
		}
		if len(leaders) == 1 {
			return leaders[0]
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("no single leader elected in time")
	return nil
}

func TestElectionAndReplication(t *testing.T) {
	nodes := newTestCluster(t)
	leader := waitForLeader(t, nodes, 5*time.Second)

	for i := 1; i <= 3; i++ {
		require.NoError(t, leader.ApplyPlacement(fmt.Sprintf("job-%d", i), "worker-1"))
	}

	// Every FSM converges on the same placement history.
	require.Eventually(t, func() bool {
		for _, n := range nodes {
			count, _ := n.fsm.Placements()
			if count != 3 {
				return false
			}
		}
		return true
	}, 3*time.Second, 20*time.Millisecond, "placements should replicate to all FSMs")

	for _, n := range nodes {
		_, recent := n.fsm.Placements()
		require.Len(t, recent, 3)
		assert.Equal(t, "job-1", recent[0].JobID)
		assert.Equal(t, "job-3", recent[2].JobID)
	}

	info := leader.Info()
	assert.Equal(t, "Leader", info.State)
	assert.Equal(t, leader.id, info.LeaderID)
	assert.Equal(t, []string{"node-0", "node-1", "node-2"}, info.Peers)
	assert.GreaterOrEqual(t, info.Term, uint64(1))
}

func TestFailoverElectsNewLeader(t *testing.T) {
	nodes := newTestCluster(t)
	leader := waitForLeader(t, nodes, 5*time.Second)
	require.NoError(t, leader.ApplyPlacement("job-before", "worker-1"))

	require.NoError(t, leader.raft.Shutdown().Error())

	var survivors []*Node
	for _, n := range nodes {
		if n != leader {
			survivors = append(survivors, n)
		}
	}
	newLeader := waitForLeader(t, survivors, 5*time.Second)
	assert.NotEqual(t, leader.id, newLeader.id)

	// The new leader carries the replicated history and accepts new entries.
	require.Eventually(t, func() bool {
		return newLeader.ApplyPlacement("job-after", "worker-2") == nil
	}, 3*time.Second, 50*time.Millisecond)
	count, _ := newLeader.fsm.Placements()
	assert.Equal(t, uint64(2), count)

	// Followers reject applies fast.
	var follower *Node
	for _, n := range survivors {
		if n != newLeader {
			follower = n
		}
	}
	assert.Error(t, follower.ApplyPlacement("job-nope", "worker-1"))
}

// --- pure FSM ---

type memSink struct{ bytes.Buffer }

func (s *memSink) ID() string    { return "test" }
func (s *memSink) Cancel() error { return nil }
func (s *memSink) Close() error  { return nil }

func TestFSMSnapshotRestoreRoundTrip(t *testing.T) {
	fsm := &placementFSM{}
	for i := 0; i < 20; i++ { // overflow the ring buffer
		data, err := json.Marshal(Placement{JobID: fmt.Sprintf("job-%d", i), Worker: "w"})
		require.NoError(t, err)
		require.Nil(t, fsm.Apply(&raft.Log{Data: data}))
	}

	count, recent := fsm.Placements()
	assert.Equal(t, uint64(20), count)
	require.Len(t, recent, maxRecentPlacements)
	assert.Equal(t, "job-4", recent[0].JobID) // oldest retained after overflow

	snap, err := fsm.Snapshot()
	require.NoError(t, err)
	sink := &memSink{}
	require.NoError(t, snap.Persist(sink))

	restored := &placementFSM{}
	require.NoError(t, restored.Restore(io.NopCloser(&sink.Buffer)))
	rCount, rRecent := restored.Placements()
	assert.Equal(t, count, rCount)
	assert.Equal(t, recent, rRecent)
}

func TestFSMRejectsGarbage(t *testing.T) {
	fsm := &placementFSM{}
	resp := fsm.Apply(&raft.Log{Data: []byte("not json")})
	_, ok := resp.(error)
	assert.True(t, ok, "garbage entries should surface as apply errors")
}
