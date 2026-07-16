package raftnode

import (
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/hashicorp/raft"
)

// maxRecentPlacements bounds the placement history surfaced in RaftStatus.
const maxRecentPlacements = 16

// Placement is one placement decision replicated through the Raft log.
type Placement struct {
	JobID  string    `json:"job_id"`
	Worker string    `json:"worker"`
	At     time.Time `json:"at"`
}

// fsmState is the snapshot-able FSM state.
type fsmState struct {
	Count  uint64      `json:"count"`
	Recent []Placement `json:"recent"`
}

// placementFSM applies replicated placement entries. Every scheduler's FSM
// converges on the same count and recent history — the visible half of the
// consensus demo. It is deliberately not the store of record (Postgres is).
type placementFSM struct {
	mu    sync.Mutex
	state fsmState
}

// Apply decodes one placement entry into the FSM.
func (f *placementFSM) Apply(l *raft.Log) any {
	var p Placement
	if err := json.Unmarshal(l.Data, &p); err != nil {
		return fmt.Errorf("raftnode: decode placement entry: %w", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.state.Count++
	f.state.Recent = append(f.state.Recent, p)
	if len(f.state.Recent) > maxRecentPlacements {
		f.state.Recent = f.state.Recent[len(f.state.Recent)-maxRecentPlacements:]
	}
	return nil
}

// Placements returns the total applied count and a copy of the recent
// placements, newest last.
func (f *placementFSM) Placements() (uint64, []Placement) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]Placement, len(f.state.Recent))
	copy(out, f.state.Recent)
	return f.state.Count, out
}

// Snapshot captures the FSM state for log compaction.
func (f *placementFSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	data, err := json.Marshal(f.state)
	if err != nil {
		return nil, err
	}
	return fsmSnapshot(data), nil
}

// Restore replaces the FSM state from a snapshot.
func (f *placementFSM) Restore(rc io.ReadCloser) error {
	defer func() { _ = rc.Close() }()
	var st fsmState
	if err := json.NewDecoder(rc).Decode(&st); err != nil {
		return err
	}
	f.mu.Lock()
	f.state = st
	f.mu.Unlock()
	return nil
}

type fsmSnapshot []byte

func (s fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := sink.Write(s); err != nil {
		_ = sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s fsmSnapshot) Release() {}
