// Package registry tracks worker liveness and load from heartbeats. Every
// scheduler keeps its own registry (workers report to all members), so the
// Raft leader always has fresh data no matter when it was elected.
package registry

import (
	"sort"
	"sync"
	"time"
)

// WorkerState is one worker's last-reported liveness and load.
type WorkerState struct {
	Instance string
	Capacity int
	Inflight int
	LastSeen time.Time
	// Alive is false once heartbeats have gone stale past the dead TTL.
	Alive bool
}

// Registry is a thread-safe heartbeat table.
type Registry struct {
	mu      sync.Mutex
	workers map[string]WorkerState

	// staleTTL: too old to receive new placements. deadTTL: presumed
	// crashed, jobs eligible for reclaim.
	staleTTL time.Duration
	deadTTL  time.Duration
	now      func() time.Time
}

// New builds a registry with the given staleness thresholds.
func New(staleTTL, deadTTL time.Duration) *Registry {
	return &Registry{
		workers:  map[string]WorkerState{},
		staleTTL: staleTTL,
		deadTTL:  deadTTL,
		now:      time.Now,
	}
}

// Record stores a heartbeat.
func (r *Registry) Record(instance string, capacity, inflight int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.workers[instance] = WorkerState{
		Instance: instance,
		Capacity: capacity,
		Inflight: inflight,
		LastSeen: r.now(),
	}
}

// Snapshot returns every tracked worker sorted by instance name, with Alive
// evaluated against the dead TTL.
func (r *Registry) Snapshot() []WorkerState {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.now()
	out := make([]WorkerState, 0, len(r.workers))
	for _, w := range r.workers {
		w.Alive = now.Sub(w.LastSeen) <= r.deadTTL
		out = append(out, w)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Instance < out[j].Instance })
	return out
}

// Available returns placement candidates: fresh heartbeat and free capacity,
// most headroom first (ties broken by name for determinism).
func (r *Registry) Available() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.now()

	type cand struct {
		name string
		free int
	}
	cands := []cand{}
	for _, w := range r.workers {
		if now.Sub(w.LastSeen) <= r.staleTTL && w.Inflight < w.Capacity {
			cands = append(cands, cand{name: w.Instance, free: w.Capacity - w.Inflight})
		}
	}
	sort.Slice(cands, func(i, j int) bool {
		if cands[i].free != cands[j].free {
			return cands[i].free > cands[j].free
		}
		return cands[i].name < cands[j].name
	})

	out := make([]string, len(cands))
	for i, c := range cands {
		out[i] = c.name
	}
	return out
}

// Dead returns workers whose heartbeats are older than the dead TTL — their
// jobs are eligible for reclaim.
func (r *Registry) Dead() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.now()
	out := []string{}
	for _, w := range r.workers {
		if now.Sub(w.LastSeen) > r.deadTTL {
			out = append(out, w.Instance)
		}
	}
	sort.Strings(out)
	return out
}

// Forget drops a worker (after its jobs were reclaimed); a returning
// heartbeat re-registers it.
func (r *Registry) Forget(instance string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.workers, instance)
}

// SetClock overrides the time source for tests.
func (r *Registry) SetClock(now func() time.Time) { r.now = now }
