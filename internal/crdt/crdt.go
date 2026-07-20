// Package crdt implements the conflict-free replicated data types backing
// the statemanager: a grow-only counter and a last-writer-wins map. Merge is
// commutative, associative, and idempotent, so replicas converge regardless
// of exchange order or repetition — no coordination required.
//
// Types are not thread-safe; the statemanager serializes access.
package crdt

// GCounter is a grow-only counter sharded by replica id: each replica
// increments only its own shard, so concurrent increments never conflict and
// merge is the pointwise maximum. Job counts only grow, which is why a
// G-Counter (rather than a PN-Counter) suffices here.
type GCounter map[string]uint64

// NewGCounter returns an empty counter.
func NewGCounter() GCounter { return GCounter{} }

// Inc adds one to this replica's shard.
func (c GCounter) Inc(replica string) { c[replica]++ }

// Value is the sum of all shards.
func (c GCounter) Value() uint64 {
	var sum uint64
	for _, v := range c {
		sum += v
	}
	return sum
}

// Merge folds other into c, taking the pointwise maximum per shard.
func (c GCounter) Merge(other GCounter) {
	for replica, v := range other {
		if v > c[replica] {
			c[replica] = v
		}
	}
}

// Clone returns an independent copy.
func (c GCounter) Clone() GCounter {
	out := make(GCounter, len(c))
	for r, v := range c {
		out[r] = v
	}
	return out
}

// LWWEntry is one last-writer-wins value: the latest observed outcome of a
// job, stamped with when and by which replica it was observed.
type LWWEntry struct {
	Status   string
	Worker   string
	TSUnixMS int64
	Replica  string
}

// wins reports whether a should replace b: newer timestamp wins, ties break
// deterministically on replica id so all replicas agree.
func wins(a, b LWWEntry) bool {
	if a.TSUnixMS != b.TSUnixMS {
		return a.TSUnixMS > b.TSUnixMS
	}
	return a.Replica > b.Replica
}

// LWWMap is a last-writer-wins map bounded to the newest limit entries.
// Pruning trades strict CRDT semantics for a bounded window: a pruned entry
// can reappear via merge, which is harmless for a recent-events view.
type LWWMap struct {
	entries map[string]LWWEntry
	limit   int
}

// NewLWWMap returns an empty map that keeps at most limit entries.
func NewLWWMap(limit int) *LWWMap {
	return &LWWMap{entries: map[string]LWWEntry{}, limit: limit}
}

// Set records e under key if it wins against any existing entry.
func (m *LWWMap) Set(key string, e LWWEntry) {
	if cur, ok := m.entries[key]; ok && !wins(e, cur) {
		return
	}
	m.entries[key] = e
	m.prune()
}

// Merge folds other's entries in under LWW rules.
func (m *LWWMap) Merge(other map[string]LWWEntry) {
	for key, e := range other {
		if cur, ok := m.entries[key]; ok && !wins(e, cur) {
			continue
		}
		m.entries[key] = e
	}
	m.prune()
}

// Entries returns a copy of the current window.
func (m *LWWMap) Entries() map[string]LWWEntry {
	out := make(map[string]LWWEntry, len(m.entries))
	for k, v := range m.entries {
		out[k] = v
	}
	return out
}

// prune drops the oldest entries beyond the limit.
func (m *LWWMap) prune() {
	for len(m.entries) > m.limit {
		oldestKey := ""
		var oldest LWWEntry
		first := true
		for k, e := range m.entries {
			if first || wins(oldest, e) {
				oldestKey, oldest, first = k, e, false
			}
		}
		delete(m.entries, oldestKey)
	}
}
