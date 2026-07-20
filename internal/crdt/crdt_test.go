package crdt_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sohamghodake/ironwork/internal/crdt"
)

// counterFixtures builds pairs of counters with overlapping and disjoint
// shards for the merge-law tables.
func counterFixtures() [][2]crdt.GCounter {
	return [][2]crdt.GCounter{
		{{"a": 1, "b": 5}, {"a": 3, "c": 2}}, // overlapping
		{{"a": 4}, {"b": 7}},                 // disjoint
		{{}, {"a": 1}},                       // empty vs non-empty
		{{"a": 2, "b": 2}, {"a": 2, "b": 2}}, // identical
	}
}

func TestGCounterMergeIsCommutative(t *testing.T) {
	for i, fix := range counterFixtures() {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			ab := fix[0].Clone()
			ab.Merge(fix[1])
			ba := fix[1].Clone()
			ba.Merge(fix[0])
			assert.Equal(t, ab, ba, "merge(a,b) must equal merge(b,a)")
		})
	}
}

func TestGCounterMergeIsAssociative(t *testing.T) {
	a := crdt.GCounter{"a": 1, "b": 9}
	b := crdt.GCounter{"b": 4, "c": 2}
	c := crdt.GCounter{"a": 7, "c": 1}

	abThenC := a.Clone()
	abThenC.Merge(b)
	abThenC.Merge(c)

	bcThenA := b.Clone()
	bcThenA.Merge(c)
	withA := a.Clone()
	withA.Merge(bcThenA)

	assert.Equal(t, abThenC, withA, "(a⊔b)⊔c must equal a⊔(b⊔c)")
}

func TestGCounterMergeIsIdempotent(t *testing.T) {
	for i, fix := range counterFixtures() {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			once := fix[0].Clone()
			once.Merge(fix[1])
			twice := once.Clone()
			twice.Merge(fix[1])
			assert.Equal(t, once, twice, "re-merging the same state must change nothing")
		})
	}
}

func TestGCounterIncAndValue(t *testing.T) {
	c := crdt.NewGCounter()
	c.Inc("statemanager-1")
	c.Inc("statemanager-1")
	c.Inc("statemanager-2")

	assert.Equal(t, uint64(3), c.Value())
	assert.Equal(t, crdt.GCounter{"statemanager-1": 2, "statemanager-2": 1}, c)
}

func TestGCounterConcurrentIncrementsSurviveMerge(t *testing.T) {
	// Two replicas advance independently during a partition...
	r1 := crdt.GCounter{"statemanager-1": 5}
	r2 := crdt.GCounter{"statemanager-2": 3}
	r1.Inc("statemanager-1") // 6
	r2.Inc("statemanager-2") // 4

	// ...then heal in both directions.
	m1 := r1.Clone()
	m1.Merge(r2)
	m2 := r2.Clone()
	m2.Merge(r1)

	assert.Equal(t, uint64(10), m1.Value(), "no increment may be lost")
	assert.Equal(t, m1, m2)
}

func TestLWWMapNewerWins(t *testing.T) {
	m := crdt.NewLWWMap(10)
	m.Set("job-1", crdt.LWWEntry{Status: "running", TSUnixMS: 100, Replica: "sm-1"})
	m.Set("job-1", crdt.LWWEntry{Status: "succeeded", TSUnixMS: 200, Replica: "sm-2"})
	m.Set("job-1", crdt.LWWEntry{Status: "stale", TSUnixMS: 150, Replica: "sm-1"})

	assert.Equal(t, "succeeded", m.Entries()["job-1"].Status, "older writes must not clobber newer state")
}

func TestLWWMapTieBreaksOnReplica(t *testing.T) {
	a := crdt.NewLWWMap(10)
	a.Set("job-1", crdt.LWWEntry{Status: "from-sm-1", TSUnixMS: 100, Replica: "sm-1"})
	b := crdt.NewLWWMap(10)
	b.Set("job-1", crdt.LWWEntry{Status: "from-sm-2", TSUnixMS: 100, Replica: "sm-2"})

	a.Merge(b.Entries())
	b.Merge(a.Entries())

	require.Equal(t, a.Entries(), b.Entries(), "replicas must agree after symmetric merge")
	assert.Equal(t, "from-sm-2", a.Entries()["job-1"].Status, "ties break to the higher replica id")
}

func TestLWWMapPrunesOldest(t *testing.T) {
	m := crdt.NewLWWMap(3)
	for i := 1; i <= 5; i++ {
		m.Set(fmt.Sprintf("job-%d", i), crdt.LWWEntry{TSUnixMS: int64(i * 100), Replica: "sm-1"})
	}

	entries := m.Entries()
	require.Len(t, entries, 3)
	assert.NotContains(t, entries, "job-1")
	assert.NotContains(t, entries, "job-2")
	assert.Contains(t, entries, "job-5")
}
