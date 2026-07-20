package reaper

import (
	"context"
	"errors"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"

	"github.com/sohamghodake/ironwork/internal/store"
)

type fakeStore struct {
	reclaimable map[string][]*store.Job
	reclaimErr  error
}

func newFakeStore() *fakeStore {
	return &fakeStore{reclaimable: map[string][]*store.Job{}}
}

func (f *fakeStore) ReclaimJobs(_ context.Context, w string) ([]*store.Job, error) {
	if f.reclaimErr != nil {
		return nil, f.reclaimErr
	}
	jobs := f.reclaimable[w]
	delete(f.reclaimable, w)
	return jobs, nil
}

type fakeRegistry struct {
	dead      []string
	forgotten []string
}

func (f *fakeRegistry) Dead() []string { return f.dead }
func (f *fakeRegistry) Forget(w string) {
	f.forgotten = append(f.forgotten, w)
	f.dead = nil
}

type leader bool

func (l leader) IsLeader() bool { return bool(l) }

type fakeWaker struct{ wakes int }

func (f *fakeWaker) Wake() { f.wakes++ }

func job(id string) *store.Job { return &store.Job{ID: id, Status: store.StatusPending} }

func TestSweepReclaimsDeadWorkerAndWakesRelay(t *testing.T) {
	st := newFakeStore()
	st.reclaimable["worker-2"] = []*store.Job{job("j-1"), job("j-2")}
	reg := &fakeRegistry{dead: []string{"worker-2"}}
	waker := &fakeWaker{}

	r := New(st, reg, leader(true), waker, zerolog.Nop())
	r.sweep(context.Background())

	assert.Equal(t, []string{"worker-2"}, reg.forgotten)
	assert.Equal(t, 1, waker.wakes, "re-enqueued commands should wake the relay")
}

func TestSweepNoDeadWorkersDoesNotWake(t *testing.T) {
	waker := &fakeWaker{}
	r := New(newFakeStore(), &fakeRegistry{}, leader(true), waker, zerolog.Nop())

	r.sweep(context.Background())

	assert.Zero(t, waker.wakes)
}

func TestSweepDeadWorkerWithNoJobsForgetsButDoesNotWake(t *testing.T) {
	reg := &fakeRegistry{dead: []string{"worker-2"}}
	waker := &fakeWaker{}
	r := New(newFakeStore(), reg, leader(true), waker, zerolog.Nop())

	r.sweep(context.Background())

	assert.Equal(t, []string{"worker-2"}, reg.forgotten, "a dead worker with no jobs is still forgotten")
	assert.Zero(t, waker.wakes)
}

func TestSweepReclaimErrorKeepsWorker(t *testing.T) {
	st := newFakeStore()
	st.reclaimErr = errors.New("db down")
	reg := &fakeRegistry{dead: []string{"worker-2"}}
	waker := &fakeWaker{}
	r := New(st, reg, leader(true), waker, zerolog.Nop())

	r.sweep(context.Background())

	assert.Empty(t, reg.forgotten, "a failed reclaim must not forget the worker")
	assert.Zero(t, waker.wakes)
}
