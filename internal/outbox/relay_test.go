package outbox

import (
	"context"
	"errors"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sohamghodake/ironwork/internal/store"
)

type fakeStore struct {
	pending    []*store.OutboxEntry
	dispatched []string
	failures   map[string]string // outbox id -> last error
	abandoned  map[string]string // outbox id -> reason
	jobsFailed map[string]string // job id -> reason
}

func newFakeStore(entries ...*store.OutboxEntry) *fakeStore {
	return &fakeStore{
		pending:    entries,
		failures:   map[string]string{},
		abandoned:  map[string]string{},
		jobsFailed: map[string]string{},
	}
}

func (f *fakeStore) ClaimOutbox(context.Context, int) ([]*store.OutboxEntry, error) {
	return f.pending, nil
}
func (f *fakeStore) MarkOutboxDispatched(_ context.Context, id string) error {
	f.dispatched = append(f.dispatched, id)
	return nil
}
func (f *fakeStore) RecordOutboxFailure(_ context.Context, id, errMsg string) error {
	f.failures[id] = errMsg
	return nil
}
func (f *fakeStore) MarkOutboxFailed(_ context.Context, id, errMsg string) error {
	f.abandoned[id] = errMsg
	return nil
}
func (f *fakeStore) MarkFinished(_ context.Context, id string, _ bool, errMsg string) error {
	f.jobsFailed[id] = errMsg
	return nil
}

type fakeDispatcher struct {
	worker string
	err    error
	placed []string
}

func (f *fakeDispatcher) Dispatch(_ context.Context, job *store.Job) (string, error) {
	if f.err != nil {
		return "", f.err
	}
	f.placed = append(f.placed, job.ID)
	return f.worker, nil
}

type fakeLog struct{ applied []string }

func (f *fakeLog) ApplyPlacement(jobID, worker string) error {
	f.applied = append(f.applied, jobID+"@"+worker)
	return nil
}

type leader bool

func (l leader) IsLeader() bool { return bool(l) }

func entry(id, jobID, status string, jobAttempts, dispatchAttempts int) *store.OutboxEntry {
	return &store.OutboxEntry{
		ID:       id,
		Attempts: dispatchAttempts,
		Job:      &store.Job{ID: jobID, Status: status, Attempts: jobAttempts},
	}
}

func TestDrainDispatchesPendingCommands(t *testing.T) {
	st := newFakeStore(
		entry("o-1", "j-1", store.StatusPending, 0, 0),
		entry("o-2", "j-2", store.StatusPending, 0, 0),
	)
	disp := &fakeDispatcher{worker: "worker-1"}
	plog := &fakeLog{}
	r := New(st, disp, leader(true), plog, zerolog.Nop())

	r.drain(context.Background())

	assert.Equal(t, []string{"j-1", "j-2"}, disp.placed)
	assert.Equal(t, []string{"o-1", "o-2"}, st.dispatched)
	assert.Equal(t, []string{"j-1@worker-1", "j-2@worker-1"}, plog.applied)
}

func TestDrainDefersOnDispatchFailure(t *testing.T) {
	st := newFakeStore(entry("o-1", "j-1", store.StatusPending, 0, 0))
	disp := &fakeDispatcher{err: errors.New("all workers full")}
	r := New(st, disp, leader(true), &fakeLog{}, zerolog.Nop())

	r.drain(context.Background())

	assert.Empty(t, st.dispatched, "a deferred command stays pending")
	assert.Contains(t, st.failures["o-1"], "all workers full")
	assert.Empty(t, st.jobsFailed, "deferral is not failure")
}

func TestDrainReconcilesAlreadyDispatched(t *testing.T) {
	// The job already moved past pending — a worker accepted it — so the
	// command is closed out without a second dispatch.
	st := newFakeStore(entry("o-1", "j-1", store.StatusRunning, 1, 0))
	disp := &fakeDispatcher{worker: "worker-1"}
	r := New(st, disp, leader(true), &fakeLog{}, zerolog.Nop())

	r.drain(context.Background())

	assert.Empty(t, disp.placed, "a running job must not be re-dispatched")
	assert.Equal(t, []string{"o-1"}, st.dispatched)
}

func TestDrainAbandonsAfterExecutionBudget(t *testing.T) {
	st := newFakeStore(entry("o-1", "j-1", store.StatusPending, maxExecutionAttempts, 0))
	disp := &fakeDispatcher{worker: "worker-1"}
	r := New(st, disp, leader(true), &fakeLog{}, zerolog.Nop())

	r.drain(context.Background())

	assert.Empty(t, disp.placed)
	assert.Contains(t, st.abandoned["o-1"], "execution attempts")
	assert.Contains(t, st.jobsFailed["j-1"], "execution attempts")
}

func TestDrainAbandonsAfterDispatchBudget(t *testing.T) {
	st := newFakeStore(entry("o-1", "j-1", store.StatusPending, 0, maxDispatchAttempts))
	disp := &fakeDispatcher{worker: "worker-1"}
	r := New(st, disp, leader(true), &fakeLog{}, zerolog.Nop())

	r.drain(context.Background())

	assert.Empty(t, disp.placed)
	assert.Contains(t, st.abandoned["o-1"], "undispatchable")
	assert.Contains(t, st.jobsFailed["j-1"], "undispatchable")
}

func TestWakeIsNonBlocking(t *testing.T) {
	r := New(newFakeStore(), &fakeDispatcher{}, leader(true), &fakeLog{}, zerolog.Nop())
	// More wakes than the buffer must not block.
	for i := 0; i < 5; i++ {
		r.Wake()
	}
	require.Len(t, r.wake, 1, "coalesced to a single pending wake")
}
