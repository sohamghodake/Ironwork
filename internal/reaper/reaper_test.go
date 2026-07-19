package reaper

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"

	"github.com/sohamghodake/ironwork/internal/store"
)

type fakeStore struct {
	reclaimable map[string][]*store.Job
	unplaced    []*store.Job
	failed      map[string]string
}

func newFakeStore() *fakeStore {
	return &fakeStore{reclaimable: map[string][]*store.Job{}, failed: map[string]string{}}
}

func (f *fakeStore) ReclaimJobs(_ context.Context, w string) ([]*store.Job, error) {
	jobs := f.reclaimable[w]
	delete(f.reclaimable, w)
	return jobs, nil
}

func (f *fakeStore) ListUnplaced(context.Context, time.Duration, int) ([]*store.Job, error) {
	out := f.unplaced
	f.unplaced = nil
	return out, nil
}

func (f *fakeStore) MarkFinished(_ context.Context, id string, _ bool, errMsg string) error {
	f.failed[id] = errMsg
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

type fakeRegistry struct {
	dead      []string
	forgotten []string
}

func (f *fakeRegistry) Dead() []string { return f.dead }
func (f *fakeRegistry) Forget(w string) {
	f.forgotten = append(f.forgotten, w)
	f.dead = nil
}

type fakeLog struct{ applied []string }

func (f *fakeLog) ApplyPlacement(jobID, worker string) error {
	f.applied = append(f.applied, jobID+"@"+worker)
	return nil
}

type leader bool

func (l leader) IsLeader() bool { return bool(l) }

func job(id string, attempts int, age time.Duration) *store.Job {
	return &store.Job{ID: id, Attempts: attempts, CreatedAt: time.Now().Add(-age), Status: store.StatusPending}
}

func TestSweepReassignsDeadWorkersJobs(t *testing.T) {
	st := newFakeStore()
	st.reclaimable["worker-2"] = []*store.Job{job("j-1", 1, time.Minute), job("j-2", 1, 2*time.Minute)}
	disp := &fakeDispatcher{worker: "worker-1"}
	reg := &fakeRegistry{dead: []string{"worker-2"}}
	plog := &fakeLog{}

	r := New(st, disp, reg, leader(true), plog, zerolog.Nop())
	r.sweep(context.Background())

	assert.Equal(t, []string{"j-1", "j-2"}, disp.placed)
	assert.Equal(t, []string{"worker-2"}, reg.forgotten)
	assert.Equal(t, []string{"j-1@worker-1", "j-2@worker-1"}, plog.applied)
	assert.Empty(t, st.failed)
}

func TestSweepRetriesUnplacedJobs(t *testing.T) {
	st := newFakeStore()
	st.unplaced = []*store.Job{job("j-1", 0, 5*time.Second)}
	disp := &fakeDispatcher{worker: "worker-1"}

	r := New(st, disp, &fakeRegistry{}, leader(true), &fakeLog{}, zerolog.Nop())
	r.sweep(context.Background())

	assert.Equal(t, []string{"j-1"}, disp.placed)
}

func TestPlaceEnforcesAttemptBudget(t *testing.T) {
	st := newFakeStore()
	st.reclaimable["worker-2"] = []*store.Job{job("crashed-thrice", 3, time.Minute)}
	disp := &fakeDispatcher{worker: "worker-1"}
	reg := &fakeRegistry{dead: []string{"worker-2"}}

	r := New(st, disp, reg, leader(true), &fakeLog{}, zerolog.Nop())
	r.sweep(context.Background())

	assert.Empty(t, disp.placed)
	assert.Contains(t, st.failed["crashed-thrice"], "gave up after 3")
}

func TestPlaceExpiresNeverExecutedJobsOnly(t *testing.T) {
	st := newFakeStore()
	st.unplaced = []*store.Job{
		job("fresh", 0, 5*time.Second),
		job("expired", 0, 2*time.Minute),
		job("old-but-ran", 2, 2*time.Minute),
	}
	disp := &fakeDispatcher{worker: "worker-1"}

	r := New(st, disp, &fakeRegistry{}, leader(true), &fakeLog{}, zerolog.Nop())
	r.sweep(context.Background())

	assert.Equal(t, []string{"fresh", "old-but-ran"}, disp.placed)
	assert.Contains(t, st.failed["expired"], "unplaceable")
}

func TestPlaceLeavesJobPendingWhenNoWorkerAccepts(t *testing.T) {
	st := newFakeStore()
	st.unplaced = []*store.Job{job("j-1", 0, 5*time.Second)}
	disp := &fakeDispatcher{err: errors.New("all full")}

	r := New(st, disp, &fakeRegistry{}, leader(true), &fakeLog{}, zerolog.Nop())
	r.sweep(context.Background())

	assert.Empty(t, st.failed, "job stays pending for the next sweep")
}
