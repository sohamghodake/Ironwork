package schedulersvc_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	ironworkv1 "github.com/sohamghodake/ironwork/gen/ironwork/v1"
	"github.com/sohamghodake/ironwork/internal/schedulersvc"
	"github.com/sohamghodake/ironwork/internal/store"
)

const testJobID = "3f6f2be8-7c1e-4b3a-9f10-6a4a1c2d3e4f"

type fakeStore struct {
	jobs      map[string]*store.Job
	createErr error
	stats     store.OutboxStats
}

func newFakeStore() *fakeStore { return &fakeStore{jobs: map[string]*store.Job{}} }

func (f *fakeStore) CreateJobWithOutbox(_ context.Context, name string, payload []byte) (*store.Job, error) {
	if f.createErr != nil {
		return nil, f.createErr
	}
	j := &store.Job{ID: testJobID, Name: name, Payload: payload,
		Status: store.StatusPending, CreatedAt: time.Now(), UpdatedAt: time.Now()}
	f.jobs[j.ID] = j
	return j, nil
}

func (f *fakeStore) GetJob(_ context.Context, id string) (*store.Job, error) {
	j, ok := f.jobs[id]
	if !ok {
		return nil, store.ErrNotFound
	}
	return j, nil
}

func (f *fakeStore) ListJobs(_ context.Context, filter string, _ int) ([]*store.Job, error) {
	out := []*store.Job{}
	for _, j := range f.jobs {
		if filter == "" || j.Status == filter {
			out = append(out, j)
		}
	}
	return out, nil
}

func (f *fakeStore) OutboxStats(context.Context) (store.OutboxStats, error) {
	return f.stats, nil
}

type fakeLeadership struct {
	leader bool
	id     string
}

func (f fakeLeadership) IsLeader() bool   { return f.leader }
func (f fakeLeadership) LeaderID() string { return f.id }

type fakeWaker struct{ wakes int }

func (f *fakeWaker) Wake() { f.wakes++ }

func newLeaderSvc(st *fakeStore, waker *fakeWaker) *schedulersvc.Server {
	return schedulersvc.New(st, fakeLeadership{leader: true, id: "scheduler-1"}, waker, zerolog.Nop())
}

func TestSubmitJobEnqueuesAndWakes(t *testing.T) {
	st := newFakeStore()
	waker := &fakeWaker{}
	svc := newLeaderSvc(st, waker)

	resp, err := svc.SubmitJob(context.Background(), &ironworkv1.SubmitJobRequest{
		Name: "sleep", Payload: []byte(`{"duration_ms":100}`),
	})
	require.NoError(t, err)

	// The job is committed as pending; the relay places it asynchronously.
	assert.Equal(t, testJobID, resp.Job.Id)
	assert.Equal(t, ironworkv1.JobStatus_JOB_STATUS_PENDING, resp.Job.Status)
	assert.Empty(t, resp.Job.AssignedWorkerId)
	assert.Equal(t, []byte(`{"duration_ms":100}`), resp.Job.Payload)
	assert.Equal(t, 1, waker.wakes, "submit should wake the relay")
}

func TestSubmitJobCreateErrorIsInternal(t *testing.T) {
	st := newFakeStore()
	st.createErr = errors.New("db down")
	waker := &fakeWaker{}
	svc := newLeaderSvc(st, waker)

	_, err := svc.SubmitJob(context.Background(), &ironworkv1.SubmitJobRequest{Name: "sleep"})
	assert.Equal(t, codes.Internal, status.Code(err))
	assert.Zero(t, waker.wakes, "a failed enqueue must not wake the relay")
}

func TestSubmitJobValidatesName(t *testing.T) {
	svc := newLeaderSvc(newFakeStore(), &fakeWaker{})

	_, err := svc.SubmitJob(context.Background(), &ironworkv1.SubmitJobRequest{})
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestSubmitJobFollowerRejectsFast(t *testing.T) {
	st := newFakeStore()
	waker := &fakeWaker{}
	svc := schedulersvc.New(st, fakeLeadership{leader: false, id: "scheduler-2"}, waker, zerolog.Nop())

	_, err := svc.SubmitJob(context.Background(), &ironworkv1.SubmitJobRequest{Name: "sleep"})

	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	assert.Contains(t, status.Convert(err).Message(), "scheduler-2")
	assert.Empty(t, st.jobs, "followers must not persist jobs")
	assert.Zero(t, waker.wakes)
}

func TestGetJob(t *testing.T) {
	st := newFakeStore()
	svc := newLeaderSvc(st, &fakeWaker{})
	_, err := svc.SubmitJob(context.Background(), &ironworkv1.SubmitJobRequest{Name: "sleep"})
	require.NoError(t, err)

	resp, err := svc.GetJob(context.Background(), &ironworkv1.GetJobRequest{Id: testJobID})
	require.NoError(t, err)
	assert.Equal(t, "sleep", resp.Job.Name)

	_, err = svc.GetJob(context.Background(), &ironworkv1.GetJobRequest{Id: "00000000-0000-0000-0000-000000000000"})
	assert.Equal(t, codes.NotFound, status.Code(err))

	_, err = svc.GetJob(context.Background(), &ironworkv1.GetJobRequest{Id: "not-a-uuid"})
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestListJobsFilters(t *testing.T) {
	st := newFakeStore()
	svc := newLeaderSvc(st, &fakeWaker{})
	_, err := svc.SubmitJob(context.Background(), &ironworkv1.SubmitJobRequest{Name: "sleep"})
	require.NoError(t, err)

	resp, err := svc.ListJobs(context.Background(), &ironworkv1.ListJobsRequest{
		StatusFilter: ironworkv1.JobStatus_JOB_STATUS_PENDING,
	})
	require.NoError(t, err)
	require.Len(t, resp.Jobs, 1)

	resp, err = svc.ListJobs(context.Background(), &ironworkv1.ListJobsRequest{
		StatusFilter: ironworkv1.JobStatus_JOB_STATUS_RUNNING,
	})
	require.NoError(t, err)
	assert.Empty(t, resp.Jobs)
}

func TestGetOutboxStats(t *testing.T) {
	st := newFakeStore()
	st.stats = store.OutboxStats{Pending: 3, Dispatched: 10, Failed: 1, OldestPending: 2 * time.Second}
	svc := newLeaderSvc(st, &fakeWaker{})

	resp, err := svc.GetOutboxStats(context.Background(), &ironworkv1.GetOutboxStatsRequest{})
	require.NoError(t, err)
	assert.Equal(t, uint64(3), resp.Pending)
	assert.Equal(t, uint64(10), resp.Dispatched)
	assert.Equal(t, uint64(1), resp.Failed)
	assert.InDelta(t, 2.0, resp.OldestPendingSeconds, 0.01)
}
