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
}

func newFakeStore() *fakeStore { return &fakeStore{jobs: map[string]*store.Job{}} }

func (f *fakeStore) CreateJob(_ context.Context, name string, payload []byte) (*store.Job, error) {
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

func (f *fakeStore) MarkFinished(_ context.Context, id string, succeeded bool, errMsg string) error {
	j, ok := f.jobs[id]
	if !ok {
		return store.ErrNotFound
	}
	j.Status = store.StatusFailed
	if succeeded {
		j.Status = store.StatusSucceeded
	}
	j.Error = errMsg
	return nil
}

// fakeDispatcher mimics a worker recording acceptance before acking.
type fakeDispatcher struct {
	st  *fakeStore
	err error
}

func (f *fakeDispatcher) Dispatch(_ context.Context, job *store.Job) (string, error) {
	if f.err != nil {
		return "", f.err
	}
	if j, ok := f.st.jobs[job.ID]; ok {
		j.Status = store.StatusScheduled
		j.AssignedWorker = "worker-1"
	}
	return "worker-1", nil
}

type fakeLeadership struct {
	leader bool
	id     string
}

func (f fakeLeadership) IsLeader() bool   { return f.leader }
func (f fakeLeadership) LeaderID() string { return f.id }

type fakePlacementLog struct {
	applied []string
	err     error
}

func (f *fakePlacementLog) ApplyPlacement(jobID, worker string) error {
	f.applied = append(f.applied, jobID+"@"+worker)
	return f.err
}

func newLeaderSvc(st *fakeStore, disp *fakeDispatcher, plog *fakePlacementLog) *schedulersvc.Server {
	return schedulersvc.New(st, disp, fakeLeadership{leader: true, id: "scheduler-1"}, plog, zerolog.Nop())
}

func TestSubmitJobPlaces(t *testing.T) {
	st := newFakeStore()
	svc := newLeaderSvc(st, &fakeDispatcher{st: st}, &fakePlacementLog{})

	resp, err := svc.SubmitJob(context.Background(), &ironworkv1.SubmitJobRequest{
		Name: "sleep", Payload: []byte(`{"duration_ms":100}`),
	})
	require.NoError(t, err)

	assert.Equal(t, testJobID, resp.Job.Id)
	assert.Equal(t, ironworkv1.JobStatus_JOB_STATUS_SCHEDULED, resp.Job.Status)
	assert.Equal(t, "worker-1", resp.Job.AssignedWorkerId)
	assert.Equal(t, []byte(`{"duration_ms":100}`), resp.Job.Payload)
}

func TestSubmitJobPlacementFailureMarksFailed(t *testing.T) {
	st := newFakeStore()
	svc := newLeaderSvc(st, &fakeDispatcher{st: st, err: errors.New("all workers down")}, &fakePlacementLog{})

	resp, err := svc.SubmitJob(context.Background(), &ironworkv1.SubmitJobRequest{Name: "sleep"})
	require.NoError(t, err)

	assert.Equal(t, ironworkv1.JobStatus_JOB_STATUS_FAILED, resp.Job.Status)
	assert.Contains(t, resp.Job.Error, "all workers down")
}

func TestSubmitJobValidatesName(t *testing.T) {
	svc := newLeaderSvc(newFakeStore(), &fakeDispatcher{}, &fakePlacementLog{})

	_, err := svc.SubmitJob(context.Background(), &ironworkv1.SubmitJobRequest{})
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestGetJob(t *testing.T) {
	st := newFakeStore()
	svc := newLeaderSvc(st, &fakeDispatcher{st: st}, &fakePlacementLog{})
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

func TestSubmitJobFollowerRejectsFast(t *testing.T) {
	st := newFakeStore()
	svc := schedulersvc.New(st, &fakeDispatcher{st: st},
		fakeLeadership{leader: false, id: "scheduler-2"}, &fakePlacementLog{}, zerolog.Nop())

	_, err := svc.SubmitJob(context.Background(), &ironworkv1.SubmitJobRequest{Name: "sleep"})

	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	assert.Contains(t, status.Convert(err).Message(), "scheduler-2")
	assert.Empty(t, st.jobs, "followers must not persist jobs")
}

func TestSubmitJobReplicatesPlacement(t *testing.T) {
	st := newFakeStore()
	plog := &fakePlacementLog{}
	svc := newLeaderSvc(st, &fakeDispatcher{st: st}, plog)

	_, err := svc.SubmitJob(context.Background(), &ironworkv1.SubmitJobRequest{Name: "sleep"})
	require.NoError(t, err)

	assert.Equal(t, []string{testJobID + "@worker-1"}, plog.applied)
}

func TestSubmitJobApplyFailureDoesNotFailSubmit(t *testing.T) {
	st := newFakeStore()
	plog := &fakePlacementLog{err: errors.New("leadership lost")}
	svc := newLeaderSvc(st, &fakeDispatcher{st: st}, plog)

	resp, err := svc.SubmitJob(context.Background(), &ironworkv1.SubmitJobRequest{Name: "sleep"})
	require.NoError(t, err)
	assert.Equal(t, ironworkv1.JobStatus_JOB_STATUS_SCHEDULED, resp.Job.Status)
}

func TestSubmitJobNoPlacementLogOnDispatchFailure(t *testing.T) {
	st := newFakeStore()
	plog := &fakePlacementLog{}
	svc := newLeaderSvc(st, &fakeDispatcher{st: st, err: errors.New("workers down")}, plog)

	_, err := svc.SubmitJob(context.Background(), &ironworkv1.SubmitJobRequest{Name: "sleep"})
	require.NoError(t, err)
	assert.Empty(t, plog.applied, "failed placements must not enter the log")
}

func TestListJobsFilters(t *testing.T) {
	st := newFakeStore()
	svc := newLeaderSvc(st, &fakeDispatcher{st: st}, &fakePlacementLog{})
	_, err := svc.SubmitJob(context.Background(), &ironworkv1.SubmitJobRequest{Name: "sleep"})
	require.NoError(t, err)

	resp, err := svc.ListJobs(context.Background(), &ironworkv1.ListJobsRequest{
		StatusFilter: ironworkv1.JobStatus_JOB_STATUS_SCHEDULED,
	})
	require.NoError(t, err)
	require.Len(t, resp.Jobs, 1)

	resp, err = svc.ListJobs(context.Background(), &ironworkv1.ListJobsRequest{
		StatusFilter: ironworkv1.JobStatus_JOB_STATUS_RUNNING,
	})
	require.NoError(t, err)
	assert.Empty(t, resp.Jobs)
}
