package workersvc_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	ironworkv1 "github.com/sohamghodake/ironwork/gen/ironwork/v1"
	"github.com/sohamghodake/ironwork/internal/store"
	"github.com/sohamghodake/ironwork/internal/workersvc"
)

// fakeStore records transitions and signals when a job reaches terminal state.
type fakeStore struct {
	mu          sync.Mutex
	scheduled   []string
	running     []string
	finished    map[string]string // job id -> error message ("" = success)
	notFound    bool
	terminalped chan string
}

func newFakeStore() *fakeStore {
	return &fakeStore{finished: map[string]string{}, terminalped: make(chan string, 16)}
}

func (f *fakeStore) MarkScheduled(_ context.Context, id, _ string) error {
	if f.notFound {
		return store.ErrNotFound
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.scheduled = append(f.scheduled, id)
	return nil
}

func (f *fakeStore) MarkRunning(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.running = append(f.running, id)
	return nil
}

func (f *fakeStore) MarkFinished(_ context.Context, id string, succeeded bool, errMsg string) error {
	f.mu.Lock()
	if succeeded {
		f.finished[id] = ""
	} else {
		f.finished[id] = errMsg
	}
	f.mu.Unlock()
	f.terminalped <- id
	return nil
}

func (f *fakeStore) waitTerminal(t *testing.T, id string) string {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		select {
		case got := <-f.terminalped:
			if got == id {
				f.mu.Lock()
				defer f.mu.Unlock()
				return f.finished[id]
			}
		case <-deadline:
			t.Fatalf("job %s never reached a terminal state", id)
		}
	}
}

func TestExecuteJobSucceeds(t *testing.T) {
	fs := newFakeStore()
	svc := workersvc.New("worker-1", fs, 2, nil, zerolog.Nop())

	resp, err := svc.ExecuteJob(context.Background(), &ironworkv1.ExecuteJobRequest{
		JobId: "job-1", Name: "sleep", Payload: []byte(`{"duration_ms":10}`),
	})
	require.NoError(t, err)
	assert.Equal(t, "worker-1", resp.WorkerInstance)

	assert.Empty(t, fs.waitTerminal(t, "job-1"))
	assert.Equal(t, []string{"job-1"}, fs.scheduled)
	assert.Equal(t, []string{"job-1"}, fs.running)
}

func TestExecuteJobHonorsFailFlag(t *testing.T) {
	fs := newFakeStore()
	svc := workersvc.New("worker-1", fs, 2, nil, zerolog.Nop())

	_, err := svc.ExecuteJob(context.Background(), &ironworkv1.ExecuteJobRequest{
		JobId: "job-2", Payload: []byte(`{"duration_ms":10,"fail":true}`),
	})
	require.NoError(t, err) // dispatch is accepted; failure is an execution outcome

	assert.Contains(t, fs.waitTerminal(t, "job-2"), "requested failure")
}

func TestExecuteJobInvalidPayloadFailsJob(t *testing.T) {
	fs := newFakeStore()
	svc := workersvc.New("worker-1", fs, 2, nil, zerolog.Nop())

	_, err := svc.ExecuteJob(context.Background(), &ironworkv1.ExecuteJobRequest{
		JobId: "job-3", Payload: []byte(`not json`),
	})
	require.NoError(t, err)

	assert.Contains(t, fs.waitTerminal(t, "job-3"), "invalid payload")
}

func TestExecuteJobUnknownJobIsNotFound(t *testing.T) {
	fs := newFakeStore()
	fs.notFound = true
	svc := workersvc.New("worker-1", fs, 2, nil, zerolog.Nop())

	_, err := svc.ExecuteJob(context.Background(), &ironworkv1.ExecuteJobRequest{JobId: "ghost"})
	assert.Equal(t, codes.NotFound, status.Code(err))
}

func TestExecuteJobRequiresJobID(t *testing.T) {
	svc := workersvc.New("worker-1", newFakeStore(), 2, nil, zerolog.Nop())

	_, err := svc.ExecuteJob(context.Background(), &ironworkv1.ExecuteJobRequest{})
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

type fakeReporter struct {
	mu      sync.Mutex
	reports []string
}

func (f *fakeReporter) ReportJobEvent(_ context.Context, jobID, worker string, succeeded bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	outcome := "succeeded"
	if !succeeded {
		outcome = "failed"
	}
	f.reports = append(f.reports, jobID+":"+outcome+"@"+worker)
}

func TestTerminalOutcomesAreReported(t *testing.T) {
	fs := newFakeStore()
	rep := &fakeReporter{}
	svc := workersvc.New("worker-1", fs, 2, rep, zerolog.Nop())

	_, err := svc.ExecuteJob(context.Background(), &ironworkv1.ExecuteJobRequest{
		JobId: "ok-job", Payload: []byte(`{"duration_ms":10}`),
	})
	require.NoError(t, err)
	_, err = svc.ExecuteJob(context.Background(), &ironworkv1.ExecuteJobRequest{
		JobId: "bad-job", Payload: []byte(`{"duration_ms":10,"fail":true}`),
	})
	require.NoError(t, err)

	fs.waitTerminal(t, "ok-job")
	fs.waitTerminal(t, "bad-job")
	svc.Drain(5 * time.Second)

	rep.mu.Lock()
	defer rep.mu.Unlock()
	assert.ElementsMatch(t, []string{"ok-job:succeeded@worker-1", "bad-job:failed@worker-1"}, rep.reports)
}

func TestCapacityRejectsWithResourceExhausted(t *testing.T) {
	fs := newFakeStore()
	svc := workersvc.New("worker-1", fs, 1, nil, zerolog.Nop()) // single slot

	_, err := svc.ExecuteJob(context.Background(), &ironworkv1.ExecuteJobRequest{
		JobId: "q-1", Payload: []byte(`{"duration_ms":200}`),
	})
	require.NoError(t, err)
	assert.Equal(t, 1, svc.Inflight())

	// Second job while the first still runs: backpressure kicks in.
	_, err = svc.ExecuteJob(context.Background(), &ironworkv1.ExecuteJobRequest{
		JobId: "q-2", Payload: []byte(`{"duration_ms":200}`),
	})
	assert.Equal(t, codes.ResourceExhausted, status.Code(err))
	assert.Equal(t, []string{"q-1"}, fs.scheduled, "rejected jobs must not be marked scheduled")

	// The slot frees once the first job finishes.
	assert.Empty(t, fs.waitTerminal(t, "q-1"))
	svc.Drain(5 * time.Second)
	assert.Zero(t, svc.Inflight())

	_, err = svc.ExecuteJob(context.Background(), &ironworkv1.ExecuteJobRequest{
		JobId: "q-3", Payload: []byte(`{"duration_ms":10}`),
	})
	require.NoError(t, err)
	assert.Empty(t, fs.waitTerminal(t, "q-3"))
}
