package dispatch

import (
	"context"
	"errors"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	ironworkv1 "github.com/sohamghodake/ironwork/gen/ironwork/v1"
	"github.com/sohamghodake/ironwork/internal/store"
)

type fakeClient struct {
	name  string
	err   error
	calls int
}

func (f *fakeClient) ExecuteJob(context.Context, *ironworkv1.ExecuteJobRequest, ...grpc.CallOption) (*ironworkv1.ExecuteJobResponse, error) {
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	return &ironworkv1.ExecuteJobResponse{WorkerInstance: f.name}, nil
}

func testDispatcher(clients ...*fakeClient) *Dispatcher {
	d := &Dispatcher{log: zerolog.Nop()}
	for _, c := range clients {
		d.workers = append(d.workers, worker{name: c.name, client: c})
	}
	return d
}

func TestDispatchRoundRobins(t *testing.T) {
	a := &fakeClient{name: "worker-1"}
	b := &fakeClient{name: "worker-2"}
	d := testDispatcher(a, b)
	job := &store.Job{ID: "j"}

	first, err := d.Dispatch(context.Background(), job)
	require.NoError(t, err)
	second, err := d.Dispatch(context.Background(), job)
	require.NoError(t, err)

	assert.NotEqual(t, first, second, "consecutive dispatches should hit different workers")
	assert.Equal(t, 1, a.calls)
	assert.Equal(t, 1, b.calls)
}

func TestDispatchFallsThroughOnError(t *testing.T) {
	bad := &fakeClient{name: "worker-1", err: errors.New("connection refused")}
	good := &fakeClient{name: "worker-2"}
	d := testDispatcher(bad, good)

	got, err := d.Dispatch(context.Background(), &store.Job{ID: "j"})
	require.NoError(t, err)
	assert.Equal(t, "worker-2", got)
}

func TestDispatchAllWorkersFailing(t *testing.T) {
	d := testDispatcher(
		&fakeClient{name: "worker-1", err: errors.New("refused")},
		&fakeClient{name: "worker-2", err: errors.New("refused")},
	)

	_, err := d.Dispatch(context.Background(), &store.Job{ID: "j"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no worker accepted")
}

func TestNewRequiresWorkers(t *testing.T) {
	_, err := New(nil, nil, zerolog.Nop())
	assert.ErrorContains(t, err, "no workers configured")
}
