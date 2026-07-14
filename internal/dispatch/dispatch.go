// Package dispatch fans job executions out to workers over mTLS gRPC. Phase 1
// placement is deliberately dumb: round-robin over a static worker list with
// fall-through to the next worker on error. The scheduler service replaces
// this in Phase 2.
package dispatch

import (
	"context"
	"crypto/tls"
	"fmt"
	"sort"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	ironworkv1 "github.com/sohamghodake/ironwork/gen/ironwork/v1"
	"github.com/sohamghodake/ironwork/internal/store"
)

// perAttemptTimeout bounds one ExecuteJob ack; the worker executes async, so
// a healthy worker answers almost immediately.
const perAttemptTimeout = 3 * time.Second

// executeClient is the slice of WorkerServiceClient dispatch needs.
type executeClient interface {
	ExecuteJob(ctx context.Context, in *ironworkv1.ExecuteJobRequest, opts ...grpc.CallOption) (*ironworkv1.ExecuteJobResponse, error)
}

type worker struct {
	name   string
	client executeClient
	close  func() error
}

// Dispatcher places jobs on workers round-robin.
type Dispatcher struct {
	workers []worker
	next    atomic.Uint64
	log     zerolog.Logger
}

// New builds a dispatcher over the configured workers (instance name -> gRPC
// address), connecting lazily with the given client mTLS config.
func New(workers map[string]string, tlsCfg *tls.Config, log zerolog.Logger) (*Dispatcher, error) {
	if len(workers) == 0 {
		return nil, fmt.Errorf("dispatch: no workers configured (IRONWORK_WORKERS)")
	}

	names := make([]string, 0, len(workers))
	for name := range workers {
		names = append(names, name)
	}
	sort.Strings(names)

	creds := credentials.NewTLS(tlsCfg)
	d := &Dispatcher{log: log}
	for _, name := range names {
		conn, err := grpc.NewClient(workers[name], grpc.WithTransportCredentials(creds))
		if err != nil {
			d.Close()
			return nil, fmt.Errorf("dispatch: client for %s: %w", name, err)
		}
		d.workers = append(d.workers, worker{
			name:   name,
			client: ironworkv1.NewWorkerServiceClient(conn),
			close:  conn.Close,
		})
	}
	return d, nil
}

// Dispatch offers the job to workers starting at the round-robin cursor and
// returns the instance name of the first worker that accepts.
func (d *Dispatcher) Dispatch(ctx context.Context, job *store.Job) (string, error) {
	start := d.next.Add(1) - 1
	var lastErr error
	for i := range d.workers {
		w := d.workers[(start+uint64(i))%uint64(len(d.workers))]

		actx, cancel := context.WithTimeout(ctx, perAttemptTimeout)
		resp, err := w.client.ExecuteJob(actx, &ironworkv1.ExecuteJobRequest{
			JobId:   job.ID,
			Name:    job.Name,
			Payload: job.Payload,
		})
		cancel()
		if err == nil {
			return resp.WorkerInstance, nil
		}
		lastErr = err
		d.log.Warn().Err(err).Str("worker", w.name).Str("job_id", job.ID).Msg("worker rejected dispatch")
	}
	return "", fmt.Errorf("dispatch: no worker accepted job %s: %w", job.ID, lastErr)
}

// Close tears down all worker connections.
func (d *Dispatcher) Close() {
	for _, w := range d.workers {
		if w.close != nil {
			_ = w.close()
		}
	}
}
