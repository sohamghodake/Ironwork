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

// CandidateProvider names the workers currently eligible for placement, best
// candidate first (Phase 4: the heartbeat registry — live and non-full,
// most headroom first). A nil provider means every configured worker is
// eligible, tried round-robin.
type CandidateProvider func() []string

// Dispatcher places jobs on workers.
type Dispatcher struct {
	workers    []worker
	byName     map[string]worker
	candidates CandidateProvider
	next       atomic.Uint64
	log        zerolog.Logger
}

// New builds a dispatcher over the configured workers (instance name -> gRPC
// address) with the given client mTLS config.
func New(workers map[string]string, tlsCfg *tls.Config, candidates CandidateProvider, log zerolog.Logger) (*Dispatcher, error) {
	if len(workers) == 0 {
		return nil, fmt.Errorf("dispatch: no workers configured (IRONWORK_WORKERS)")
	}

	names := make([]string, 0, len(workers))
	for name := range workers {
		names = append(names, name)
	}
	sort.Strings(names)

	creds := credentials.NewTLS(tlsCfg)
	d := &Dispatcher{byName: map[string]worker{}, candidates: candidates, log: log}
	for _, name := range names {
		conn, err := grpc.NewClient(workers[name], grpc.WithTransportCredentials(creds))
		if err != nil {
			d.Close()
			return nil, fmt.Errorf("dispatch: client for %s: %w", name, err)
		}
		// Dial eagerly: otherwise the first dispatch after boot races the
		// connection handshake and can burn its whole attempt timeout.
		conn.Connect()
		w := worker{
			name:   name,
			client: ironworkv1.NewWorkerServiceClient(conn),
			close:  conn.Close,
		}
		d.workers = append(d.workers, w)
		d.byName[name] = w
	}
	return d, nil
}

// eligible returns the workers to offer the job to, in attempt order.
func (d *Dispatcher) eligible() []worker {
	if d.candidates == nil {
		start := d.next.Add(1) - 1
		out := make([]worker, len(d.workers))
		for i := range d.workers {
			out[i] = d.workers[(start+uint64(i))%uint64(len(d.workers))]
		}
		return out
	}
	out := []worker{}
	for _, name := range d.candidates() {
		if w, ok := d.byName[name]; ok {
			out = append(out, w)
		}
	}
	return out
}

// Dispatch offers the job to eligible workers in order and returns the
// instance name of the first that accepts. Rejections (including
// ResourceExhausted backpressure) fall through to the next candidate.
func (d *Dispatcher) Dispatch(ctx context.Context, job *store.Job) (string, error) {
	workers := d.eligible()
	if len(workers) == 0 {
		return "", fmt.Errorf("dispatch: no available workers for job %s", job.ID)
	}

	var lastErr error
	for _, w := range workers {
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
