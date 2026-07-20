// Package outbox runs the leader-side dispatch relay: the sole consumer of
// the transactional outbox. It reads committed dispatch commands and hands
// the jobs to workers, giving at-least-once delivery that survives scheduler
// crashes and leader failover. SubmitJob and the crash reaper are the
// producers; nothing dispatches except through here.
package outbox

import (
	"context"
	"fmt"
	"time"

	"github.com/rs/zerolog"

	"github.com/sohamghodake/ironwork/internal/store"
)

const (
	// pollInterval is the fallback drain cadence; producers also wake the
	// relay directly so latency is normally sub-tick.
	pollInterval = 1 * time.Second
	drainBatch   = 50
	// maxDispatchAttempts bounds how many times the relay re-offers a command
	// that no worker will accept before abandoning it.
	maxDispatchAttempts = 30
	// maxExecutionAttempts bounds how many times a job may start running
	// across worker crashes before it is failed.
	maxExecutionAttempts = 3
)

// Store is the slice of store.Store the relay needs.
type Store interface {
	ClaimOutbox(ctx context.Context, limit int) ([]*store.OutboxEntry, error)
	MarkOutboxDispatched(ctx context.Context, id string) error
	RecordOutboxFailure(ctx context.Context, id, errMsg string) error
	MarkOutboxFailed(ctx context.Context, id, errMsg string) error
	MarkFinished(ctx context.Context, id string, succeeded bool, errMsg string) error
}

// Dispatcher places a job on a worker.
type Dispatcher interface {
	Dispatch(ctx context.Context, job *store.Job) (string, error)
}

// Leadership gates the relay to the Raft leader.
type Leadership interface {
	IsLeader() bool
}

// PlacementLog replicates placement decisions to the consensus group.
type PlacementLog interface {
	ApplyPlacement(jobID, worker string) error
}

// Relay drains the outbox on the leader.
type Relay struct {
	store Store
	disp  Dispatcher
	lead  Leadership
	plog  PlacementLog
	wake  chan struct{}
	log   zerolog.Logger
}

// New builds a relay.
func New(st Store, disp Dispatcher, lead Leadership, plog PlacementLog, log zerolog.Logger) *Relay {
	return &Relay{
		store: st,
		disp:  disp,
		lead:  lead,
		plog:  plog,
		wake:  make(chan struct{}, 1),
		log:   log,
	}
}

// Wake nudges the relay to drain now instead of waiting for the next tick.
// Non-blocking: a pending wake already covers this caller.
func (r *Relay) Wake() {
	select {
	case r.wake <- struct{}{}:
	default:
	}
}

// Run drains the outbox on every tick or wake while this scheduler leads,
// until ctx is done.
func (r *Relay) Run(ctx context.Context) {
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-r.wake:
		}
		if r.lead.IsLeader() {
			r.drain(ctx)
		}
	}
}

// drain processes one batch of pending commands.
func (r *Relay) drain(ctx context.Context) {
	entries, err := r.store.ClaimOutbox(ctx, drainBatch)
	if err != nil {
		r.log.Error().Err(err).Msg("claim outbox failed")
		return
	}
	for _, e := range entries {
		r.handle(ctx, e)
	}
}

// handle dispatches, reconciles, or abandons one command.
func (r *Relay) handle(ctx context.Context, e *store.OutboxEntry) {
	// Reconcile: the job already moved past pending (a worker accepted it, or
	// it finished), so the dispatch already happened — close the command out.
	if e.Job.Status != store.StatusPending {
		if err := r.store.MarkOutboxDispatched(ctx, e.ID); err != nil {
			r.log.Error().Err(err).Str("outbox_id", e.ID).Msg("reconcile mark dispatched")
		}
		return
	}

	// Execution budget: a job that has crashed too many times is failed
	// rather than re-placed.
	if e.Job.Attempts >= maxExecutionAttempts {
		r.abandon(ctx, e, fmt.Sprintf("gave up after %d execution attempts", e.Job.Attempts))
		return
	}
	// Dispatch budget: a job no worker will accept is eventually abandoned.
	if e.Attempts >= maxDispatchAttempts {
		r.abandon(ctx, e, fmt.Sprintf("undispatchable after %d attempts", e.Attempts))
		return
	}

	worker, err := r.disp.Dispatch(ctx, e.Job)
	if err != nil {
		// Backpressure or unreachable workers: leave the command pending and
		// record the attempt; the next sweep retries.
		if rerr := r.store.RecordOutboxFailure(ctx, e.ID, err.Error()); rerr != nil {
			r.log.Error().Err(rerr).Str("outbox_id", e.ID).Msg("record outbox failure")
		}
		r.log.Debug().Err(err).Str("job_id", e.Job.ID).Msg("dispatch deferred")
		return
	}

	if err := r.store.MarkOutboxDispatched(ctx, e.ID); err != nil {
		r.log.Error().Err(err).Str("outbox_id", e.ID).Msg("mark dispatched")
	}
	r.log.Info().Str("job_id", e.Job.ID).Str("worker", worker).Msg("job dispatched from outbox")
	if err := r.plog.ApplyPlacement(e.Job.ID, worker); err != nil {
		r.log.Warn().Err(err).Str("job_id", e.Job.ID).Msg("placement log apply failed")
	}
}

// abandon fails both the command and its job.
func (r *Relay) abandon(ctx context.Context, e *store.OutboxEntry, reason string) {
	r.log.Warn().Str("job_id", e.Job.ID).Str("reason", reason).Msg("abandoning dispatch")
	if err := r.store.MarkOutboxFailed(ctx, e.ID, reason); err != nil {
		r.log.Error().Err(err).Str("outbox_id", e.ID).Msg("mark outbox failed")
	}
	if err := r.store.MarkFinished(ctx, e.Job.ID, false, reason); err != nil {
		r.log.Error().Err(err).Str("job_id", e.Job.ID).Msg("mark job failed")
	}
}
