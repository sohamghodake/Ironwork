// Package reaper runs the scheduler's maintenance sweep, active only on the
// Raft leader: reclaim in-flight jobs from workers whose heartbeats died,
// and retry placement of pending jobs that no worker has accepted yet
// (backpressure drain).
package reaper

import (
	"context"
	"fmt"
	"time"

	"github.com/rs/zerolog"

	"github.com/sohamghodake/ironwork/internal/store"
)

const (
	tick = 2 * time.Second
	// retryAfter is how long a pending job sits before the sweep re-offers
	// it (SubmitJob already tried once synchronously).
	retryAfter = 3 * time.Second
	retryBatch = 20
	// maxAttempts bounds execution attempts across crashes.
	maxAttempts = 3
	// placementBudget bounds how long a never-executed job may stay pending.
	placementBudget = 60 * time.Second
)

// Store is the slice of store.Store the reaper needs.
type Store interface {
	ReclaimJobs(ctx context.Context, workerInstance string) ([]*store.Job, error)
	ListUnplaced(ctx context.Context, olderThan time.Duration, limit int) ([]*store.Job, error)
	MarkFinished(ctx context.Context, id string, succeeded bool, errMsg string) error
}

// Dispatcher places a job on a worker.
type Dispatcher interface {
	Dispatch(ctx context.Context, job *store.Job) (string, error)
}

// Registry reports and forgets dead workers.
type Registry interface {
	Dead() []string
	Forget(instance string)
}

// Leadership gates the sweep to the Raft leader.
type Leadership interface {
	IsLeader() bool
}

// PlacementLog replicates placement decisions to the consensus group.
type PlacementLog interface {
	ApplyPlacement(jobID, worker string) error
}

// Reaper is the leader-only maintenance loop.
type Reaper struct {
	store Store
	disp  Dispatcher
	reg   Registry
	lead  Leadership
	plog  PlacementLog
	log   zerolog.Logger
}

// New builds a reaper.
func New(st Store, disp Dispatcher, reg Registry, lead Leadership, plog PlacementLog, log zerolog.Logger) *Reaper {
	return &Reaper{store: st, disp: disp, reg: reg, lead: lead, plog: plog, log: log}
}

// Run sweeps every tick while this scheduler leads, until ctx is done.
func (r *Reaper) Run(ctx context.Context) {
	ticker := time.NewTicker(tick)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !r.lead.IsLeader() {
				continue
			}
			r.sweep(ctx)
		}
	}
}

func (r *Reaper) sweep(ctx context.Context) {
	r.reapDead(ctx)
	r.retryUnplaced(ctx)
}

// reapDead reclaims in-flight jobs from workers whose heartbeats went dead
// and re-places them on live workers.
func (r *Reaper) reapDead(ctx context.Context) {
	for _, w := range r.reg.Dead() {
		jobs, err := r.store.ReclaimJobs(ctx, w)
		if err != nil {
			r.log.Error().Err(err).Str("worker", w).Msg("reclaim failed")
			continue
		}
		r.reg.Forget(w)
		if len(jobs) == 0 {
			r.log.Info().Str("worker", w).Msg("worker presumed dead; nothing to reclaim")
			continue
		}
		r.log.Warn().Str("worker", w).Int("jobs", len(jobs)).Msg("worker presumed dead; reclaiming jobs")
		for _, job := range jobs {
			r.place(ctx, job, "reassigned from "+w)
		}
	}
}

// retryUnplaced re-offers pending jobs that have sat unplaced.
func (r *Reaper) retryUnplaced(ctx context.Context) {
	jobs, err := r.store.ListUnplaced(ctx, retryAfter, retryBatch)
	if err != nil {
		r.log.Error().Err(err).Msg("list unplaced failed")
		return
	}
	for _, job := range jobs {
		r.place(ctx, job, "placement retry")
	}
}

// place re-offers one job, enforcing the attempt and placement budgets.
func (r *Reaper) place(ctx context.Context, job *store.Job, reason string) {
	if job.Attempts >= maxAttempts {
		r.fail(ctx, job, fmt.Sprintf("gave up after %d execution attempts", job.Attempts))
		return
	}
	// The budget applies only to jobs that never started executing; a
	// long-running job reclaimed from a crash must not be expired by age.
	if job.Attempts == 0 && time.Since(job.CreatedAt) > placementBudget {
		r.fail(ctx, job, fmt.Sprintf("unplaceable: no worker accepted within %s", placementBudget))
		return
	}

	worker, err := r.disp.Dispatch(ctx, job)
	if err != nil {
		r.log.Debug().Err(err).Str("job_id", job.ID).Msg("still unplaceable")
		return
	}
	r.log.Info().Str("job_id", job.ID).Str("worker", worker).Str("reason", reason).Msg("job placed by reaper")
	if err := r.plog.ApplyPlacement(job.ID, worker); err != nil {
		r.log.Warn().Err(err).Str("job_id", job.ID).Msg("placement log apply failed")
	}
}

func (r *Reaper) fail(ctx context.Context, job *store.Job, msg string) {
	r.log.Warn().Str("job_id", job.ID).Str("error", msg).Msg("job failed by reaper")
	if err := r.store.MarkFinished(ctx, job.ID, false, msg); err != nil {
		r.log.Error().Err(err).Str("job_id", job.ID).Msg("mark failed")
	}
}
