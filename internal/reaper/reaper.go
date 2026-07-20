// Package reaper runs the scheduler's crash-detection sweep, active only on
// the Raft leader: it reclaims in-flight jobs from workers whose heartbeats
// died. Reclaim re-enqueues each job's dispatch command in the outbox (in one
// transaction), and the reaper wakes the relay to re-place them — the reaper
// itself never dispatches.
package reaper

import (
	"context"
	"time"

	"github.com/rs/zerolog"

	"github.com/sohamghodake/ironwork/internal/store"
)

const tick = 2 * time.Second

// Store is the slice of store.Store the reaper needs.
type Store interface {
	ReclaimJobs(ctx context.Context, workerInstance string) ([]*store.Job, error)
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

// Waker nudges the outbox relay to dispatch the re-enqueued commands now.
type Waker interface {
	Wake()
}

// Reaper is the leader-only crash-detection loop.
type Reaper struct {
	store Store
	reg   Registry
	lead  Leadership
	waker Waker
	log   zerolog.Logger
}

// New builds a reaper.
func New(st Store, reg Registry, lead Leadership, waker Waker, log zerolog.Logger) *Reaper {
	return &Reaper{store: st, reg: reg, lead: lead, waker: waker, log: log}
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

// sweep reclaims jobs from every dead worker and wakes the relay if anything
// was re-enqueued.
func (r *Reaper) sweep(ctx context.Context) {
	reenqueued := false
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
		reenqueued = true
		r.log.Warn().Str("worker", w).Int("jobs", len(jobs)).
			Msg("worker presumed dead; reclaimed jobs re-enqueued for dispatch")
	}
	if reenqueued {
		// ReclaimJobs committed fresh outbox commands; let the relay place
		// them immediately instead of on its next tick.
		r.waker.Wake()
	}
}
