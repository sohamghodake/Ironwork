package store

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// Outbox row statuses.
const (
	OutboxPending    = "pending"
	OutboxDispatched = "dispatched"
	OutboxFailed     = "failed"
)

// OutboxEntry is one pending dispatch command joined with the job it places.
type OutboxEntry struct {
	ID       string
	Attempts int
	Job      *Job
}

// OutboxStats summarizes the dispatch backlog.
type OutboxStats struct {
	Pending       uint64
	Dispatched    uint64
	Failed        uint64
	OldestPending time.Duration
}

// enqueueOutbox inserts a pending dispatch command for jobID. Runs inside the
// caller's transaction so the command commits atomically with the job change.
func enqueueOutbox(ctx context.Context, tx pgx.Tx, jobID string) error {
	_, err := tx.Exec(ctx, `INSERT INTO outbox (job_id) VALUES ($1::uuid)`, jobID)
	if err != nil {
		return fmt.Errorf("store: enqueue outbox: %w", err)
	}
	return nil
}

// CreateJobWithOutbox inserts a pending job and its dispatch command in one
// transaction — the transactional-outbox write. Either both land or neither
// does, so a committed job always has a dispatch command waiting for it.
func (s *Store) CreateJobWithOutbox(ctx context.Context, name string, payload []byte) (*Job, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("store: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	job, err := scanJob(tx.QueryRow(ctx,
		`INSERT INTO jobs (name, payload) VALUES ($1, $2) RETURNING `+jobColumns, name, payload))
	if err != nil {
		return nil, err
	}
	if err := enqueueOutbox(ctx, tx, job.ID); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("store: commit: %w", err)
	}
	return job, nil
}

// ClaimOutbox returns up to limit pending dispatch commands, oldest first,
// each joined with its job. The relay decides per entry whether to dispatch,
// reconcile, or fail based on the job's current status and attempts.
func (s *Store) ClaimOutbox(ctx context.Context, limit int) ([]*OutboxEntry, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT o.id, o.attempts,
		        j.id, j.name, j.payload, j.status, COALESCE(j.assigned_worker_id, ''),
		        j.attempts, COALESCE(j.error, ''), j.created_at, j.updated_at, j.started_at, j.finished_at
		 FROM outbox o JOIN jobs j ON j.id = o.job_id
		 WHERE o.status = $1
		 ORDER BY o.created_at ASC LIMIT $2`,
		OutboxPending, limit)
	if err != nil {
		return nil, fmt.Errorf("store: claim outbox: %w", err)
	}
	defer rows.Close()

	entries := []*OutboxEntry{}
	for rows.Next() {
		var e OutboxEntry
		var j Job
		if err := rows.Scan(&e.ID, &e.Attempts,
			&j.ID, &j.Name, &j.Payload, &j.Status, &j.AssignedWorker,
			&j.Attempts, &j.Error, &j.CreatedAt, &j.UpdatedAt, &j.StartedAt, &j.FinishedAt); err != nil {
			return nil, fmt.Errorf("store: scan outbox entry: %w", err)
		}
		e.Job = &j
		entries = append(entries, &e)
	}
	return entries, rows.Err()
}

// MarkOutboxDispatched records that the command was relayed to a worker.
func (s *Store) MarkOutboxDispatched(ctx context.Context, id string) error {
	return s.exec(ctx,
		`UPDATE outbox SET status = $2, dispatched_at = now() WHERE id = $1::uuid`,
		id, OutboxDispatched)
}

// RecordOutboxFailure bumps the attempt count and records the error, leaving
// the command pending for the next relay sweep.
func (s *Store) RecordOutboxFailure(ctx context.Context, id, errMsg string) error {
	return s.exec(ctx,
		`UPDATE outbox SET attempts = attempts + 1, last_error = $2 WHERE id = $1::uuid`,
		id, errMsg)
}

// MarkOutboxFailed abandons the command after its budgets are exhausted.
func (s *Store) MarkOutboxFailed(ctx context.Context, id, errMsg string) error {
	return s.exec(ctx,
		`UPDATE outbox SET status = $2, last_error = $3 WHERE id = $1::uuid`,
		id, OutboxFailed, errMsg)
}

// OutboxStats returns the current dispatch backlog.
func (s *Store) OutboxStats(ctx context.Context) (OutboxStats, error) {
	var st OutboxStats
	var oldestSeconds *float64
	err := s.pool.QueryRow(ctx,
		`SELECT
		   count(*) FILTER (WHERE status = 'pending'),
		   count(*) FILTER (WHERE status = 'dispatched'),
		   count(*) FILTER (WHERE status = 'failed'),
		   extract(epoch FROM now() - min(created_at) FILTER (WHERE status = 'pending'))
		 FROM outbox`).Scan(&st.Pending, &st.Dispatched, &st.Failed, &oldestSeconds)
	if err != nil {
		return OutboxStats{}, fmt.Errorf("store: outbox stats: %w", err)
	}
	if oldestSeconds != nil {
		st.OldestPending = time.Duration(*oldestSeconds * float64(time.Second))
	}
	return st, nil
}
