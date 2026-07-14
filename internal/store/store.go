// Package store is the jobs repository over Postgres (pgx). The gateway
// creates and reads jobs; the worker owns every status transition after
// acceptance (scheduled -> running -> succeeded/failed).
package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrNotFound is returned when no job matches the given id.
var ErrNotFound = errors.New("job not found")

// Job statuses as stored in the jobs.status column.
const (
	StatusPending   = "pending"
	StatusScheduled = "scheduled"
	StatusRunning   = "running"
	StatusSucceeded = "succeeded"
	StatusFailed    = "failed"
)

// Job mirrors one row of the jobs table.
type Job struct {
	ID             string     `json:"id"`
	Name           string     `json:"name"`
	Payload        []byte     `json:"-"`
	Status         string     `json:"status"`
	AssignedWorker string     `json:"assigned_worker,omitempty"`
	Attempts       int        `json:"attempts"`
	Error          string     `json:"error,omitempty"`
	CreatedAt      time.Time  `json:"created_at"`
	UpdatedAt      time.Time  `json:"updated_at"`
	StartedAt      *time.Time `json:"started_at,omitempty"`
	FinishedAt     *time.Time `json:"finished_at,omitempty"`
}

// Store executes job queries on a pgx pool.
type Store struct {
	pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

const jobColumns = `id, name, payload, status, COALESCE(assigned_worker_id, ''),
	attempts, COALESCE(error, ''), created_at, updated_at, started_at, finished_at`

func scanJob(row pgx.Row) (*Job, error) {
	var j Job
	err := row.Scan(&j.ID, &j.Name, &j.Payload, &j.Status, &j.AssignedWorker,
		&j.Attempts, &j.Error, &j.CreatedAt, &j.UpdatedAt, &j.StartedAt, &j.FinishedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("store: scan job: %w", err)
	}
	return &j, nil
}

// CreateJob inserts a new pending job and returns the stored row.
func (s *Store) CreateJob(ctx context.Context, name string, payload []byte) (*Job, error) {
	row := s.pool.QueryRow(ctx,
		`INSERT INTO jobs (name, payload) VALUES ($1, $2) RETURNING `+jobColumns,
		name, payload)
	return scanJob(row)
}

// GetJob fetches one job by id (must be a valid UUID).
func (s *Store) GetJob(ctx context.Context, id string) (*Job, error) {
	row := s.pool.QueryRow(ctx,
		`SELECT `+jobColumns+` FROM jobs WHERE id = $1::uuid`, id)
	return scanJob(row)
}

// ListJobs returns the newest jobs, optionally filtered by status.
func (s *Store) ListJobs(ctx context.Context, status string, limit int) ([]*Job, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT `+jobColumns+` FROM jobs
		 WHERE ($1 = '' OR status = $1)
		 ORDER BY created_at DESC LIMIT $2`, status, limit)
	if err != nil {
		return nil, fmt.Errorf("store: list jobs: %w", err)
	}
	defer rows.Close()

	jobs := []*Job{}
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, j)
	}
	return jobs, rows.Err()
}

// MarkScheduled records acceptance by a worker.
func (s *Store) MarkScheduled(ctx context.Context, id, workerInstance string) error {
	return s.exec(ctx,
		`UPDATE jobs SET status = $2, assigned_worker_id = $3, updated_at = now()
		 WHERE id = $1::uuid`, id, StatusScheduled, workerInstance)
}

// MarkRunning records the start of an execution attempt.
func (s *Store) MarkRunning(ctx context.Context, id string) error {
	return s.exec(ctx,
		`UPDATE jobs SET status = $2, started_at = now(), attempts = attempts + 1,
		 updated_at = now() WHERE id = $1::uuid`, id, StatusRunning)
}

// MarkFinished records the terminal state of an execution. errMsg is stored
// only when non-empty.
func (s *Store) MarkFinished(ctx context.Context, id string, succeeded bool, errMsg string) error {
	status := StatusSucceeded
	if !succeeded {
		status = StatusFailed
	}
	return s.exec(ctx,
		`UPDATE jobs SET status = $2, error = NULLIF($3, ''), finished_at = now(),
		 updated_at = now() WHERE id = $1::uuid`, id, status, errMsg)
}

func (s *Store) exec(ctx context.Context, sql string, args ...any) error {
	tag, err := s.pool.Exec(ctx, sql, args...)
	if err != nil {
		return fmt.Errorf("store: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}
