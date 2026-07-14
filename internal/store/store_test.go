package store_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pressly/goose/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/sohamghodake/ironwork/internal/store"
	"github.com/sohamghodake/ironwork/migrations"
)

// newTestStore boots a migrated Postgres and returns a Store over it.
func newTestStore(t *testing.T) *store.Store {
	t.Helper()
	if testing.Short() {
		t.Skip("requires docker")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	t.Cleanup(cancel)

	pgc, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("ironwork"),
		tcpostgres.WithUsername("ironwork"),
		tcpostgres.WithPassword("ironwork"),
		tcpostgres.BasicWaitStrategies(),
	)
	testcontainers.CleanupContainer(t, pgc)
	require.NoError(t, err)

	dsn, err := pgc.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	db, err := sql.Open("pgx", dsn)
	require.NoError(t, err)
	goose.SetBaseFS(migrations.FS)
	require.NoError(t, goose.SetDialect("postgres"))
	require.NoError(t, goose.Up(db, "."))
	require.NoError(t, db.Close())

	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	return store.New(pool)
}

func TestJobLifecycle(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	job, err := s.CreateJob(ctx, "sleepy", []byte(`{"duration_ms":100}`))
	require.NoError(t, err)
	assert.NotEmpty(t, job.ID)
	assert.Equal(t, store.StatusPending, job.Status)
	assert.Zero(t, job.Attempts)
	assert.Nil(t, job.StartedAt)

	require.NoError(t, s.MarkScheduled(ctx, job.ID, "worker-1"))
	require.NoError(t, s.MarkRunning(ctx, job.ID))
	require.NoError(t, s.MarkFinished(ctx, job.ID, true, ""))

	got, err := s.GetJob(ctx, job.ID)
	require.NoError(t, err)
	assert.Equal(t, store.StatusSucceeded, got.Status)
	assert.Equal(t, "worker-1", got.AssignedWorker)
	assert.Equal(t, 1, got.Attempts)
	assert.Empty(t, got.Error)
	require.NotNil(t, got.StartedAt)
	require.NotNil(t, got.FinishedAt)
	assert.False(t, got.FinishedAt.Before(*got.StartedAt))
	assert.Equal(t, []byte(`{"duration_ms":100}`), got.Payload)
}

func TestMarkFinishedFailureStoresError(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	job, err := s.CreateJob(ctx, "doomed", nil)
	require.NoError(t, err)
	require.NoError(t, s.MarkFinished(ctx, job.ID, false, "job requested failure"))

	got, err := s.GetJob(ctx, job.ID)
	require.NoError(t, err)
	assert.Equal(t, store.StatusFailed, got.Status)
	assert.Equal(t, "job requested failure", got.Error)
}

func TestGetJobNotFound(t *testing.T) {
	s := newTestStore(t)

	_, err := s.GetJob(context.Background(), "00000000-0000-0000-0000-000000000000")
	assert.ErrorIs(t, err, store.ErrNotFound)

	err = s.MarkScheduled(context.Background(), "00000000-0000-0000-0000-000000000000", "worker-1")
	assert.ErrorIs(t, err, store.ErrNotFound)
}

func TestListJobsFilterAndOrder(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	first, err := s.CreateJob(ctx, "a", nil)
	require.NoError(t, err)
	second, err := s.CreateJob(ctx, "b", nil)
	require.NoError(t, err)
	require.NoError(t, s.MarkScheduled(ctx, first.ID, "worker-1"))

	all, err := s.ListJobs(ctx, "", 10)
	require.NoError(t, err)
	require.Len(t, all, 2)

	pending, err := s.ListJobs(ctx, store.StatusPending, 10)
	require.NoError(t, err)
	require.Len(t, pending, 1)
	assert.Equal(t, second.ID, pending[0].ID)

	limited, err := s.ListJobs(ctx, "", 1)
	require.NoError(t, err)
	assert.Len(t, limited, 1)
}
