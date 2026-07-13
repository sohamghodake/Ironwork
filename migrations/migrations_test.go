package migrations_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/sohamghodake/ironwork/migrations"
)

// TestMigrationsApply spins up a real Postgres (testcontainers) and verifies
// the embedded migrations produce the partitioned jobs table.
func TestMigrationsApply(t *testing.T) {
	if testing.Short() {
		t.Skip("requires docker")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

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
	defer db.Close()

	goose.SetBaseFS(migrations.FS)
	require.NoError(t, goose.SetDialect("postgres"))
	require.NoError(t, goose.Up(db, "."))

	// jobs is a partitioned table (relkind "p").
	var relkind string
	require.NoError(t, db.QueryRow(`SELECT relkind FROM pg_class WHERE relname = 'jobs'`).Scan(&relkind))
	assert.Equal(t, "p", relkind)

	// Default partition + 4 monthly partitions attached.
	var partitions int
	require.NoError(t, db.QueryRow(
		`SELECT count(*) FROM pg_inherits WHERE inhparent = 'jobs'::regclass`,
	).Scan(&partitions))
	assert.Equal(t, 5, partitions)

	// An insert routes into a partition and round-trips.
	var id string
	require.NoError(t, db.QueryRow(
		`INSERT INTO jobs (name, payload) VALUES ('smoke', 'hi') RETURNING id`,
	).Scan(&id))
	assert.NotEmpty(t, id)

	var status string
	require.NoError(t, db.QueryRow(`SELECT status FROM jobs WHERE id = $1`, id).Scan(&status))
	assert.Equal(t, "pending", status)

	// The status CHECK constraint rejects unknown states.
	_, err = db.Exec(`INSERT INTO jobs (name, status) VALUES ('bad', 'exploded')`)
	assert.Error(t, err)

	// Down migration tears everything back out.
	require.NoError(t, goose.DownTo(db, ".", 0))
	var exists bool
	require.NoError(t, db.QueryRow(`SELECT to_regclass('jobs') IS NOT NULL`).Scan(&exists))
	assert.False(t, exists)
}
