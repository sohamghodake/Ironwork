// Command migrate applies the embedded goose migrations to Postgres and
// exits. It runs as a one-shot compose service before the cluster starts.
package main

import (
	"database/sql"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"

	"github.com/sohamghodake/ironwork/internal/config"
	"github.com/sohamghodake/ironwork/internal/logging"
	"github.com/sohamghodake/ironwork/migrations"
)

const dbWaitTimeout = 60 * time.Second

func main() {
	cfg, err := config.Load("migrate")
	if err != nil {
		fallback := logging.New("migrate", "migrate", "info")
		fallback.Fatal().Err(err).Msg("load config")
	}
	log := logging.New(cfg.Component, cfg.Instance, cfg.LogLevel)

	db, err := sql.Open("pgx", cfg.DBDSN)
	if err != nil {
		log.Fatal().Err(err).Msg("open database")
	}
	defer db.Close()

	// The compose healthcheck gates on pg_isready, but the server can still
	// refuse connections briefly after that; poll until reachable.
	deadline := time.Now().Add(dbWaitTimeout)
	for {
		if err = db.Ping(); err == nil {
			break
		}
		if time.Now().After(deadline) {
			log.Fatal().Err(err).Dur("waited", dbWaitTimeout).Msg("postgres unreachable")
		}
		log.Info().Err(err).Msg("waiting for postgres")
		time.Sleep(2 * time.Second)
	}

	goose.SetBaseFS(migrations.FS)
	if err := goose.SetDialect("postgres"); err != nil {
		log.Fatal().Err(err).Msg("set goose dialect")
	}
	if err := goose.Up(db, "."); err != nil {
		log.Fatal().Err(err).Msg("apply migrations")
	}

	version, err := goose.GetDBVersion(db)
	if err != nil {
		log.Fatal().Err(err).Msg("read migration version")
	}
	log.Info().Int64("db_version", version).Msg("migrations applied")
}
