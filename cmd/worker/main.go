// Command worker executes placed jobs: it serves WorkerService.ExecuteJob
// (dispatched by the gateway in Phase 1, the scheduler from Phase 2 on) and
// owns every job status transition in Postgres after acceptance. Execution is
// the Phase 1 stub — sleep + return. Backpressure and crash detection land in
// Phase 4.
package main

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	ironworkv1 "github.com/sohamghodake/ironwork/gen/ironwork/v1"
	"github.com/sohamghodake/ironwork/internal/app"
	"github.com/sohamghodake/ironwork/internal/healthsvc"
	"github.com/sohamghodake/ironwork/internal/store"
	"github.com/sohamghodake/ironwork/internal/tlsutil"
	"github.com/sohamghodake/ironwork/internal/workersvc"
)

const drainTimeout = 30 * time.Second

func main() {
	cfg, log := app.MustLoad("worker")

	tlsCfg, err := tlsutil.Server(cfg.TLS)
	if err != nil {
		log.Fatal().Err(err).Msg("build server TLS")
	}

	pool, err := pgxpool.New(context.Background(), cfg.DBDSN)
	if err != nil {
		log.Fatal().Err(err).Msg("create db pool")
	}
	defer pool.Close()

	svc := workersvc.New(cfg.Instance, store.New(pool), cfg.Capacity, log)
	srv := grpc.NewServer(grpc.Creds(credentials.NewTLS(tlsCfg)))
	ironworkv1.RegisterWorkerServiceServer(srv, svc)
	ironworkv1.RegisterHealthServiceServer(srv, healthsvc.New(cfg.Component, cfg.Instance))

	log.Info().Int("capacity", cfg.Capacity).Msg("worker starting")
	app.ServeGRPC(srv, cfg.GRPCAddr, log)

	// ServeGRPC returns after GracefulStop: no new RPCs, but accepted jobs may
	// still be executing.
	svc.Drain(drainTimeout)
}
