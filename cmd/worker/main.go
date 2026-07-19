// Command worker executes placed jobs under a hard concurrency cap
// (ExecuteJob rejects with ResourceExhausted at capacity — backpressure) and
// heartbeats its liveness and load to every scheduler so the leader can
// steer placement and detect crashes (Phase 4). It owns every job status
// transition in Postgres after acceptance; execution is still the stub
// (sleep + return).
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
	"github.com/sohamghodake/ironwork/internal/heartbeat"
	"github.com/sohamghodake/ironwork/internal/store"
	"github.com/sohamghodake/ironwork/internal/tlsutil"
	"github.com/sohamghodake/ironwork/internal/workersvc"
)

const drainTimeout = 30 * time.Second

func main() {
	cfg, log := app.MustLoad("worker")

	serverTLS, err := tlsutil.Server(cfg.TLS)
	if err != nil {
		log.Fatal().Err(err).Msg("build server TLS")
	}
	clientTLS, err := tlsutil.Client(cfg.TLS)
	if err != nil {
		log.Fatal().Err(err).Msg("build client TLS")
	}

	pool, err := pgxpool.New(context.Background(), cfg.DBDSN)
	if err != nil {
		log.Fatal().Err(err).Msg("create db pool")
	}
	defer pool.Close()

	svc := workersvc.New(cfg.Instance, store.New(pool), cfg.Capacity, log)

	hb, err := heartbeat.New(heartbeat.Config{
		Instance:   cfg.Instance,
		Capacity:   svc.Capacity(),
		Inflight:   svc.Inflight,
		Schedulers: cfg.Schedulers,
		TLS:        clientTLS,
	}, log)
	if err != nil {
		log.Fatal().Err(err).Msg("build heartbeat loop")
	}
	defer hb.Close()
	hbCtx, hbCancel := context.WithCancel(context.Background())
	defer hbCancel()
	go hb.Run(hbCtx)

	srv := grpc.NewServer(grpc.Creds(credentials.NewTLS(serverTLS)))
	ironworkv1.RegisterWorkerServiceServer(srv, svc)
	ironworkv1.RegisterHealthServiceServer(srv, healthsvc.New(cfg.Component, cfg.Instance))

	log.Info().Int("capacity", svc.Capacity()).Int("schedulers", len(cfg.Schedulers)).Msg("worker starting")
	app.ServeGRPC(srv, cfg.GRPCAddr, log)

	// ServeGRPC returns after GracefulStop: no new RPCs, but accepted jobs may
	// still be executing.
	svc.Drain(drainTimeout)
}
