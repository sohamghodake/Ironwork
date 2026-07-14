// Command scheduler is the job-placement service. Phase 2: it serves
// JobService — persisting submitted jobs and placing them on workers
// (round-robin) — plus HealthService. Three instances run in compose for the
// future Raft quorum shape; the gateway targets scheduler-1 until leader
// election lands in Phase 3.
package main

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	ironworkv1 "github.com/sohamghodake/ironwork/gen/ironwork/v1"
	"github.com/sohamghodake/ironwork/internal/app"
	"github.com/sohamghodake/ironwork/internal/dispatch"
	"github.com/sohamghodake/ironwork/internal/healthsvc"
	"github.com/sohamghodake/ironwork/internal/schedulersvc"
	"github.com/sohamghodake/ironwork/internal/store"
	"github.com/sohamghodake/ironwork/internal/tlsutil"
)

func main() {
	cfg, log := app.MustLoad("scheduler")

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

	disp, err := dispatch.New(cfg.Workers, clientTLS, log)
	if err != nil {
		log.Fatal().Err(err).Msg("build dispatcher")
	}
	defer disp.Close()

	srv := grpc.NewServer(grpc.Creds(credentials.NewTLS(serverTLS)))
	ironworkv1.RegisterJobServiceServer(srv, schedulersvc.New(store.New(pool), disp, log))
	ironworkv1.RegisterHealthServiceServer(srv, healthsvc.New(cfg.Component, cfg.Instance))

	log.Info().Int("workers", len(cfg.Workers)).Msg("scheduler starting")
	app.ServeGRPC(srv, cfg.GRPCAddr, log)
}
