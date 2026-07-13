// Command observer aggregates cluster health: it fans HealthService.Check out
// to every configured component over mTLS, pings Postgres, and serves the
// combined result via HealthService.ClusterCheck (consumed by the gateway's
// REST /health).
package main

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	ironworkv1 "github.com/sohamghodake/ironwork/gen/ironwork/v1"
	"github.com/sohamghodake/ironwork/internal/app"
	"github.com/sohamghodake/ironwork/internal/observersvc"
	"github.com/sohamghodake/ironwork/internal/tlsutil"
)

func main() {
	cfg, log := app.MustLoad("observer")

	serverTLS, err := tlsutil.Server(cfg.TLS)
	if err != nil {
		log.Fatal().Err(err).Msg("build server TLS")
	}
	clientTLS, err := tlsutil.Client(cfg.TLS)
	if err != nil {
		log.Fatal().Err(err).Msg("build client TLS")
	}

	// The pool connects lazily: a down database surfaces as NOT_SERVING in
	// ClusterCheck rather than crashing the observer.
	pool, err := pgxpool.New(context.Background(), cfg.DBDSN)
	if err != nil {
		log.Fatal().Err(err).Msg("create db pool")
	}
	defer pool.Close()

	svc := observersvc.New(cfg.Instance, cfg.Targets, pool, observersvc.GRPCTargetChecker(clientTLS), log)
	srv := grpc.NewServer(grpc.Creds(credentials.NewTLS(serverTLS)))
	ironworkv1.RegisterHealthServiceServer(srv, svc)
	app.ServeGRPC(srv, cfg.GRPCAddr, log)
}
