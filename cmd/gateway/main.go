// Command gateway is the REST edge of the cluster. Phase 1: the job API
// (POST/GET /jobs) — jobs are persisted to Postgres and dispatched directly
// to workers over mTLS gRPC (round-robin; the scheduler takes over placement
// in Phase 2) — plus aggregated cluster health via the observer.
package main

import (
	"context"
	"errors"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	ironworkv1 "github.com/sohamghodake/ironwork/gen/ironwork/v1"
	"github.com/sohamghodake/ironwork/internal/app"
	"github.com/sohamghodake/ironwork/internal/dispatch"
	"github.com/sohamghodake/ironwork/internal/gatewayhttp"
	"github.com/sohamghodake/ironwork/internal/store"
	"github.com/sohamghodake/ironwork/internal/tlsutil"
)

func main() {
	cfg, log := app.MustLoad("gateway")

	clientTLS, err := tlsutil.Client(cfg.TLS)
	if err != nil {
		log.Fatal().Err(err).Msg("build client TLS")
	}

	conn, err := grpc.NewClient(cfg.ObserverAddr, grpc.WithTransportCredentials(credentials.NewTLS(clientTLS)))
	if err != nil {
		log.Fatal().Err(err).Str("addr", cfg.ObserverAddr).Msg("create observer client")
	}
	defer func() { _ = conn.Close() }()

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

	srv := &http.Server{
		Addr: cfg.HTTPAddr,
		Handler: gatewayhttp.NewRouter(gatewayhttp.Deps{
			Health: ironworkv1.NewHealthServiceClient(conn),
			Jobs:   store.New(pool),
			Disp:   disp,
			Log:    log,
		}),
		ReadHeaderTimeout: 5 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		log.Info().Msg("shutting down")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	log.Info().Str("addr", cfg.HTTPAddr).Int("workers", len(cfg.Workers)).Msg("HTTP server listening")
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal().Err(err).Msg("serve")
	}
	log.Info().Msg("shutdown complete")
}
