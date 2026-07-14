// Command gateway is the REST edge of the cluster. Phase 2: a pure edge — no
// database, no placement. POST/GET /jobs translate to the scheduler's
// JobService over mTLS gRPC (scheduler-1 until Raft leader routing in
// Phase 3); /health aggregates via the observer.
package main

import (
	"context"
	"errors"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	ironworkv1 "github.com/sohamghodake/ironwork/gen/ironwork/v1"
	"github.com/sohamghodake/ironwork/internal/app"
	"github.com/sohamghodake/ironwork/internal/gatewayhttp"
	"github.com/sohamghodake/ironwork/internal/tlsutil"
)

func main() {
	cfg, log := app.MustLoad("gateway")

	clientTLS, err := tlsutil.Client(cfg.TLS)
	if err != nil {
		log.Fatal().Err(err).Msg("build client TLS")
	}
	creds := grpc.WithTransportCredentials(credentials.NewTLS(clientTLS))

	obsConn, err := grpc.NewClient(cfg.ObserverAddr, creds)
	if err != nil {
		log.Fatal().Err(err).Str("addr", cfg.ObserverAddr).Msg("create observer client")
	}
	defer func() { _ = obsConn.Close() }()

	schedConn, err := grpc.NewClient(cfg.SchedulerAddr, creds)
	if err != nil {
		log.Fatal().Err(err).Str("addr", cfg.SchedulerAddr).Msg("create scheduler client")
	}
	defer func() { _ = schedConn.Close() }()

	srv := &http.Server{
		Addr: cfg.HTTPAddr,
		Handler: gatewayhttp.NewRouter(gatewayhttp.Deps{
			Health: ironworkv1.NewHealthServiceClient(obsConn),
			Jobs:   ironworkv1.NewJobServiceClient(schedConn),
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

	log.Info().Str("addr", cfg.HTTPAddr).Str("scheduler", cfg.SchedulerAddr).Msg("HTTP server listening")
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal().Err(err).Msg("serve")
	}
	log.Info().Msg("shutdown complete")
}
