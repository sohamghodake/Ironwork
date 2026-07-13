// Command gateway is the REST edge of the cluster. Phase 0 exposes
// GET /health, backed by the observer's ClusterCheck over mTLS gRPC, and
// GET /healthz for its own liveness.
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

	conn, err := grpc.NewClient(cfg.ObserverAddr, grpc.WithTransportCredentials(credentials.NewTLS(clientTLS)))
	if err != nil {
		log.Fatal().Err(err).Str("addr", cfg.ObserverAddr).Msg("create observer client")
	}
	defer conn.Close()

	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           gatewayhttp.NewRouter(ironworkv1.NewHealthServiceClient(conn), log),
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

	log.Info().Str("addr", cfg.HTTPAddr).Msg("HTTP server listening")
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal().Err(err).Msg("serve")
	}
	log.Info().Msg("shutdown complete")
}
