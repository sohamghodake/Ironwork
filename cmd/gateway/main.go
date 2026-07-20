// Command gateway is the REST edge of the cluster. Phase 3: job traffic
// routes to whichever scheduler leads the Raft group (followers reject,
// the router walks the set and caches the leader); GET /raft exposes every
// scheduler's consensus view; /health aggregates via the observer.
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
	"github.com/sohamghodake/ironwork/internal/crdtview"
	"github.com/sohamghodake/ironwork/internal/gatewayhttp"
	"github.com/sohamghodake/ironwork/internal/leaderclient"
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

	router, err := leaderclient.New(cfg.Schedulers, clientTLS, log)
	if err != nil {
		log.Fatal().Err(err).Msg("create scheduler router")
	}
	defer router.Close()

	crdt, err := crdtview.New(cfg.Statemanagers, clientTLS, log)
	if err != nil {
		log.Fatal().Err(err).Msg("create statemanager view")
	}
	defer crdt.Close()

	srv := &http.Server{
		Addr: cfg.HTTPAddr,
		Handler: gatewayhttp.NewRouter(gatewayhttp.Deps{
			Health: ironworkv1.NewHealthServiceClient(obsConn),
			Jobs:   router,
			Raft:   router,
			CRDT:   crdt,
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

	log.Info().Str("addr", cfg.HTTPAddr).Int("schedulers", len(cfg.Schedulers)).Msg("HTTP server listening")
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal().Err(err).Msg("serve")
	}
	log.Info().Msg("shutdown complete")
}
