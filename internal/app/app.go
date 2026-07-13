// Package app wires configuration, logging, TLS, and process lifecycle for
// Ironwork server binaries.
package app

import (
	"context"
	"net"
	"os/signal"
	"syscall"

	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	ironworkv1 "github.com/sohamghodake/ironwork/gen/ironwork/v1"
	"github.com/sohamghodake/ironwork/internal/config"
	"github.com/sohamghodake/ironwork/internal/healthsvc"
	"github.com/sohamghodake/ironwork/internal/logging"
	"github.com/sohamghodake/ironwork/internal/tlsutil"
)

// MustLoad loads config for the component and builds its logger, exiting the
// process on error.
func MustLoad(component string) (*config.Config, zerolog.Logger) {
	cfg, err := config.Load(component)
	if err != nil {
		fallback := logging.New(component, component, "info")
		fallback.Fatal().Err(err).Msg("load config")
	}
	return cfg, logging.New(cfg.Component, cfg.Instance, cfg.LogLevel)
}

// RunStub runs an mTLS gRPC server exposing only HealthService — the Phase 0
// shape of scheduler, worker, and statemanager. Blocks until SIGINT/SIGTERM.
func RunStub(component string) {
	cfg, log := MustLoad(component)

	tlsCfg, err := tlsutil.Server(cfg.TLS)
	if err != nil {
		log.Fatal().Err(err).Msg("build server TLS")
	}

	srv := grpc.NewServer(grpc.Creds(credentials.NewTLS(tlsCfg)))
	ironworkv1.RegisterHealthServiceServer(srv, healthsvc.New(cfg.Component, cfg.Instance))
	ServeGRPC(srv, cfg.GRPCAddr, log)
}

// ServeGRPC listens on addr and serves srv until SIGINT/SIGTERM, then stops
// gracefully.
func ServeGRPC(srv *grpc.Server, addr string, log zerolog.Logger) {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatal().Err(err).Str("addr", addr).Msg("listen")
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		log.Info().Msg("shutting down")
		srv.GracefulStop()
	}()

	log.Info().Str("addr", addr).Msg("gRPC server listening")
	if err := srv.Serve(lis); err != nil {
		log.Fatal().Err(err).Msg("serve")
	}
	log.Info().Msg("shutdown complete")
}
