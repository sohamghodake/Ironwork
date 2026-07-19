// Command statemanager runs one replica of the CRDT-backed statistics view:
// it ingests job outcome reports from its workers, push-pull gossips its
// state to peer replicas every second, and always converges with them — no
// consensus, no coordination, writable through partitions (Phase 5).
package main

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	ironworkv1 "github.com/sohamghodake/ironwork/gen/ironwork/v1"
	"github.com/sohamghodake/ironwork/internal/app"
	"github.com/sohamghodake/ironwork/internal/healthsvc"
	"github.com/sohamghodake/ironwork/internal/statesvc"
	"github.com/sohamghodake/ironwork/internal/tlsutil"
)

func main() {
	cfg, log := app.MustLoad("statemanager")

	serverTLS, err := tlsutil.Server(cfg.TLS)
	if err != nil {
		log.Fatal().Err(err).Msg("build server TLS")
	}
	clientTLS, err := tlsutil.Client(cfg.TLS)
	if err != nil {
		log.Fatal().Err(err).Msg("build client TLS")
	}

	svc := statesvc.New(cfg.Instance, log)

	gossipCtx, gossipCancel := context.WithCancel(context.Background())
	defer gossipCancel()
	go func() {
		if err := svc.RunGossip(gossipCtx, cfg.Statemanagers, clientTLS); err != nil {
			log.Fatal().Err(err).Msg("start gossip loop")
		}
	}()

	srv := grpc.NewServer(grpc.Creds(credentials.NewTLS(serverTLS)))
	ironworkv1.RegisterStateServiceServer(srv, svc)
	ironworkv1.RegisterHealthServiceServer(srv, healthsvc.New(cfg.Component, cfg.Instance))

	log.Info().Int("peers", len(cfg.Statemanagers)-1).Msg("statemanager replica starting")
	app.ServeGRPC(srv, cfg.GRPCAddr, log)
}
