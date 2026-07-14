// Command scheduler is the job-placement service. Phase 3: the three
// instances form a Raft consensus group (mTLS transport on :9444). Only the
// elected leader accepts SubmitJob and places jobs; followers reject fast and
// the gateway reroutes. Placement decisions replicate through the Raft log,
// and StateService exposes each node's consensus view.
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
	"github.com/sohamghodake/ironwork/internal/raftnode"
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

	node, err := raftnode.New(raftnode.Config{
		Instance: cfg.Instance,
		BindAddr: cfg.RaftAddr,
		Peers:    cfg.RaftPeers,
		DataDir:  cfg.RaftDataDir,
		TLS:      cfg.TLS,
	}, log)
	if err != nil {
		log.Fatal().Err(err).Msg("start raft member")
	}

	srv := grpc.NewServer(grpc.Creds(credentials.NewTLS(serverTLS)))
	ironworkv1.RegisterJobServiceServer(srv, schedulersvc.New(store.New(pool), disp, node, node, log))
	ironworkv1.RegisterStateServiceServer(srv, schedulersvc.NewStateServer(cfg.Instance, node))
	ironworkv1.RegisterHealthServiceServer(srv, healthsvc.New(cfg.Component, cfg.Instance))

	log.Info().Int("workers", len(cfg.Workers)).Int("raft_peers", len(cfg.RaftPeers)).Msg("scheduler starting")
	app.ServeGRPC(srv, cfg.GRPCAddr, log)

	// gRPC has stopped accepting; hand leadership off before exiting so the
	// survivors elect a replacement quickly on graceful shutdown.
	node.Shutdown()
}
