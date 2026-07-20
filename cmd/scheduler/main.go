// Command scheduler is the job-placement service. Phase 3: the three
// instances form a Raft consensus group (mTLS transport on :9444). Only the
// elected leader accepts SubmitJob and places jobs; followers reject fast and
// the gateway reroutes. Placement decisions replicate through the Raft log,
// and StateService exposes each node's consensus view.
package main

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	ironworkv1 "github.com/sohamghodake/ironwork/gen/ironwork/v1"
	"github.com/sohamghodake/ironwork/internal/app"
	"github.com/sohamghodake/ironwork/internal/dispatch"
	"github.com/sohamghodake/ironwork/internal/healthsvc"
	"github.com/sohamghodake/ironwork/internal/heartbeat"
	"github.com/sohamghodake/ironwork/internal/outbox"
	"github.com/sohamghodake/ironwork/internal/raftnode"
	"github.com/sohamghodake/ironwork/internal/reaper"
	"github.com/sohamghodake/ironwork/internal/registry"
	"github.com/sohamghodake/ironwork/internal/schedulersvc"
	"github.com/sohamghodake/ironwork/internal/store"
	"github.com/sohamghodake/ironwork/internal/tlsutil"
)

// Workers past staleTTL take no new placements; past deadTTL their jobs are
// reclaimed. Derived from the heartbeat interval (2s): one missed beat is
// tolerated, four means presumed dead.
const (
	workerStaleTTL = 5 * time.Second
	workerDeadTTL  = 8 * time.Second
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

	reg := registry.New(workerStaleTTL, workerDeadTTL)

	disp, err := dispatch.New(cfg.Workers, clientTLS, reg.Available, log)
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

	st := store.New(pool)

	// The outbox relay is the sole dispatcher: SubmitJob and the reaper enqueue
	// commands, the relay drains them to workers (leader-only).
	relay := outbox.New(st, disp, node, node, log)
	svcCtx, svcCancel := context.WithCancel(context.Background())
	defer svcCancel()
	go relay.Run(svcCtx)

	srv := grpc.NewServer(grpc.Creds(credentials.NewTLS(serverTLS)))
	ironworkv1.RegisterJobServiceServer(srv, schedulersvc.New(st, node, relay, log))
	ironworkv1.RegisterStateServiceServer(srv, schedulersvc.NewStateServer(cfg.Instance, node, reg))
	ironworkv1.RegisterWorkerServiceServer(srv, schedulersvc.NewWorkerServer(reg, heartbeat.Interval, log))
	ironworkv1.RegisterHealthServiceServer(srv, healthsvc.New(cfg.Component, cfg.Instance))

	// Leader-only crash detection: reclaim jobs from dead workers (which
	// re-enqueues them) and wake the relay to re-place them.
	rp := reaper.New(st, reg, node, relay, log)
	go rp.Run(svcCtx)

	log.Info().Int("workers", len(cfg.Workers)).Int("raft_peers", len(cfg.RaftPeers)).Msg("scheduler starting")
	app.ServeGRPC(srv, cfg.GRPCAddr, log)

	// gRPC has stopped accepting; hand leadership off before exiting so the
	// survivors elect a replacement quickly on graceful shutdown.
	node.Shutdown()
}
