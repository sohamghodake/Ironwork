// Package heartbeat runs the worker's liveness loop: RegisterWorker once at
// startup, then Heartbeat on an interval, fanned out to every scheduler.
// Reporting to all members keeps every registry warm, so a Raft leader
// failover loses no liveness state.
package heartbeat

import (
	"context"
	"crypto/tls"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/types/known/timestamppb"

	ironworkv1 "github.com/sohamghodake/ironwork/gen/ironwork/v1"
)

// Interval is how often workers report; schedulers advertise the same value
// on registration and derive their staleness TTLs from it.
const Interval = 2 * time.Second

// perCallTimeout bounds one heartbeat RPC; a slow scheduler must not delay
// reports to the others past the tick.
const perCallTimeout = 1500 * time.Millisecond

// Config describes the reporting worker.
type Config struct {
	Instance string
	Capacity int
	// Inflight is sampled at each tick.
	Inflight func() int
	// Schedulers maps instance name -> gRPC address.
	Schedulers map[string]string
	TLS        *tls.Config
}

type target struct {
	name   string
	client ironworkv1.WorkerServiceClient
	close  func() error
}

// Loop is the worker's heartbeat reporter.
type Loop struct {
	cfg     Config
	targets []target
	log     zerolog.Logger
}

// New connects (eagerly) to every scheduler.
func New(cfg Config, log zerolog.Logger) (*Loop, error) {
	if len(cfg.Schedulers) == 0 {
		return nil, fmt.Errorf("heartbeat: no schedulers configured (IRONWORK_SCHEDULERS)")
	}

	names := make([]string, 0, len(cfg.Schedulers))
	for name := range cfg.Schedulers {
		names = append(names, name)
	}
	sort.Strings(names)

	creds := credentials.NewTLS(cfg.TLS)
	l := &Loop{cfg: cfg, log: log}
	for _, name := range names {
		conn, err := grpc.NewClient(cfg.Schedulers[name], grpc.WithTransportCredentials(creds))
		if err != nil {
			l.Close()
			return nil, fmt.Errorf("heartbeat: client for %s: %w", name, err)
		}
		conn.Connect()
		l.targets = append(l.targets, target{
			name:   name,
			client: ironworkv1.NewWorkerServiceClient(conn),
			close:  conn.Close,
		})
	}
	return l, nil
}

// Run registers, then heartbeats every Interval until ctx is done.
func (l *Loop) Run(ctx context.Context) {
	l.fanOut(ctx, func(cctx context.Context, t target) error {
		_, err := t.client.RegisterWorker(cctx, &ironworkv1.RegisterWorkerRequest{
			WorkerId: l.cfg.Instance,
			Capacity: uint32(l.cfg.Capacity), //nolint:gosec // capacity is a small config value
		})
		return err
	})

	ticker := time.NewTicker(Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			inflight := l.cfg.Inflight()
			l.fanOut(ctx, func(cctx context.Context, t target) error {
				_, err := t.client.Heartbeat(cctx, &ironworkv1.HeartbeatRequest{
					WorkerId: l.cfg.Instance,
					Capacity: uint32(l.cfg.Capacity), //nolint:gosec // capacity is a small config value
					Inflight: uint32(inflight),       //nolint:gosec // bounded by capacity
					SentAt:   timestamppb.Now(),
				})
				return err
			})
		}
	}
}

// fanOut calls fn against every scheduler in parallel, logging failures at
// debug — a scheduler being down is normal during failover.
func (l *Loop) fanOut(ctx context.Context, fn func(context.Context, target) error) {
	var wg sync.WaitGroup
	for _, t := range l.targets {
		wg.Add(1)
		go func(t target) {
			defer wg.Done()
			cctx, cancel := context.WithTimeout(ctx, perCallTimeout)
			defer cancel()
			if err := fn(cctx, t); err != nil {
				l.log.Debug().Err(err).Str("scheduler", t.name).Msg("heartbeat not delivered")
			}
		}(t)
	}
	wg.Wait()
}

// Close tears down the scheduler connections.
func (l *Loop) Close() {
	for _, t := range l.targets {
		if t.close != nil {
			_ = t.close()
		}
	}
}
