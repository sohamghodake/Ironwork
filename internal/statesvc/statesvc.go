// Package statesvc implements the statemanager's StateService: a CRDT-backed
// eventually-consistent statistics view over job outcomes. Replicas ingest
// worker reports independently and converge via push-pull gossip — no
// consensus, no coordination, always writable. SetGossip simulates a
// network partition for the convergence demo.
package statesvc

import (
	"context"
	"crypto/tls"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	ironworkv1 "github.com/sohamghodake/ironwork/gen/ironwork/v1"
	"github.com/sohamghodake/ironwork/internal/crdt"
)

// GossipInterval is how often each replica push-pulls with its peers.
const GossipInterval = 1 * time.Second

const (
	recentLimit    = 50
	perPeerTimeout = 1500 * time.Millisecond
)

// Outcomes accepted by ReportJobEvent.
const (
	OutcomeSucceeded = "succeeded"
	OutcomeFailed    = "failed"
)

// Server implements StateService for one statemanager replica.
type Server struct {
	ironworkv1.UnimplementedStateServiceServer

	replica  string
	gossipOn atomic.Bool
	log      zerolog.Logger

	mu       sync.Mutex
	byStatus map[string]crdt.GCounter
	byWorker map[string]crdt.GCounter
	recent   *crdt.LWWMap
}

// New builds a replica's state service with gossip enabled.
func New(replica string, log zerolog.Logger) *Server {
	s := &Server{
		replica:  replica,
		byStatus: map[string]crdt.GCounter{},
		byWorker: map[string]crdt.GCounter{},
		recent:   crdt.NewLWWMap(recentLimit),
		log:      log,
	}
	s.gossipOn.Store(true)
	return s
}

// ReportJobEvent ingests one terminal job outcome into this replica's CRDT.
func (s *Server) ReportJobEvent(_ context.Context, req *ironworkv1.ReportJobEventRequest) (*ironworkv1.ReportJobEventResponse, error) {
	if req.JobId == "" || req.Worker == "" {
		return nil, status.Error(codes.InvalidArgument, "job_id and worker are required")
	}
	if req.Outcome != OutcomeSucceeded && req.Outcome != OutcomeFailed {
		return nil, status.Errorf(codes.InvalidArgument, "outcome must be %q or %q", OutcomeSucceeded, OutcomeFailed)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.counterLocked(s.byStatus, req.Outcome).Inc(s.replica)
	s.counterLocked(s.byWorker, req.Worker).Inc(s.replica)
	s.recent.Set(req.JobId, crdt.LWWEntry{
		Status:   req.Outcome,
		Worker:   req.Worker,
		TSUnixMS: time.Now().UnixMilli(),
		Replica:  s.replica,
	})
	s.log.Debug().Str("job_id", req.JobId).Str("outcome", req.Outcome).Msg("event ingested")
	return &ironworkv1.ReportJobEventResponse{}, nil
}

// SyncState is one push-pull gossip exchange: merge the sender's state,
// answer with our own (now merged) state. A partitioned replica refuses.
func (s *Server) SyncState(_ context.Context, req *ironworkv1.SyncStateRequest) (*ironworkv1.SyncStateResponse, error) {
	if !s.gossipOn.Load() {
		return nil, status.Error(codes.Unavailable, "gossip disabled (simulated partition)")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.mergeLocked(req.State)
	return &ironworkv1.SyncStateResponse{State: s.snapshotLocked()}, nil
}

// GetCRDTState dumps this replica's full CRDT internals for the dashboard.
func (s *Server) GetCRDTState(context.Context, *ironworkv1.GetCRDTStateRequest) (*ironworkv1.GetCRDTStateResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return &ironworkv1.GetCRDTStateResponse{
		State:         s.snapshotLocked(),
		GossipEnabled: s.gossipOn.Load(),
		Replica:       s.replica,
	}, nil
}

// SetGossip toggles gossip participation (partition simulation).
func (s *Server) SetGossip(_ context.Context, req *ironworkv1.SetGossipRequest) (*ironworkv1.SetGossipResponse, error) {
	s.gossipOn.Store(req.Enabled)
	s.log.Warn().Bool("enabled", req.Enabled).Msg("gossip toggled")
	return &ironworkv1.SetGossipResponse{Enabled: req.Enabled}, nil
}

// GetClusterState identifies the replica; the interesting state lives in
// GetCRDTState.
func (s *Server) GetClusterState(context.Context, *ironworkv1.GetClusterStateRequest) (*ironworkv1.GetClusterStateResponse, error) {
	return &ironworkv1.GetClusterStateResponse{
		State: &ironworkv1.ClusterState{ReportingNode: s.replica, AsOf: timestamppb.Now()},
	}, nil
}

// counterLocked returns the counter for key, creating it if absent.
func (s *Server) counterLocked(m map[string]crdt.GCounter, key string) crdt.GCounter {
	c, ok := m[key]
	if !ok {
		c = crdt.NewGCounter()
		m[key] = c
	}
	return c
}

func (s *Server) mergeLocked(state *ironworkv1.CRDTState) {
	if state == nil {
		return
	}
	mergeCounters(s, s.byStatus, state.ByStatus)
	mergeCounters(s, s.byWorker, state.ByWorker)

	entries := make(map[string]crdt.LWWEntry, len(state.Recent))
	for id, e := range state.Recent {
		entries[id] = crdt.LWWEntry{Status: e.Status, Worker: e.Worker, TSUnixMS: e.TsUnixMs, Replica: e.Replica}
	}
	s.recent.Merge(entries)
}

func mergeCounters(s *Server, dst map[string]crdt.GCounter, src map[string]*ironworkv1.GCounterState) {
	for key, remote := range src {
		s.counterLocked(dst, key).Merge(crdt.GCounter(remote.Shards))
	}
}

func (s *Server) snapshotLocked() *ironworkv1.CRDTState {
	out := &ironworkv1.CRDTState{
		ByStatus: countersToProto(s.byStatus),
		ByWorker: countersToProto(s.byWorker),
		Recent:   map[string]*ironworkv1.LWWEntry{},
	}
	for id, e := range s.recent.Entries() {
		out.Recent[id] = &ironworkv1.LWWEntry{Status: e.Status, Worker: e.Worker, TsUnixMs: e.TSUnixMS, Replica: e.Replica}
	}
	return out
}

func countersToProto(m map[string]crdt.GCounter) map[string]*ironworkv1.GCounterState {
	out := make(map[string]*ironworkv1.GCounterState, len(m))
	for key, c := range m {
		out[key] = &ironworkv1.GCounterState{Shards: c.Clone()}
	}
	return out
}

// RunGossip push-pulls with every peer each interval until ctx is done.
// peers maps instance name -> gRPC address and may include this replica
// (skipped by name).
func (s *Server) RunGossip(ctx context.Context, peers map[string]string, tlsCfg *tls.Config) error {
	type peer struct {
		name   string
		client ironworkv1.StateServiceClient
	}

	names := make([]string, 0, len(peers))
	for name := range peers {
		if name != s.replica {
			names = append(names, name)
		}
	}
	sort.Strings(names)

	creds := credentials.NewTLS(tlsCfg)
	clients := make([]peer, 0, len(names))
	for _, name := range names {
		conn, err := grpc.NewClient(peers[name], grpc.WithTransportCredentials(creds))
		if err != nil {
			return err
		}
		conn.Connect()
		defer func() { _ = conn.Close() }()
		clients = append(clients, peer{name: name, client: ironworkv1.NewStateServiceClient(conn)})
	}

	ticker := time.NewTicker(GossipInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if !s.gossipOn.Load() {
				continue
			}
			for _, p := range clients {
				s.mu.Lock()
				own := s.snapshotLocked()
				s.mu.Unlock()

				cctx, cancel := context.WithTimeout(ctx, perPeerTimeout)
				resp, err := p.client.SyncState(cctx, &ironworkv1.SyncStateRequest{State: own})
				cancel()
				if err != nil {
					s.log.Debug().Err(err).Str("peer", p.name).Msg("gossip exchange failed")
					continue
				}
				s.mu.Lock()
				s.mergeLocked(resp.State)
				s.mu.Unlock()
			}
		}
	}
}
