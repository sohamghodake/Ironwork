// Package raftnode runs a scheduler's membership in the Raft consensus
// group: leader election plus a replicated log of placement decisions.
// Postgres remains the store of record for jobs — the Raft log carries
// placement authority and its visible history, nothing else.
package raftnode

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"github.com/rs/zerolog"

	"github.com/sohamghodake/ironwork/internal/config"
	"github.com/sohamghodake/ironwork/internal/tlsutil"
)

const (
	applyTimeout     = 2 * time.Second
	transportTimeout = 10 * time.Second
	maxConnPool      = 3
	snapshotRetain   = 2
)

// Config describes this member and its peer group.
type Config struct {
	// Instance is this node's raft ServerID (e.g. scheduler-2).
	Instance string
	// BindAddr is the raft transport listen address (":9444").
	BindAddr string
	// Peers maps instance name -> advertised raft address for every member,
	// including this one.
	Peers map[string]string
	// DataDir holds the boltdb log/stable store and snapshots.
	DataDir string
	TLS     config.TLSPaths
}

// Info is a point-in-time view of one node's consensus state.
type Info struct {
	State            string
	LeaderID         string
	Term             uint64
	LastLogIndex     uint64
	AppliedIndex     uint64
	Peers            []string
	PlacementCount   uint64
	RecentPlacements []Placement
}

// Node is one member of the scheduler consensus group.
type Node struct {
	raft *raft.Raft
	fsm  *placementFSM
	id   string
	log  zerolog.Logger
}

// New starts the raft member (mTLS transport, durable boltdb state) and
// bootstraps the 3-node cluster on first boot.
func New(cfg Config, log zerolog.Logger) (*Node, error) {
	advertise, ok := cfg.Peers[cfg.Instance]
	if !ok {
		return nil, fmt.Errorf("raftnode: instance %q missing from IRONWORK_RAFT_PEERS", cfg.Instance)
	}

	serverTLS, err := tlsutil.Server(cfg.TLS)
	if err != nil {
		return nil, err
	}
	clientTLS, err := tlsutil.Client(cfg.TLS)
	if err != nil {
		return nil, err
	}
	stream, err := newTLSStreamLayer(cfg.BindAddr, advertise, serverTLS, clientTLS)
	if err != nil {
		return nil, err
	}
	transport := raft.NewNetworkTransport(stream, maxConnPool, transportTimeout, os.Stderr)

	if err := os.MkdirAll(cfg.DataDir, 0o700); err != nil {
		return nil, fmt.Errorf("raftnode: create data dir: %w", err)
	}
	boltStore, err := raftboltdb.NewBoltStore(filepath.Join(cfg.DataDir, "raft.db"))
	if err != nil {
		return nil, fmt.Errorf("raftnode: open bolt store: %w", err)
	}
	snaps, err := raft.NewFileSnapshotStore(cfg.DataDir, snapshotRetain, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("raftnode: open snapshot store: %w", err)
	}

	hasState, err := raft.HasExistingState(boltStore, boltStore, snaps)
	if err != nil {
		return nil, fmt.Errorf("raftnode: check existing state: %w", err)
	}

	rcfg := raft.DefaultConfig()
	rcfg.LocalID = raft.ServerID(cfg.Instance)

	node, err := newWithBackends(cfg.Instance, rcfg, boltStore, boltStore, snaps, transport, log)
	if err != nil {
		return nil, err
	}

	if !hasState {
		servers := make([]raft.Server, 0, len(cfg.Peers))
		for name, addr := range cfg.Peers {
			servers = append(servers, raft.Server{
				ID:      raft.ServerID(name),
				Address: raft.ServerAddress(addr),
			})
		}
		sort.Slice(servers, func(i, j int) bool { return servers[i].ID < servers[j].ID })
		// Every member races to bootstrap the same configuration; losers get
		// ErrCantBootstrap, which is the expected outcome for two of three.
		if err := node.raft.BootstrapCluster(raft.Configuration{Servers: servers}).Error(); err != nil &&
			!errors.Is(err, raft.ErrCantBootstrap) {
			return nil, fmt.Errorf("raftnode: bootstrap: %w", err)
		}
	}

	log.Info().Str("advertise", advertise).Int("peers", len(cfg.Peers)).Bool("had_state", hasState).
		Msg("raft member started")
	return node, nil
}

// newWithBackends wires a Node from explicit raft backends; tests inject
// in-memory stores and transports.
func newWithBackends(id string, rcfg *raft.Config, logs raft.LogStore, stable raft.StableStore,
	snaps raft.SnapshotStore, transport raft.Transport, log zerolog.Logger,
) (*Node, error) {
	fsm := &placementFSM{}
	r, err := raft.NewRaft(rcfg, fsm, logs, stable, snaps, transport)
	if err != nil {
		return nil, fmt.Errorf("raftnode: start raft: %w", err)
	}
	return &Node{raft: r, fsm: fsm, id: id, log: log}, nil
}

// IsLeader reports whether this node currently holds leadership.
func (n *Node) IsLeader() bool { return n.raft.State() == raft.Leader }

// LeaderID returns the instance name of the leader as this node sees it,
// or "" mid-election.
func (n *Node) LeaderID() string {
	_, id := n.raft.LeaderWithID()
	return string(id)
}

// ApplyPlacement replicates one placement decision through the log. Callers
// gate on IsLeader; a non-leader apply fails fast.
func (n *Node) ApplyPlacement(jobID, worker string) error {
	data, err := json.Marshal(Placement{JobID: jobID, Worker: worker, At: time.Now().UTC()})
	if err != nil {
		return err
	}
	future := n.raft.Apply(data, applyTimeout)
	if err := future.Error(); err != nil {
		return fmt.Errorf("raftnode: apply placement: %w", err)
	}
	if resp, ok := future.Response().(error); ok {
		return resp
	}
	return nil
}

// Info returns this node's current consensus view.
func (n *Node) Info() Info {
	stats := n.raft.Stats()
	count, recent := n.fsm.Placements()

	var peers []string
	if cfg := n.raft.GetConfiguration(); cfg.Error() == nil {
		for _, s := range cfg.Configuration().Servers {
			peers = append(peers, string(s.ID))
		}
		sort.Strings(peers)
	}

	return Info{
		State:            n.raft.State().String(),
		LeaderID:         n.LeaderID(),
		Term:             statUint(stats, "term"),
		LastLogIndex:     statUint(stats, "last_log_index"),
		AppliedIndex:     statUint(stats, "applied_index"),
		Peers:            peers,
		PlacementCount:   count,
		RecentPlacements: recent,
	}
}

func statUint(stats map[string]string, key string) uint64 {
	v, _ := strconv.ParseUint(stats[key], 10, 64)
	return v
}

// Shutdown hands leadership off if held, then stops the raft member.
func (n *Node) Shutdown() {
	if n.IsLeader() {
		if err := n.raft.LeadershipTransfer().Error(); err != nil {
			n.log.Warn().Err(err).Msg("leadership transfer failed")
		}
	}
	if err := n.raft.Shutdown().Error(); err != nil {
		n.log.Warn().Err(err).Msg("raft shutdown")
	}
	n.log.Info().Msg("raft member stopped")
}
