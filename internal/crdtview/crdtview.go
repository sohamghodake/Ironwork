// Package crdtview aggregates the statemanager replicas' CRDT internals for
// the gateway's convergence dashboard, and proxies the partition/heal
// controls (SetGossip) to every replica.
package crdtview

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	ironworkv1 "github.com/sohamghodake/ironwork/gen/ironwork/v1"
)

const perReplicaTimeout = 2 * time.Second

type member struct {
	name   string
	states ironworkv1.StateServiceClient
	close  func() error
}

// Client fans out to every statemanager replica.
type Client struct {
	members []member
	log     zerolog.Logger
}

// New connects (eagerly) to every replica.
func New(statemanagers map[string]string, tlsCfg *tls.Config, log zerolog.Logger) (*Client, error) {
	if len(statemanagers) == 0 {
		return nil, fmt.Errorf("crdtview: no statemanagers configured (IRONWORK_STATEMANAGERS)")
	}

	names := make([]string, 0, len(statemanagers))
	for name := range statemanagers {
		names = append(names, name)
	}
	sort.Strings(names)

	creds := credentials.NewTLS(tlsCfg)
	c := &Client{log: log}
	for _, name := range names {
		conn, err := grpc.NewClient(statemanagers[name], grpc.WithTransportCredentials(creds))
		if err != nil {
			c.Close()
			return nil, fmt.Errorf("crdtview: client for %s: %w", name, err)
		}
		conn.Connect()
		c.members = append(c.members, member{
			name:   name,
			states: ironworkv1.NewStateServiceClient(conn),
			close:  conn.Close,
		})
	}
	return c, nil
}

// Close tears down the replica connections.
func (c *Client) Close() {
	for _, m := range c.members {
		if m.close != nil {
			_ = m.close()
		}
	}
}

// CounterView is one G-Counter with its shard breakdown (the shard table is
// what the dashboard renders to show why merge works).
type CounterView struct {
	Total  uint64            `json:"total"`
	Shards map[string]uint64 `json:"shards"`
}

// RecentView is one LWW entry, flattened for display.
type RecentView struct {
	JobID    string `json:"job_id"`
	Status   string `json:"status"`
	Worker   string `json:"worker"`
	TSUnixMS int64  `json:"ts_unix_ms"`
	Replica  string `json:"replica"`
}

// ReplicaView is one replica's full CRDT state (or its unreachability).
type ReplicaView struct {
	Name          string                 `json:"name"`
	Reachable     bool                   `json:"reachable"`
	Error         string                 `json:"error,omitempty"`
	GossipEnabled bool                   `json:"gossip_enabled"`
	ByStatus      map[string]CounterView `json:"by_status,omitempty"`
	ByWorker      map[string]CounterView `json:"by_worker,omitempty"`
	Recent        []RecentView           `json:"recent,omitempty"`
}

// View is the aggregated dashboard payload.
type View struct {
	// Converged is true when every replica answered and their CRDT states
	// are identical.
	Converged bool          `json:"converged"`
	Replicas  []ReplicaView `json:"replicas"`
}

// Fetch queries every replica in parallel and computes convergence.
func (c *Client) Fetch(ctx context.Context) View {
	views := make([]ReplicaView, len(c.members))
	states := make([]*ironworkv1.CRDTState, len(c.members))

	var wg sync.WaitGroup
	for i, m := range c.members {
		wg.Add(1)
		go func(i int, m member) {
			defer wg.Done()
			cctx, cancel := context.WithTimeout(ctx, perReplicaTimeout)
			defer cancel()

			resp, err := m.states.GetCRDTState(cctx, &ironworkv1.GetCRDTStateRequest{})
			if err != nil {
				views[i] = ReplicaView{Name: m.name, Reachable: false, Error: status.Convert(err).Message()}
				return
			}
			states[i] = resp.State
			views[i] = ReplicaView{
				Name:          m.name,
				Reachable:     true,
				GossipEnabled: resp.GossipEnabled,
				ByStatus:      countersToView(resp.State.GetByStatus()),
				ByWorker:      countersToView(resp.State.GetByWorker()),
				Recent:        recentToView(resp.State.GetRecent()),
			}
		}(i, m)
	}
	wg.Wait()

	converged := true
	for i, st := range states {
		if st == nil || (i > 0 && !proto.Equal(states[0], st)) {
			converged = false
			break
		}
	}
	return View{Converged: converged, Replicas: views}
}

// SetGossip toggles gossip on every replica (partition/heal control).
func (c *Client) SetGossip(ctx context.Context, enabled bool) error {
	var errs []error
	for _, m := range c.members {
		cctx, cancel := context.WithTimeout(ctx, perReplicaTimeout)
		_, err := m.states.SetGossip(cctx, &ironworkv1.SetGossipRequest{Enabled: enabled})
		cancel()
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", m.name, err))
		}
	}
	return errors.Join(errs...)
}

func countersToView(src map[string]*ironworkv1.GCounterState) map[string]CounterView {
	out := make(map[string]CounterView, len(src))
	for key, c := range src {
		var total uint64
		for _, v := range c.Shards {
			total += v
		}
		out[key] = CounterView{Total: total, Shards: c.Shards}
	}
	return out
}

func recentToView(src map[string]*ironworkv1.LWWEntry) []RecentView {
	out := make([]RecentView, 0, len(src))
	for id, e := range src {
		out = append(out, RecentView{
			JobID: id, Status: e.Status, Worker: e.Worker, TSUnixMS: e.TsUnixMs, Replica: e.Replica,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].TSUnixMS > out[j].TSUnixMS })
	return out
}
