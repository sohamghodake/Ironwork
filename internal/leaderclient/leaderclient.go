// Package leaderclient routes the gateway's job traffic to whichever
// scheduler currently leads the Raft group, and aggregates every scheduler's
// consensus view for the /raft endpoint. Followers reject writes with
// FailedPrecondition; the router walks the scheduler set (leader-first, two
// extra rounds with a pause to ride out elections) and caches whoever
// answers.
package leaderclient

import (
	"context"
	"crypto/tls"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"

	ironworkv1 "github.com/sohamghodake/ironwork/gen/ironwork/v1"
)

const (
	// retryRounds full passes over the scheduler set; the pauses between
	// rounds comfortably cover a ~1-2s leader election.
	retryRounds = 3
	roundPause  = 400 * time.Millisecond
	// raftStatusTimeout bounds each node's StateService call in the fan-out.
	raftStatusTimeout = 2 * time.Second
)

type member struct {
	name   string
	jobs   ironworkv1.JobServiceClient
	states ironworkv1.StateServiceClient
	close  func() error
}

// Router implements ironworkv1.JobServiceClient over the scheduler set.
type Router struct {
	members []member
	leader  atomic.Int64
	log     zerolog.Logger
}

// New connects (lazily) to every scheduler. schedulers maps instance name ->
// gRPC address.
func New(schedulers map[string]string, tlsCfg *tls.Config, log zerolog.Logger) (*Router, error) {
	if len(schedulers) == 0 {
		return nil, fmt.Errorf("leaderclient: no schedulers configured (IRONWORK_SCHEDULERS)")
	}

	names := make([]string, 0, len(schedulers))
	for name := range schedulers {
		names = append(names, name)
	}
	sort.Strings(names)

	creds := credentials.NewTLS(tlsCfg)
	r := &Router{log: log}
	for _, name := range names {
		conn, err := grpc.NewClient(schedulers[name], grpc.WithTransportCredentials(creds))
		if err != nil {
			r.Close()
			return nil, fmt.Errorf("leaderclient: client for %s: %w", name, err)
		}
		// Dial eagerly so the first request after boot doesn't race the
		// connection handshake.
		conn.Connect()
		r.members = append(r.members, member{
			name:   name,
			jobs:   ironworkv1.NewJobServiceClient(conn),
			states: ironworkv1.NewStateServiceClient(conn),
			close:  conn.Close,
		})
	}
	return r, nil
}

// Close tears down all scheduler connections.
func (r *Router) Close() {
	for _, m := range r.members {
		if m.close != nil {
			_ = m.close()
		}
	}
}

// retryable reports whether the error means "try another scheduler" rather
// than a definitive answer from a serving one.
func retryable(err error) bool {
	switch status.Code(err) {
	case codes.FailedPrecondition, codes.Unavailable, codes.DeadlineExceeded:
		return true
	default:
		return false
	}
}

// route walks the scheduler set leader-first until fn succeeds, caching the
// member that answers.
func route[T any](r *Router, ctx context.Context, fn func(ironworkv1.JobServiceClient) (T, error)) (T, error) {
	var zero T
	var lastErr error
	start := r.leader.Load()

	for round := 0; round < retryRounds; round++ {
		for i := range r.members {
			idx := (start + int64(i)) % int64(len(r.members))
			out, err := fn(r.members[idx].jobs)
			if err == nil {
				if idx != start {
					r.log.Info().Str("scheduler", r.members[idx].name).Msg("routing to new leader")
				}
				r.leader.Store(idx)
				return out, nil
			}
			if !retryable(err) {
				return zero, err
			}
			lastErr = err
			r.log.Debug().Err(err).Str("scheduler", r.members[idx].name).Msg("scheduler declined")
		}
		if round < retryRounds-1 {
			select {
			case <-ctx.Done():
				return zero, lastErr
			case <-time.After(roundPause):
			}
		}
	}
	return zero, lastErr
}

// SubmitJob routes to the current leader.
func (r *Router) SubmitJob(ctx context.Context, in *ironworkv1.SubmitJobRequest, opts ...grpc.CallOption) (*ironworkv1.SubmitJobResponse, error) {
	return route(r, ctx, func(c ironworkv1.JobServiceClient) (*ironworkv1.SubmitJobResponse, error) {
		return c.SubmitJob(ctx, in, opts...)
	})
}

// GetJob routes like writes for a single code path; any scheduler answers.
func (r *Router) GetJob(ctx context.Context, in *ironworkv1.GetJobRequest, opts ...grpc.CallOption) (*ironworkv1.GetJobResponse, error) {
	return route(r, ctx, func(c ironworkv1.JobServiceClient) (*ironworkv1.GetJobResponse, error) {
		return c.GetJob(ctx, in, opts...)
	})
}

// ListJobs routes like writes for a single code path; any scheduler answers.
func (r *Router) ListJobs(ctx context.Context, in *ironworkv1.ListJobsRequest, opts ...grpc.CallOption) (*ironworkv1.ListJobsResponse, error) {
	return route(r, ctx, func(c ironworkv1.JobServiceClient) (*ironworkv1.ListJobsResponse, error) {
		return c.ListJobs(ctx, in, opts...)
	})
}

// NodeStatus is one scheduler's consensus view (or its unreachability).
type NodeStatus struct {
	Name         string      `json:"name"`
	Reachable    bool        `json:"reachable"`
	Error        string      `json:"error,omitempty"`
	State        string      `json:"state,omitempty"`
	LeaderID     string      `json:"leader_id,omitempty"`
	Term         uint64      `json:"term,omitempty"`
	LastLogIndex uint64      `json:"last_log_index,omitempty"`
	AppliedIndex uint64      `json:"applied_index,omitempty"`
	Placements   []Placement `json:"recent_placements,omitempty"`
}

// Placement is one replicated placement decision as reported by a node.
type Placement struct {
	JobID  string    `json:"job_id"`
	Worker string    `json:"worker"`
	At     time.Time `json:"at"`
}

// RaftStatus fans GetClusterState out to every scheduler concurrently and
// returns each node's view, sorted by name.
func (r *Router) RaftStatus(ctx context.Context) []NodeStatus {
	out := make([]NodeStatus, len(r.members))
	var wg sync.WaitGroup
	for i, m := range r.members {
		wg.Add(1)
		go func(i int, m member) {
			defer wg.Done()
			cctx, cancel := context.WithTimeout(ctx, raftStatusTimeout)
			defer cancel()

			resp, err := m.states.GetClusterState(cctx, &ironworkv1.GetClusterStateRequest{})
			if err != nil {
				out[i] = NodeStatus{Name: m.name, Reachable: false, Error: status.Convert(err).Message()}
				return
			}
			raft := resp.State.GetRaft()
			ns := NodeStatus{
				Name:         m.name,
				Reachable:    true,
				State:        raft.GetState(),
				LeaderID:     raft.GetLeaderId(),
				Term:         raft.GetTerm(),
				LastLogIndex: raft.GetLastLogIndex(),
				AppliedIndex: raft.GetAppliedIndex(),
			}
			for _, p := range raft.GetRecentPlacements() {
				ns.Placements = append(ns.Placements, Placement{
					JobID:  p.JobId,
					Worker: p.Worker,
					At:     p.At.AsTime(),
				})
			}
			out[i] = ns
		}(i, m)
	}
	wg.Wait()
	return out
}
