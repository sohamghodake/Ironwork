// Package observersvc implements the observer's HealthService: the usual self
// Check plus ClusterCheck, which fans out Check calls to every configured
// component, pings Postgres, and aggregates the results.
package observersvc

import (
	"context"
	"crypto/tls"
	"sort"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/types/known/timestamppb"

	ironworkv1 "github.com/sohamghodake/ironwork/gen/ironwork/v1"
	"github.com/sohamghodake/ironwork/internal/healthsvc"
)

// perTargetTimeout bounds each individual component check so one hung target
// cannot stall the whole aggregation.
const perTargetTimeout = 3 * time.Second

// dbComponentName is how the Postgres primary appears in aggregated results.
const dbComponentName = "postgres-primary"

// TargetChecker checks the health of one component instance at addr,
// returning nil if it reports SERVING.
type TargetChecker func(ctx context.Context, addr string) error

// Pinger is the slice of pgxpool.Pool the observer needs; nil-able in tests.
type Pinger interface {
	Ping(ctx context.Context) error
}

// Server implements HealthService for the observer.
type Server struct {
	*healthsvc.Server

	targets map[string]string
	db      Pinger
	check   TargetChecker
	log     zerolog.Logger
}

// New builds the observer service. targets maps instance name -> gRPC address.
func New(instance string, targets map[string]string, db Pinger, check TargetChecker, log zerolog.Logger) *Server {
	return &Server{
		Server:  healthsvc.New("observer", instance),
		targets: targets,
		db:      db,
		check:   check,
		log:     log,
	}
}

// GRPCTargetChecker returns a TargetChecker that dials addr with the given
// client mTLS config and calls HealthService.Check.
func GRPCTargetChecker(tlsCfg *tls.Config) TargetChecker {
	creds := credentials.NewTLS(tlsCfg)
	return func(ctx context.Context, addr string) error {
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(creds))
		if err != nil {
			return err
		}
		defer func() { _ = conn.Close() }()
		_, err = ironworkv1.NewHealthServiceClient(conn).Check(ctx, &ironworkv1.CheckRequest{})
		return err
	}
}

// ClusterCheck checks every configured target and the database concurrently
// and reports per-component health plus an all-SERVING-or-not overall status.
func (s *Server) ClusterCheck(ctx context.Context, _ *ironworkv1.ClusterCheckRequest) (*ironworkv1.ClusterCheckResponse, error) {
	var (
		mu         sync.Mutex
		components []*ironworkv1.ComponentHealth
		wg         sync.WaitGroup
	)
	add := func(c *ironworkv1.ComponentHealth) {
		mu.Lock()
		defer mu.Unlock()
		components = append(components, c)
	}

	for name, addr := range s.targets {
		wg.Add(1)
		go func(name, addr string) {
			defer wg.Done()
			cctx, cancel := context.WithTimeout(ctx, perTargetTimeout)
			defer cancel()
			add(componentHealth(name, s.check(cctx, addr)))
		}(name, addr)
	}

	if s.db != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cctx, cancel := context.WithTimeout(ctx, perTargetTimeout)
			defer cancel()
			add(componentHealth(dbComponentName, s.db.Ping(cctx)))
		}()
	}

	wg.Wait()
	sort.Slice(components, func(i, j int) bool { return components[i].Name < components[j].Name })

	overall := ironworkv1.ServingStatus_SERVING_STATUS_SERVING
	for _, c := range components {
		if c.Status != ironworkv1.ServingStatus_SERVING_STATUS_SERVING {
			overall = ironworkv1.ServingStatus_SERVING_STATUS_NOT_SERVING
			s.log.Warn().Str("target", c.Name).Str("error", c.Message).Msg("component not serving")
		}
	}

	return &ironworkv1.ClusterCheckResponse{
		Status:     overall,
		Components: components,
		CheckedAt:  timestamppb.Now(),
	}, nil
}

func componentHealth(name string, err error) *ironworkv1.ComponentHealth {
	if err != nil {
		return &ironworkv1.ComponentHealth{
			Name:    name,
			Status:  ironworkv1.ServingStatus_SERVING_STATUS_NOT_SERVING,
			Message: err.Error(),
		}
	}
	return &ironworkv1.ComponentHealth{
		Name:   name,
		Status: ironworkv1.ServingStatus_SERVING_STATUS_SERVING,
	}
}
