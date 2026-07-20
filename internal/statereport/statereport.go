// Package statereport sends job outcome events from a worker to its
// designated statemanager replica. Reports are fire-and-forget: a lost
// report is a lost increment in an eventually-consistent view, never a
// failed job.
package statereport

import (
	"context"
	"crypto/tls"
	"fmt"
	"time"

	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	ironworkv1 "github.com/sohamghodake/ironwork/gen/ironwork/v1"
	"github.com/sohamghodake/ironwork/internal/statesvc"
)

const reportTimeout = 2 * time.Second

// Client reports to one statemanager replica.
type Client struct {
	states ironworkv1.StateServiceClient
	close  func() error
	log    zerolog.Logger
}

// New connects (eagerly) to the replica at addr.
func New(addr string, tlsCfg *tls.Config, log zerolog.Logger) (*Client, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg)))
	if err != nil {
		return nil, fmt.Errorf("statereport: client for %s: %w", addr, err)
	}
	conn.Connect()
	return &Client{
		states: ironworkv1.NewStateServiceClient(conn),
		close:  conn.Close,
		log:    log,
	}, nil
}

// ReportJobEvent sends one terminal outcome; failures are logged, not
// returned — job completion must never block on the statistics view.
func (c *Client) ReportJobEvent(ctx context.Context, jobID, worker string, succeeded bool) {
	outcome := statesvc.OutcomeSucceeded
	if !succeeded {
		outcome = statesvc.OutcomeFailed
	}

	rctx, cancel := context.WithTimeout(ctx, reportTimeout)
	defer cancel()
	_, err := c.states.ReportJobEvent(rctx, &ironworkv1.ReportJobEventRequest{
		JobId:   jobID,
		Worker:  worker,
		Outcome: outcome,
	})
	if err != nil {
		c.log.Warn().Err(err).Str("job_id", jobID).Msg("outcome report not delivered")
	}
}

// Close tears down the replica connection.
func (c *Client) Close() { _ = c.close() }
