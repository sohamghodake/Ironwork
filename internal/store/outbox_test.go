package store_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sohamghodake/ironwork/internal/store"
)

func TestCreateJobWithOutbox(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	job, err := s.CreateJobWithOutbox(ctx, "outboxed", []byte(`{"duration_ms":10}`))
	require.NoError(t, err)
	assert.Equal(t, store.StatusPending, job.Status)

	// The dispatch command lands atomically with the job.
	entries, err := s.ClaimOutbox(ctx, 10)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, job.ID, entries[0].Job.ID)
	assert.Equal(t, "outboxed", entries[0].Job.Name)
	assert.Equal(t, store.StatusPending, entries[0].Job.Status)
	assert.Zero(t, entries[0].Attempts)

	stats, err := s.OutboxStats(ctx)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), stats.Pending)
	assert.Positive(t, stats.OldestPending)
}

func TestOutboxDispatchLifecycle(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	job, err := s.CreateJobWithOutbox(ctx, "j", nil)
	require.NoError(t, err)
	id := mustClaimID(t, s, job.ID)

	require.NoError(t, s.MarkOutboxDispatched(ctx, id))

	// A dispatched command is no longer claimable.
	entries, err := s.ClaimOutbox(ctx, 10)
	require.NoError(t, err)
	assert.Empty(t, entries)

	stats, err := s.OutboxStats(ctx)
	require.NoError(t, err)
	assert.Equal(t, uint64(0), stats.Pending)
	assert.Equal(t, uint64(1), stats.Dispatched)
	assert.Zero(t, stats.OldestPending)
}

func TestOutboxFailureThenAbandon(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	job, err := s.CreateJobWithOutbox(ctx, "j", nil)
	require.NoError(t, err)
	id := mustClaimID(t, s, job.ID)

	require.NoError(t, s.RecordOutboxFailure(ctx, id, "no worker accepted"))
	entries, err := s.ClaimOutbox(ctx, 10)
	require.NoError(t, err)
	require.Len(t, entries, 1, "a failed-but-not-abandoned command stays claimable")
	assert.Equal(t, 1, entries[0].Attempts)

	require.NoError(t, s.MarkOutboxFailed(ctx, id, "gave up"))
	entries, err = s.ClaimOutbox(ctx, 10)
	require.NoError(t, err)
	assert.Empty(t, entries)

	stats, err := s.OutboxStats(ctx)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), stats.Failed)
}

func TestReclaimJobsReEnqueuesOutbox(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	// A running job whose initial dispatch command was already consumed.
	job, err := s.CreateJobWithOutbox(ctx, "running", nil)
	require.NoError(t, err)
	require.NoError(t, s.MarkOutboxDispatched(ctx, mustClaimID(t, s, job.ID)))
	require.NoError(t, s.MarkScheduled(ctx, job.ID, "worker-1"))
	require.NoError(t, s.MarkRunning(ctx, job.ID))

	reclaimed, err := s.ReclaimJobs(ctx, "worker-1")
	require.NoError(t, err)
	require.Len(t, reclaimed, 1)

	// Reclaim re-enqueues a fresh dispatch command for the pending job.
	entries, err := s.ClaimOutbox(ctx, 10)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, job.ID, entries[0].Job.ID)
	assert.Equal(t, store.StatusPending, entries[0].Job.Status)
}

// mustClaimID returns the pending outbox row id for jobID.
func mustClaimID(t *testing.T, s *store.Store, jobID string) string {
	t.Helper()
	entries, err := s.ClaimOutbox(context.Background(), 50)
	require.NoError(t, err)
	for _, e := range entries {
		if e.Job.ID == jobID {
			return e.ID
		}
	}
	t.Fatalf("no pending outbox row for job %s", jobID)
	return ""
}
