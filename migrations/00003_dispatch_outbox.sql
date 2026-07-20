-- +goose Up

-- Transactional outbox for job dispatch. A dispatch command is committed in
-- the same transaction as the job insert (or the reclaim of a crashed job),
-- so the intent to place a job survives any crash. A leader-side relay reads
-- pending rows and hands them to workers, marking them dispatched. Not
-- partitioned: this is a small, high-churn work queue, not historical data.
CREATE TABLE outbox (
    id            uuid        NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    job_id        uuid        NOT NULL,
    status        text        NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'dispatched', 'failed')),
    attempts      integer     NOT NULL DEFAULT 0,
    last_error    text,
    created_at    timestamptz NOT NULL DEFAULT now(),
    dispatched_at timestamptz
);

-- The relay scans pending rows oldest-first; a partial index keeps that scan
-- proportional to the backlog, not the full dispatch history.
CREATE INDEX outbox_pending_idx ON outbox (created_at) WHERE status = 'pending';

-- +goose Down
DROP TABLE outbox;
