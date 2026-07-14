-- +goose Up

-- Execution timestamps: set by the worker when it starts and finishes a job.
-- ALTER on the partitioned parent propagates to all partitions.
ALTER TABLE jobs
    ADD COLUMN started_at timestamptz,
    ADD COLUMN finished_at timestamptz;

-- +goose Down
ALTER TABLE jobs
    DROP COLUMN started_at,
    DROP COLUMN finished_at;
