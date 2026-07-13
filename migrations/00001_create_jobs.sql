-- +goose Up

-- jobs is range-partitioned by created_at (monthly). Partitioned tables must
-- include the partition key in the primary key, hence (id, created_at).
CREATE TABLE jobs (
    id                 uuid        NOT NULL DEFAULT gen_random_uuid(),
    name               text        NOT NULL,
    payload            bytea,
    status             text        NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'scheduled', 'running', 'succeeded', 'failed')),
    assigned_worker_id text,
    attempts           integer     NOT NULL DEFAULT 0,
    error              text,
    created_at         timestamptz NOT NULL DEFAULT now(),
    updated_at         timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (id, created_at)
) PARTITION BY RANGE (created_at);

CREATE INDEX jobs_status_created_at_idx ON jobs (status, created_at);

-- Catch-all partition so an insert outside the monthly ranges never fails.
-- Trade-off: a future monthly partition cannot be created for a range that
-- already holds rows in the default partition; the partition-maintenance job
-- in a later phase must move rows out first.
CREATE TABLE jobs_default PARTITION OF jobs DEFAULT;

-- Monthly partitions for the previous, current, and next two months relative
-- to when this migration runs.
-- +goose StatementBegin
DO $$
DECLARE
    m      date;
    p_name text;
BEGIN
    FOR i IN -1..2 LOOP
        m := (date_trunc('month', now()) + make_interval(months => i))::date;
        p_name := 'jobs_' || to_char(m, 'YYYY_MM');
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS %I PARTITION OF jobs FOR VALUES FROM (%L) TO (%L)',
            p_name, m, (m + interval '1 month')::date
        );
    END LOOP;
END $$;
-- +goose StatementEnd

-- +goose Down
DROP TABLE jobs;
