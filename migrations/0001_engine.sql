CREATE TABLE runs (
    id              UUID PRIMARY KEY,
    workflow_type   TEXT NOT NULL,
    idempotency_key TEXT NOT NULL UNIQUE,   -- dedupes Kafka ingress & API retries
    status          TEXT NOT NULL,          -- denormalized for cheap queries;
                                            -- authoritative status = fold(events)
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE events (
    id         BIGSERIAL PRIMARY KEY,
    run_id     UUID NOT NULL REFERENCES runs(id),
    seq        INT  NOT NULL,               -- per-run, gapless, assigned in-txn
    type       TEXT NOT NULL,
    payload    JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (run_id, seq)                    -- OCC backstop for concurrent appenders
);

CREATE TABLE step_queue (
    id               BIGSERIAL PRIMARY KEY,
    run_id           UUID NOT NULL REFERENCES runs(id),
    step_id          TEXT NOT NULL,          -- stable within a run, e.g. "extract"
    attempt          INT  NOT NULL DEFAULT 1,
    run_after        TIMESTAMPTZ NOT NULL DEFAULT now(),  -- backoff lands here
    lease_expires_at TIMESTAMPTZ,            -- NULL = unclaimed
    claimed_by       TEXT,                   -- worker id, for observability
    UNIQUE (run_id, step_id, attempt)        -- idempotency key, materialized
);

-- Plain index (not partial on lease IS NULL): the claim query also matches
-- expired leases, which a lease-IS-NULL partial index cannot serve.
CREATE INDEX step_queue_claim_idx ON step_queue (run_after);
