-- P2.3 — durable job-run telemetry for demand-feed daemons.
--
-- Forward-only schema for Phase 2 of the Scout reliability plan.
-- CREATE TABLE IF NOT EXISTS only — never DROP, never remove columns.
-- Apply manually after review; the plan owner applies these against prod CH.
--
-- Tables:
--   job_runs              — per-run telemetry for scraper / harvester / hourly_shadow /
--                           revenue_tracker / autocheck daemons (P2.5-P2.8).
--   per_network_status    — latest-known health row per network, kept fresh via
--                           ReplacingMergeTree on updated_at.
--   normalization_errors  — structured log of field-level normalization failures
--                           so we can grep "which networks lie about payout".
--
-- Write path: scout_core/job_runs.py (best-effort, swallows exceptions).

CREATE TABLE IF NOT EXISTS job_runs
(
    ts             DateTime64(3, 'UTC'),
    job_name       LowCardinality(String),
    network        LowCardinality(String) DEFAULT '',
    status         LowCardinality(String),                  -- 'success' | 'error' | 'started'
    error          String DEFAULT '',
    payload_hash   String DEFAULT '',
    duration_ms    UInt32 DEFAULT 0
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY (job_name, ts)
TTL toDateTime(ts) + INTERVAL 90 DAY;

CREATE TABLE IF NOT EXISTS per_network_status
(
    network                  LowCardinality(String),
    last_successful_scrape   DateTime64(3, 'UTC'),
    last_failure_ts          Nullable(DateTime64(3, 'UTC')),
    last_failure_reason      String DEFAULT '',
    last_offer_count         UInt32 DEFAULT 0,
    updated_at               DateTime64(6, 'UTC') DEFAULT now64(6)
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY network;

CREATE TABLE IF NOT EXISTS normalization_errors
(
    ts          DateTime64(3, 'UTC'),
    network     LowCardinality(String),
    offer_id    String,
    field       LowCardinality(String),
    raw_value   String,
    error       String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY (network, ts)
TTL toDateTime(ts) + INTERVAL 30 DAY;
