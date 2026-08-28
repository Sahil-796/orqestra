-- 0009_observability.sql
-- Phase 8: observability & dashboard (build-plan.html) — storage read model.
-- Append-only, CREATE-only: this migration adds indexes to support the new
-- read queries and one new table (`worker_health`) for #34. It does not
-- alter any existing table, column, or constraint.
--
-- What lands here:
--   * `worker_health` (#34) — a durable heartbeat row per worker, upserted by
--     the worker loop (Agent: collectors) and read by `listWorkerHealth` to
--     show which workers are alive vs. stale in the dashboard.
--   * Read-supporting indexes for #30 (run listing/timeline), #32 (duration
--     metrics), #33 (error drill-down), #35 (queue depth/throughput) — all of
--     them narrow the exact predicates/orderings the new repository
--     functions in repositories.ts use, the same "index the query you wrote"
--     discipline earlier migrations follow (see 0002/0003/0007's partial
--     indexes).

-- One row per worker process, kept current by periodic heartbeats
-- (`upsertWorkerHeartbeat`). Not append-only like `history` — a worker's row
-- is overwritten in place on every heartbeat, so this table always reflects
-- "as of the last heartbeat", not a log of every heartbeat ever sent.
--
--   worker_id      — stable identifier the worker process claims steps under
--                    (the same id used as step.lease_owner), primary key.
--   hostname       — best-effort provenance for an operator triaging a
--                    specific machine; nullable, since not every deployment
--                    can name one.
--   status         — a small self-reported label ('running' | 'draining' |
--                    'stopped', kept as text + CHECK for the same
--                    add-values-without-ALTER-TYPE reason run/step statuses
--                    are). `listWorkerHealth`'s alive/stale flag is derived
--                    from `last_heartbeat_at`, not from this column — a
--                    worker can self-report 'running' and still be stale if
--                    it wedged before its next heartbeat.
--   leased_steps   — how many steps this worker currently holds a lease on,
--                    self-reported at heartbeat time (a point-in-time
--                    snapshot, not queried live against `step` here).
--   concurrency    — the worker's configured concurrency limit
--                    (ORQ_WORKER_CONCURRENCY), so the dashboard can show
--                    utilization (leased_steps / concurrency) alongside
--                    aliveness.
--   started_at     — when this worker process came up; set once on first
--                    upsert, never moved by later heartbeats.
--   last_heartbeat_at — bumped on every heartbeat; this is the only column
--                    `listWorkerHealth`'s staleness check reads.
create table worker_health (
  worker_id text primary key,
  hostname text,
  status text not null default 'running'
    check (status in ('running', 'draining', 'stopped')),
  leased_steps integer not null default 0,
  concurrency integer,
  started_at timestamptz not null default now(),
  last_heartbeat_at timestamptz not null default now()
);

-- `listWorkerHealth` orders by last_heartbeat_at (freshest/stalest first) and
-- filters on the staleness cutoff; this index backs both. The table is
-- expected to stay small (one row per live worker process), so this is more
-- about keeping the dashboard query an index scan as fleets grow than about
-- avoiding a sequential scan disaster today.
create index worker_health_last_heartbeat_idx on worker_health (last_heartbeat_at);

-- ---- run listing / timeline (#30) -----------------------------------------

-- `listRuns` filters by status and orders by created_at desc (its default,
-- newest-first listing) — this composite index serves both the equality
-- filter and the ordering in one pass. Not partial: unlike the dead-letter
-- index (0008), every run status is a plausible filter value here, so there
-- is no small subset to narrow to.
create index run_status_created_at_idx on run (status, created_at desc);

-- `listRuns` also filters by namespace; separate from the status+created_at
-- index above so a namespace-only query (no status filter) still gets index
-- support instead of falling back to a sequential scan.
create index run_namespace_created_at_idx on run (namespace, created_at desc);

-- `getRunTimeline` reads `history` for one run ordered by `at`; `history`
-- already has `history_run_id_idx on history (run_id, at)` from 0001, which
-- covers this exactly. Nothing new needed there.

-- `getRunLogs` (#31) additionally filters that same history-by-run read down
-- to `type = 'log'` rows. A composite (run_id, type, at) index serves that
-- filtered-and-ordered read directly, rather than relying on the run_id
-- index alone and rechecking `type` row-by-row.
create index history_run_id_type_at_idx on history (run_id, type, at);

-- ---- duration metrics (#32) ------------------------------------------------

-- `getRunMetrics` aggregates step durations/queue-wait over a window and
-- optionally groups by workflow; both paths filter on step.status (to find
-- terminal steps) and join back to run.created_at for the window bound. A
-- plain index on step.status (not partial) supports the "count by terminal
-- status" aggregate across every status value, complementing the existing
-- partial `step_claim_idx`/`step_lease_idx` which only cover 'ready'/
-- 'running'.
create index step_status_idx on step (status);

-- ---- error details (#33) ---------------------------------------------------
--
-- `getRunErrors` reads `step` filtered to (run_id, status = 'failed')` — the
-- existing `step_run_id_idx` (0001) already narrows to the run; a targeted
-- partial index further narrows to the failed subset, which is what a
-- drill-down view actually renders.
create index step_run_id_failed_idx on step (run_id) where status = 'failed';

-- ---- queue depth / throughput (#35) ----------------------------------------
--
-- `getQueueDepth` is `countStepsByStatus` without a run_id filter — a
-- full-table group-by on step.status, already served by `step_status_idx`
-- above.
--
-- `getThroughput` buckets recently-finished steps (or runs) by
-- `finished_at`. A plain index on step.finished_at would help, but `step`
-- has no `finished_at` column (only `created_at`/`updated_at` — terminal
-- steps are told apart by `status`, not a dedicated timestamp), so
-- throughput buckets on `run.finished_at` instead. This index backs that.
create index run_finished_at_idx on run (finished_at) where finished_at is not null;
