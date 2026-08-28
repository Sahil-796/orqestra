-- 0008_failure_handling.sql
-- Phase 7: failure handling (build-plan.html) — dead-letter + operator retry
-- (#22/#23), per-run failure policy (#25), and idempotent saga compensation
-- (#26). Append-only — earlier migrations are never edited.
--
-- Three concerns land here:
--   * two new terminal run states, so an exhausted or partially-failed run has
--     somewhere to rest that is distinct from a plain `failed`;
--   * dead-letter metadata + a per-run failure policy, both persisted on the
--     `run` row so a failed run can explain itself and be re-run without a join;
--   * a durable `compensation` log keyed by (run, step) that makes saga rollback
--     idempotent — a re-executed run can never run the same compensation twice.

-- Two new run statuses, anticipated by 0001's decision to keep run.status as
-- `text` + CHECK (not a native enum) so new values need no ALTER TYPE:
--
--   'dead_letter' — a run that exhausted its retries (or failed
--   unrecoverably) and has been parked for human inspection / manual retry
--   rather than silently discarded. Distinct from `failed`: `failed` is the
--   immediate outcome of a run whose logic gave up, `dead_letter` is the
--   durable holding state an operator surface lists and can revive.
--   `deadLetterRun` is the only writer; `resetRunForRetry` is the way out.
--
--   'completed_with_errors' — the terminal state of a run that finished under
--   the `continue_on_error` failure policy with at least one failed step: the
--   run did not stop at the first failure, ran everything it could, and ended
--   in a partial-success state that is neither a clean `completed` nor an
--   aborted `failed`.
--
-- 0001 declared run.status inline, so Postgres auto-named the constraint
-- `run_status_check` (the same `<table>_<column>_check` convention 0004 relied
-- on when it dropped `step_status_check`).
alter table run drop constraint run_status_check;
alter table run add constraint run_status_check
  check (status in (
    'queued', 'running', 'completed', 'failed', 'cancelled',
    'dead_letter', 'completed_with_errors'
  ));

-- Dead-letter metadata, on the run row itself. The run has no `error` column
-- (only `output`), so rather than overload output we add a dedicated reason +
-- timestamp: `dead_lettered_at` records when the run was parked (also the flag
-- `listDeadLetterRuns` can order by), `dead_letter_reason` is the human-facing
-- explanation (retry exhaustion, poison pill, unrecoverable error). Both null
-- for every run that was never dead-lettered. `resetRunForRetry` clears them on
-- the way back out, so a revived-and-re-dead-lettered run always reflects its
-- latest parking, not a stale one.
alter table run add column dead_lettered_at timestamptz;
alter table run add column dead_letter_reason text;

-- The failure policy the run ran under (#25), persisted so the executor can
-- read back which behaviour a given run chose without re-deriving it from the
-- workflow definition:
--   'fail_fast'         — stop the run at the first step that exhausts retries
--                         (the default, and the pre-Phase-7 behaviour).
--   'continue_on_error' — keep scheduling independent steps past a failure and
--                         end in `completed_with_errors` if any step failed.
-- Nullable with a default so existing callers (createRun) that don't set it get
-- fail-fast, and the CHECK still admits null to keep that default honest.
alter table run add column failure_policy text default 'fail_fast';
alter table run add constraint run_failure_policy_check
  check (failure_policy is null or failure_policy in ('fail_fast', 'continue_on_error'));

-- Partial index on the dead-letter set: `listDeadLetterRuns` scans exactly this
-- predicate, and dead-lettered runs are a tiny fraction of all runs, so a
-- partial index keeps the operator listing an index scan instead of a full
-- table scan as the run table grows (same reasoning as the lease / cancel
-- partial indexes in 0002/0003).
create index run_dead_letter_idx on run (dead_lettered_at)
  where status = 'dead_letter';

-- Durable compensation log for idempotent saga rollback (#26). A step that
-- performed a side effect (a charge, a booking) registers a compensating action
-- (a refund, a cancellation); when a saga unwinds, each compensation must run at
-- most once even though the run itself may be re-executed (replayed from the top
-- after a crash, or manually retried out of dead-letter). This table is the
-- durable "did this compensation already run?" record: one row per
-- (run, step_name), inserted the moment a compensation is claimed/executed.
--
-- Idempotency is enforced by the UNIQUE (run_id, step_name): `recordCompensation`
-- inserts ON CONFLICT DO NOTHING, so the first caller to record a given
-- compensation wins and every re-execution sees the row already there and skips
-- the side effect. `result`/`error` capture the compensation's outcome for
-- observability and for a reader deciding whether a failed compensation needs
-- operator attention; they are jsonb for the same reasons step.result/error are.
create table compensation (
  id uuid primary key default gen_random_uuid(),
  run_id uuid not null references run (id),
  step_id uuid references step (id),
  step_name text not null,
  status text not null default 'executed'
    check (status in ('executed', 'failed')),
  result jsonb,
  error jsonb,
  created_at timestamptz not null default now(),

  unique (run_id, step_name)
);

create index compensation_run_id_idx on compensation (run_id);
