-- 0003_execution_control.sql
-- Phase 3: sleep / delayed steps, step timeouts, cancellation
-- (build-plan.html, features #9-#11). Append-only — 0001/0002 are never edited.
--
-- Most of what Phase 3 needs is already in the 0001 schema, reserved for it:
-- `step.run_after` (the "not claimable until" gate the claim query already
-- enforces), `step.timeout_ms`, and 'cancelled' in both status CHECK
-- constraints. This migration only adds what genuinely has nowhere to live.

-- Cancellation is cooperative, not a hard kill, so it needs a *request* flag
-- distinct from the terminal `status = 'cancelled'`. At the moment a cancel
-- arrives, a step of this run may be mid-flight on another worker's process:
-- there is no way to reach into that process and stop the JS, and stomping
-- the step row from here would race the fenced commit path (lockStepIfOwner)
-- that worker is about to take — exactly the double-write 0002's fencing
-- exists to prevent. So the API records intent here, the owning worker
-- observes it at a safe point (between steps, or via an explicit
-- ctx.checkCancellation()) and finalizes its own step, and only then does the
-- run reach the terminal 'cancelled' state. Nullable timestamp rather than a
-- boolean: *when* the cancel was asked for is the interesting datum for
-- latency debugging ("we asked at T, the run stopped at T+9s"), and NULL is a
-- cheaper "no request" than a NOT NULL boolean default.
alter table run add column cancel_requested_at timestamptz;

-- How many ctx.sleep() calls this step has ALREADY SERVED. Load-bearing and
-- easy to under-estimate: a sleeping step is not a suspended coroutine — we
-- do not (and on a crash-proof engine, cannot) freeze a JS stack across a
-- 24-hour sleep and a process restart. Instead the step goes back to 'ready'
-- with a future `run_after`, the worker is released, and on wake the step
-- function is claimed and **re-run from the very top**. Without a durable
-- counter the first ctx.sleep() would suspend again on every wake and the
-- step would never progress past it. With it, the re-executing context
-- resolves the first `sleep_seq` sleep calls immediately and only suspends on
-- call number sleep_seq + 1 — i.e. this is the sleep-side equivalent of the
-- memoized-result replay that makes any durable engine's re-execution safe.
--
-- Because of that replay, everything a step does before its first sleep runs
-- again on each wake: step bodies must keep their side effects idempotent,
-- the same contract retries already impose.
alter table step add column sleep_seq integer not null default 0;

-- Observability only — nothing in the claim path reads this. A sleeping step
-- and a step in retry backoff are byte-for-byte indistinguishable otherwise
-- (both are status = 'ready' with run_after in the future), which makes
-- "why is this run sitting still?" unanswerable from the row alone. Set on
-- suspend, cleared when the woken step is running again, so a non-null value
-- means "deliberately asleep until this instant", not "failing and backing
-- off". Kept out of the CHECK/claim logic on purpose: a stale value must
-- never be able to hold a step back from being claimed.
alter table step add column sleeping_until timestamptz;

-- No new index for the wake path. Waking is not a separate sweep — a sleeping
-- step is just a `ready` row with a future `run_after`, so it re-enters the
-- queue through the ordinary claimNextStep query, and `step_claim_idx`
-- (0001: (priority desc, run_after) where status = 'ready') already covers
-- it exactly: the index is ordered by run_after within a priority, so the
-- `run_after <= now()` predicate is a range scan over the leading edge of the
-- index and still-sleeping rows are simply never visited. An index on
-- sleeping_until would be pure write amplification for a column only humans
-- and tests read.

-- The cancellation sweep (getCancelRequestedRuns) does need one. It scans for
-- runs with a pending request, which is a vanishingly small fraction of the
-- run table in any healthy system — the textbook case for a partial index:
-- it indexes only the rows that can ever match, so it stays tiny, costs
-- almost nothing to maintain (no entry is written for the overwhelming
-- majority of runs, which are never cancelled), and turns a full scan of
-- every run ever executed into a lookup proportional to the number of
-- in-flight cancellations.
create index run_cancel_requested_idx on run (cancel_requested_at)
  where cancel_requested_at is not null;
