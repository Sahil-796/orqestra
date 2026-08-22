-- 0006_flow_control.sql
-- Phase 6: flow control at scale (build-plan.html, features #12 & #14).
-- Append-only — earlier migrations are never edited.
--
-- Two concerns land here, both as additions to the existing `step` table:
--   #12 concurrency limits — a step may declare a concurrency *key* + *limit*;
--       at claim time it is only claimable if fewer than `limit` steps sharing
--       that key are currently `running`. Null key = unlimited (the common
--       case, and the un-keyed fast path must stay untouched).
--   #14 priority aging — no schema needed: the effective priority a claim
--       orders by is computed in the claim query from `priority` + how long the
--       step has been ready (`run_after` age). Documented here for the record.

-- #12: the declared key + limit, persisted onto the step row so the claim
-- query is self-contained (no join back to the workflow dag to learn a step's
-- concurrency policy). Both nullable: a step with no key is unlimited, exactly
-- as before, and the CHECK ties the two columns together so a key without a
-- positive limit (or a limit without a key) can never be written.
alter table step add column concurrency_key text;
alter table step add column concurrency_limit integer;

alter table step add constraint step_concurrency_coherent check (
  (concurrency_key is null and concurrency_limit is null)
  or (concurrency_key is not null and concurrency_limit is not null and concurrency_limit >= 1)
);

-- The claim's per-key running count (see claimNextStep in repositories.ts) is
-- `select count(*) from step where concurrency_key = $1 and status = 'running'`.
-- Without a dedicated index that's a sequential scan on every keyed claim as
-- the step table grows. Partial on exactly the predicate the count uses.
create index step_concurrency_running_idx on step (concurrency_key)
  where status = 'running' and concurrency_key is not null;

-- #14 priority aging is a pure ordering change in the claim query — the
-- effective priority is `priority + least(maxBoost, age_seconds * rate)`, with
-- `age_seconds = extract(epoch from (now() - run_after))`. It cannot be indexed
-- (now() is not immutable), but that's fine: step_claim_idx (0001) still
-- narrows the scan to status = 'ready', and the effective-priority sort runs
-- over that already-small candidate set before `FOR UPDATE SKIP LOCKED LIMIT 1`
-- stops it. With the aging rate at 0 the expression collapses to the original
-- `order by priority desc, run_after`, so the index remains a perfect match for
-- the un-aged default.
