-- 0002_queue.sql
-- Phase 2: turn the durable step log into a claimable, leased queue
-- (build-plan.html, features #4-#7). Append-only — 0001 is never edited.

-- Poison-pill guard: a step whose worker keeps crashing (not just throwing —
-- crashing, so the normal fail/retry path in the step function never runs)
-- must not reclaim forever. We count reclaims separately from `attempt`
-- (which counts claims, i.e. real execution attempts) so a ceiling can be
-- enforced purely on "how many times did this step's lease expire without
-- the worker finishing it", independent of ordinary retry/backoff.
alter table step add column reclaim_count integer not null default 0;

-- The reclaim sweep (reclaimExpiredLeases) scans for `running` steps whose
-- lease has expired. Without a dedicated index that's a sequential scan of
-- the whole step table on every sweep tick. `step_claim_idx` (0001) only
-- covers status = 'ready', so it doesn't help here — this is a second
-- partial index for the other status the queue cares about.
create index step_lease_idx on step (lease_expires_at) where status = 'running';

-- No change to step_claim_idx: it already orders by (priority desc,
-- run_after) under status = 'ready', which is exactly the ORDER BY the
-- claim query in repositories.ts (claimNextStep) uses. Adding run_after/
-- lease_expires_at predicates to the WHERE clause doesn't need its own
-- index — those are cheap re-checks on the small set of rows the priority
-- index already narrows us to, and `FOR UPDATE SKIP LOCKED LIMIT 1` stops
-- the scan at the first winning row anyway.
