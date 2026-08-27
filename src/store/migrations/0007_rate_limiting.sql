-- 0007_rate_limiting.sql
-- Phase 6, feature #13 — rate limiting (build-plan.html: "≤100 calls/min
-- enforced under a 1k-run flood"). Append-only; earlier migrations are never
-- edited (the #12/#14 flow-control columns live in 0006).
--
-- A step may declare a rate *key*, a *limit*, and a *window* (ms): at most
-- `limit` steps sharing that key may START within any one fixed window. Null
-- key = unlimited (the common case) and the un-keyed claim path stays
-- untouched. The declaration is persisted onto the step row so the claim query
-- is self-contained (no join back to the workflow dag to learn a step's rate
-- policy), exactly as #12 did for concurrency.

alter table step add column rate_key text;
alter table step add column rate_limit integer;
alter table step add column rate_window_ms integer;

-- All three columns move together: a step is either fully unlimited (all null)
-- or fully declared (key + positive limit + positive window). Mirrors the
-- step_concurrency_coherent CHECK from 0006 so a half-declared rate policy can
-- never be written.
alter table step add constraint step_rate_coherent check (
  (rate_key is null and rate_limit is null and rate_window_ms is null)
  or (
    rate_key is not null
    and rate_limit is not null and rate_limit >= 1
    and rate_window_ms is not null and rate_window_ms >= 1
  )
);

-- Window model: a FIXED-WINDOW counter. Time is chopped into contiguous
-- `window_ms` buckets aligned to the epoch; `window_start` is the bucket's
-- start as epoch milliseconds (floor(now_ms / window_ms) * window_ms). One row
-- per (key, window) holds how many starts that window has already granted. The
-- claim path (claimNextStep in repositories.ts) takes a per-key advisory lock,
-- then count-and-consumes against this row atomically — so two workers racing
-- the same key can never both see budget and both consume it (the same race
-- #12's advisory lock closes for concurrency). Chosen over a sliding window
-- because a single counter row is enough to enforce the cap and needs no
-- per-start timestamp history.
--
-- Old window rows are harmless to leave behind (a key's next window is a new
-- row with a larger window_start); a periodic vacuum of stale windows is a
-- future concern, not a correctness one.
create table rate_window (
  rate_key text not null,
  window_start bigint not null,
  count integer not null default 0,
  primary key (rate_key, window_start)
);
