-- 0005_signals_triggers.sql
-- Phase 5: signals & triggers — wait-for-event / signals (#18), plus the
-- schema the whole phase needs so the HTTP-ingress and trigger-daemon agents
-- never have to touch DDL: an append-only events log and a schedules table.
-- Append-only — 0001..0004 are never edited.
--
-- 0001 shipped placeholder `event` / `signal_wait` tables ahead of this
-- phase. They are left in place untouched (append-only rule) but are NOT the
-- Phase 5 tables — this migration introduces the real `events` and the
-- step-side event-wait columns. The old placeholders carry no correlation
-- key, no source/dispatch bookkeeping, and no exactly-once wake machinery,
-- so reusing them would mean editing 0001; instead they simply go unused.

-- ---- events: the append-only published-signal log ------------------------
--
-- Every published signal lands here, whether or not anything is waiting for
-- it. Two independent consumers read it:
--   * `waitForEvent` (#18) matches by `name` (+ optional `correlation_key`)
--     to wake a blocked step — the live wake is done by `publishEvent` at
--     insert time, this row is the durable record + the throw->block race
--     backstop (see repositories.publishEvent / commitEventWait).
--   * the trigger daemon (Agent 3) scans `dispatched_at is null` to find
--     events it has not yet routed to event-triggered workflows, then stamps
--     `dispatched_at` so each event is routed at most once.
create table events (
  id uuid primary key default gen_random_uuid(),
  name text not null,
  -- Optional narrowing key: a waiter that supplies one is woken only by an
  -- event carrying the same value (e.g. wait for "payment.confirmed" for THIS
  -- order id). A waiter with no correlation matches any event of that name.
  correlation_key text,
  payload jsonb,
  -- Where the event came from — 'api' (control/signal.ts), 'webhook'
  -- (Agent 2), 'trigger' (Agent 3), etc. Observability only.
  source text not null default 'api',
  -- Optional publish-side idempotency: a webhook that redelivers the same
  -- event supplies a stable key and the second insert is a no-op, so a
  -- redelivery cannot double-wake a waiter. Distinct from correlation_key,
  -- which is about matching, not deduplication.
  idempotency_key text unique,
  created_at timestamptz not null default now(),
  -- Null until the trigger daemon has routed this event to event-triggered
  -- workflows. Not touched by the `waitForEvent` wake path at all.
  dispatched_at timestamptz
);

-- Match a waiter to its event: by name, optionally narrowed by correlation.
create index events_name_idx on events (name);
create index events_name_correlation_idx on events (name, correlation_key);

-- The trigger daemon's scan: undispatched events, oldest first. Partial on
-- exactly that predicate, same reasoning as step_lease_idx (0002) — the
-- undispatched set is tiny relative to the whole (append-only) log.
create index events_undispatched_idx on events (created_at) where dispatched_at is null;

-- ---- step-side event wait (#18) ------------------------------------------
--
-- A step that calls `ctx.waitForEvent(name)` suspends into `blocked` exactly
-- like a step awaiting a child run (0004): it releases its lease, holds no
-- worker, and is invisible to both the claim path (`status = 'ready'` only)
-- and the lease reaper (`status = 'running'` only). The difference is what
-- wakes it — a matching `publishEvent`, not a child's terminal transition.
--
-- `waiting_event_name` / `waiting_event_correlation` record what the blocked
-- step is waiting for. `wakeStepsWaitingForEvent` (repositories.ts) uses
-- `status = 'blocked' and waiting_event_name = :name [and correlation]` as
-- its WHERE clause, which is also what makes the wake exactly-once: the
-- instant a step is woken it leaves `blocked`, so a second matching publish
-- can no longer touch it.
alter table step add column waiting_event_name text;
alter table step add column waiting_event_correlation text;

-- Replay delivery, mirroring `sleep_seq` (0003). A woken step re-runs from
-- the top (a JS stack cannot survive a worker restart), so it hits the same
-- `ctx.waitForEvent()` call again; `event_seq` counts how many event-waits
-- have already been served, and the re-executing context resolves the first
-- `event_seq` calls immediately instead of re-suspending. Unlike sleep, an
-- event delivers a value: `event_payloads` holds the delivered payloads in
-- seq order, and the context returns `event_payloads[seq - 1]` for an
-- already-served wait. The two move together — every wake appends one
-- payload and bumps the counter.
alter table step add column event_seq integer not null default 0;
alter table step add column event_payloads jsonb not null default '[]';

-- Which blocked steps a publish must scan. Partial on the same predicate the
-- wake UPDATE uses, so a `publishEvent` is an index lookup, not a table scan.
create index step_waiting_event_idx on step (waiting_event_name, waiting_event_correlation)
  where status = 'blocked';

-- ---- schedules: time-based run starts ------------------------------------
--
-- Populated by Agent 2 (delayed / one-shot starts) and Agent 3 (cron
-- registration); drained by Agent 3's poller via `claimDueSchedules`. This
-- migration only defines the shape and the claim/reschedule primitives — the
-- poller loop and the cron-expression maths are Agent 3's to implement.
create table schedules (
  id uuid primary key default gen_random_uuid(),
  workflow_name text not null,
  -- 'cron' repeats on `cron_expression`; 'once' fires a single time at
  -- `next_run_at` and then disables itself (markScheduleFired).
  kind text not null check (kind in ('cron', 'once')),
  cron_expression text,
  -- When the schedule is next due. The poller claims rows with
  -- `enabled and next_run_at <= now()`.
  next_run_at timestamptz not null,
  -- What to start the run with.
  input jsonb,
  namespace text not null default 'default',
  priority integer not null default 0,
  enabled boolean not null default true,
  -- Last time the poller actually fired this schedule — observability + the
  -- "don't fire a just-fired 'once' again" guard.
  last_fired_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),

  -- A cron schedule must carry an expression; a one-shot must not.
  constraint schedules_cron_expression_check
    check ((kind = 'cron') = (cron_expression is not null))
);

-- The poller's claim scan: due + enabled, oldest first. Partial on `enabled`
-- so disabled ('once' already fired, or paused) rows never enter the scan.
create index schedules_due_idx on schedules (next_run_at) where enabled;
create index schedules_workflow_name_idx on schedules (workflow_name);
