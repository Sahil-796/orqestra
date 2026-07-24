-- 0001_init.sql
-- Core data model for orqestra (build-plan.html, section 03).
--
-- Status enums (kept as text + check constraint rather than native `enum`
-- types so future values can be added without an `ALTER TYPE`):
--   run.status:  queued | running | completed | failed | cancelled
--   step.status: pending | ready | running | completed | failed | cancelled
--
-- jsonb is used for every "shape varies" column: dag/input/output/result/
-- error/payload/data/snapshot.

create extension if not exists pgcrypto;

-- a registered workflow definition + version (versioning matters for
-- long-running runs: a run started on v1 must finish on v1's logic)
create table workflow (
  id uuid primary key default gen_random_uuid(),
  name text not null,
  version integer not null default 1,
  dag jsonb not null,
  created_at timestamptz not null default now(),

  unique (name, version)
);

create index workflow_name_idx on workflow (name);

-- one execution of a workflow
create table run (
  id uuid primary key default gen_random_uuid(),
  workflow_id uuid not null references workflow (id),
  namespace text not null default 'default',
  status text not null default 'queued'
    check (status in ('queued', 'running', 'completed', 'failed', 'cancelled')),
  priority integer not null default 0,
  input jsonb,
  output jsonb,
  idempotency_key text unique,
  created_at timestamptz not null default now(),
  started_at timestamptz,
  finished_at timestamptz
);

create index run_workflow_id_idx on run (workflow_id);
create index run_namespace_status_idx on run (namespace, status);

-- the queue AND the durable step log, unified
create table step (
  id uuid primary key default gen_random_uuid(),
  run_id uuid not null references run (id),
  name text not null,
  status text not null default 'pending'
    check (status in ('pending', 'ready', 'running', 'completed', 'failed', 'cancelled')),
  attempt integer not null default 0,
  max_attempts integer not null default 1,
  result jsonb,
  error jsonb,
  depends_on text[] not null default '{}',

  -- sleep / delay / backoff
  run_after timestamptz not null default now(),

  -- leasing
  lease_owner text,
  lease_expires_at timestamptz,

  timeout_ms integer,
  priority integer not null default 0,

  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index step_run_id_idx on step (run_id);

-- the claim query every worker runs, in a tx:
--   SELECT * FROM step
--   WHERE status = 'ready' AND run_after <= now()
--     AND (lease_expires_at IS NULL OR lease_expires_at < now())
--   ORDER BY priority DESC, run_after
--   FOR UPDATE SKIP LOCKED LIMIT 1;
create index step_claim_idx on step (priority desc, run_after)
  where status = 'ready';

-- things a run is blocked on: an external signal / event
create table signal_wait (
  id uuid primary key default gen_random_uuid(),
  run_id uuid not null references run (id),
  event_key text not null,
  satisfied_at timestamptz,
  created_at timestamptz not null default now()
);

create index signal_wait_run_id_idx on signal_wait (run_id);
create index signal_wait_event_key_idx on signal_wait (event_key)
  where satisfied_at is null;

create table event (
  id uuid primary key default gen_random_uuid(),
  key text not null,
  payload jsonb,
  received_at timestamptz not null default now()
);

create index event_key_idx on event (key);

-- append-only observability spine: timeline + logs
create table history (
  id uuid primary key default gen_random_uuid(),
  run_id uuid not null references run (id),
  step_id uuid references step (id),
  type text not null,
  data jsonb,
  at timestamptz not null default now()
);

create index history_run_id_idx on history (run_id, at);

create table dead_letter (
  id uuid primary key default gen_random_uuid(),
  run_id uuid not null references run (id),
  reason text not null,
  snapshot jsonb,
  at timestamptz not null default now()
);

create index dead_letter_run_id_idx on dead_letter (run_id);
