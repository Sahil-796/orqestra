-- 0004_orchestration.sql
-- Phase 4: orchestration & DAGs — step dependencies (#19), fan-out (#15),
-- fan-in (#16), conditional branching (#17), child workflows (#20).
-- Append-only — 0001/0002/0003 are never edited.
--
-- `step.depends_on text[]` (0001) already carries the DAG edges, and
-- executor.ts already flips a `pending` step to `ready` once every named
-- dep has completed by re-reading the whole run's steps under `lockRun`
-- (see engine/scheduler.ts's `newlyReadySteps`). That whole-run rescan,
-- serialized on the run row lock, is correct but becomes a bottleneck (and
-- a single point of lock contention) exactly where Phase 4's shipping bar
-- lives: a step with ten fan-in parents completing near-simultaneously
-- across ten different workers. This migration adds a second, narrower
-- readiness path — a per-row counter update that only ever touches the one
-- dependent step being satisfied — so a fan-in join can be released by
-- whichever commit happens to be last without every sibling completion
-- having to fight over the run lock.

-- Which of THIS step's `depends_on` names have already resolved (whether
-- by completing or by being skipped — see `skipped` below; the storage
-- layer doesn't judge which outcomes count as "resolved enough to
-- proceed", the caller decides what it passes here). Read by
-- `recordDependencySatisfied` (repositories.ts): every call appends one
-- name and, in the same statement, flips `pending` -> `ready` the instant
-- `depends_on` becomes a subset of `satisfied_deps`. That containment
-- check is what makes "last dependency wins" safe under concurrency: two
-- concurrent calls for the same step each run as their own single-statement
-- transaction, so Postgres's row-level lock on the `step` row serializes
-- them (whichever commits second is the only one that can observe the full
-- set and therefore the only one whose UPDATE actually changes `status`) —
-- no deadlock, because neither ever waits on more than the one row it's
-- already updating.
alter table step add column satisfied_deps text[] not null default '{}';

-- Two new terminal-ish states, both anticipated by 0001's decision to keep
-- statuses as `text` + `CHECK` rather than a native `enum` specifically so
-- new values wouldn't need an `ALTER TYPE`:
--
--   'skipped' — feature #17 (conditional branching). A step whose branch
--   condition was not taken never runs; it is not a failure (nothing went
--   wrong) and not `cancelled` (nothing asked the run to stop), so it needs
--   its own terminal value. `skipStep` (repositories.ts) is the only writer.
--
--   'blocked' — feature #20 (child workflows). A step that spawned a child
--   run and is durably waiting on it releases its worker exactly the way a
--   sleeping step does (see 0003's `sleepStep`): it cannot stay `running`
--   and hold a lease for however long the child takes, because that pins a
--   worker slot for a child run that may itself sleep, retry, or fan out.
--   `blockStepOnChildRun` moves it here and clears its lease;
--   `resolveBlockedStepForChildRun` flips it back to `ready` once the
--   child reaches a terminal status, and the step function replays from
--   the top and reads the child's outcome off `run.parent_step_id`'s
--   linkage — the same replay contract `sleep_seq` already establishes for
--   sleep, just keyed on a child run's terminal state instead of a clock.
alter table step drop constraint step_status_check;
alter table step add constraint step_status_check
  check (status in ('pending', 'ready', 'running', 'completed', 'failed', 'cancelled', 'skipped', 'blocked'));

-- Why a step was skipped (which branch condition it lost to, or any other
-- caller-supplied reason) — observability only, never read by the claim
-- path or any readiness check. Mirrors `error`'s role for `failed`: a
-- terminal state's row should be able to explain itself without a join
-- into `history`.
alter table step add column skip_reason text;

-- Which child run a `blocked` step is waiting on. Set together with the
-- transition to `blocked` and cleared on release, so "is this step still
-- genuinely waiting, and on what" is answerable from the row alone. Not
-- lease-fenced the way `sleeping_until` roughly is either, but unlike
-- `sleeping_until` this one IS load-bearing for the claim/release path:
-- `resolveBlockedStepForChildRun` uses it (plus `status = 'blocked'`) as
-- its WHERE clause.
alter table step add column awaited_child_run_id uuid references run (id);

-- child-run linkage lives on `run`, pointing at the parent run and — the
-- half executor.ts's existing `run` shape has no room for — the specific
-- parent step that is awaiting this child's outcome. Both nullable: most
-- runs are not child runs. `createRun` (repositories.ts) grows two
-- optional fields to set these atomically at creation; nothing about
-- idempotent run creation (0001/Phase1) changes for callers that don't
-- pass them.
alter table run add column parent_run_id uuid references run (id);
alter table run add column parent_step_id uuid references step (id);

-- `getChildRuns` scans by `parent_run_id`; a plain index (not partial) is
-- right here because, unlike `run_cancel_requested_idx`'s vanishingly rare
-- predicate, "does this run have children" is looked up by a stable,
-- reusable key rather than a transient flag.
create index run_parent_run_id_idx on run (parent_run_id) where parent_run_id is not null;

-- `resolveBlockedStepForChildRun` scans for the one `blocked` step (if any)
-- waiting on a given child run id — a partial index on exactly that
-- predicate, same reasoning as `step_lease_idx` (0002) and
-- `run_cancel_requested_idx` (0003): the matching set is tiny relative to
-- the whole step table, so this keeps the lookup an index scan instead of
-- degrading into a sequential one as the table grows.
create index step_awaited_child_run_id_idx on step (awaited_child_run_id)
  where status = 'blocked';
