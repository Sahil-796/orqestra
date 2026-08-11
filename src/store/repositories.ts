// Typed query functions — the only layer (besides client.ts/migrate.ts)
// that talks to Postgres. engine/ and define/ call through here, never
// through `postgres` directly, so storage can be swapped later.

import type postgres from 'postgres'
import type { Db } from './client.ts'
import { withTransaction } from './client.ts'
import type { RunStatus, SerializedError, StepStatus, WorkflowDefinition } from '../types.ts'
import { encodeResult, ok, type Result } from '../types.ts'

function toJson(value: unknown): postgres.JSONValue {
  return value as postgres.JSONValue
}

export interface WorkflowRow {
  id: string
  name: string
  version: number
  dag: WorkflowDefinition
  created_at: Date
}

export interface RunRow {
  id: string
  workflow_id: string
  namespace: string
  status: RunStatus
  priority: number
  input: unknown
  output: unknown
  idempotency_key: string | null
  created_at: Date
  started_at: Date | null
  finished_at: Date | null
  cancel_requested_at: Date | null
  parent_run_id: string | null
  parent_step_id: string | null
}

export interface StepRow {
  id: string
  run_id: string
  name: string
  status: StepStatus
  attempt: number
  max_attempts: number
  result: unknown
  error: unknown
  depends_on: string[]
  run_after: Date
  lease_owner: string | null
  lease_expires_at: Date | null
  timeout_ms: number | null
  priority: number
  reclaim_count: number
  sleep_seq: number
  sleeping_until: Date | null
  created_at: Date
  updated_at: Date
  satisfied_deps: string[]
  skip_reason: string | null
  awaited_child_run_id: string | null
}

export interface HistoryRow {
  id: string
  run_id: string
  step_id: string | null
  type: string
  data: unknown
  at: Date
}

// ---- workflow --------------------------------------------------------

export async function insertWorkflow(
  sql: Db,
  input: { name: string; version?: number; dag: WorkflowDefinition }
): Promise<WorkflowRow> {
  const version = input.version ?? 1
  const rows = await sql<WorkflowRow[]>`
    insert into workflow (name, version, dag)
    values (${input.name}, ${version}, ${sql.json(toJson(input.dag))})
    returning *
  `
  const row = rows[0]
  if (!row) throw new Error(`insertWorkflow: insert returned no row for "${input.name}"`)
  return row
}

export async function getWorkflowById(sql: Db, id: string): Promise<WorkflowRow | undefined> {
  const rows = await sql<WorkflowRow[]>`select * from workflow where id = ${id}`
  return rows[0]
}

export async function getWorkflowByName(
  sql: Db,
  name: string,
  version?: number
): Promise<WorkflowRow | undefined> {
  if (version !== undefined) {
    const rows = await sql<WorkflowRow[]>`
      select * from workflow where name = ${name} and version = ${version}
    `
    return rows[0]
  }
  const rows = await sql<WorkflowRow[]>`
    select * from workflow where name = ${name} order by version desc limit 1
  `
  return rows[0]
}

// ---- run ---------------------------------------------------------------

// Idempotent when `idempotencyKey` is provided: a second createRun with the
// same key does not insert a second row. `created` tells the caller whether
// this call actually inserted the row (and therefore whether steps still
// need to be materialized) or found a pre-existing run for that key.
//
// `parentRunId`/`parentStepId` (Phase 4, #20 child workflows) are additive
// and optional: pass both together when this run is a child spawned by a
// step of another run, so `run.parent_run_id`/`run.parent_step_id` link it
// back. Every existing caller that omits them behaves exactly as before —
// both columns default to null.
export async function createRun(
  sql: Db,
  input: {
    workflowId: string
    namespace?: string
    priority?: number
    input?: unknown
    idempotencyKey?: string
    parentRunId?: string
    parentStepId?: string
  }
): Promise<{ run: RunRow; created: boolean }> {
  if (input.idempotencyKey === undefined) {
    const rows = await sql<RunRow[]>`
      insert into run (workflow_id, namespace, priority, input, idempotency_key, parent_run_id, parent_step_id)
      values (
        ${input.workflowId},
        ${input.namespace ?? 'default'},
        ${input.priority ?? 0},
        ${input.input === undefined ? null : sql.json(toJson(input.input))},
        ${null},
        ${input.parentRunId ?? null},
        ${input.parentStepId ?? null}
      )
      returning *
    `
    const row = rows[0]
    if (!row) throw new Error('createRun: insert returned no row')
    return { run: row, created: true }
  }

  const inserted = await sql<RunRow[]>`
    insert into run (workflow_id, namespace, priority, input, idempotency_key, parent_run_id, parent_step_id)
    values (
      ${input.workflowId},
      ${input.namespace ?? 'default'},
      ${input.priority ?? 0},
      ${input.input === undefined ? null : sql.json(toJson(input.input))},
      ${input.idempotencyKey},
      ${input.parentRunId ?? null},
      ${input.parentStepId ?? null}
    )
    on conflict (idempotency_key) do nothing
    returning *
  `
  const insertedRow = inserted[0]
  if (insertedRow) return { run: insertedRow, created: true }

  const existing = await sql<RunRow[]>`
    select * from run where idempotency_key = ${input.idempotencyKey}
  `
  const existingRow = existing[0]
  if (!existingRow) throw new Error('createRun: idempotency conflict but no existing row found')
  return { run: existingRow, created: false }
}

export async function getRun(sql: Db, id: string): Promise<RunRow | undefined> {
  const rows = await sql<RunRow[]>`select * from run where id = ${id}`
  return rows[0]
}

// The serialization point for advanceRun's readiness computation (see the
// call site in engine/executor.ts for why this is load-bearing, not
// paranoia). Locks the run row for the rest of the caller's transaction —
// harmless as a bare, non-transactional call too (Phase 1's inline
// executeRun loop uses advanceRun this way), since a `for update` outside
// an explicit transaction just acquires and immediately releases the lock
// within its own implicit one-statement transaction.
export async function lockRun(sql: Db, runId: string): Promise<RunRow | undefined> {
  const rows = await sql<RunRow[]>`select * from run where id = ${runId} for update`
  return rows[0]
}

export async function updateRunStatus(
  sql: Db,
  runId: string,
  status: RunStatus,
  opts: { output?: unknown; startedAt?: Date; finishedAt?: Date } = {}
): Promise<RunRow> {
  const rows = await sql<RunRow[]>`
    update run set
      status = ${status},
      output = coalesce(${opts.output === undefined ? null : sql.json(toJson(opts.output))}, output),
      started_at = coalesce(${opts.startedAt ?? null}, started_at),
      finished_at = coalesce(${opts.finishedAt ?? null}, finished_at)
    where id = ${runId}
    returning *
  `
  const row = rows[0]
  if (!row) throw new Error(`updateRunStatus: no run found for id "${runId}"`)
  return row
}

// Flips a run from `queued` to `running`, but only if it's still `queued` —
// the WHERE clause makes this "first step of the run to be picked up wins"
// safe under concurrent workers: whichever worker's UPDATE actually matches
// a row gets one back and is the one that should log `run.started`; every
// other worker racing the same run gets undefined and does nothing further,
// so the history stream never gets a duplicate `run.started` entry.
export async function markRunStarted(sql: Db, runId: string): Promise<RunRow | undefined> {
  const rows = await sql<RunRow[]>`
    update run set status = 'running', started_at = now()
    where id = ${runId} and status = 'queued'
    returning *
  `
  return rows[0]
}

// Runs a resumeAll pass should look at: not yet in a terminal state.
export async function getIncompleteRuns(sql: Db): Promise<RunRow[]> {
  return sql<RunRow[]>`
    select * from run where status in ('queued', 'running')
  `
}

// ---- child workflows (#20) -------------------------------------------------
//
// A child run is an ordinary `run` row (created via `createRun` with
// `parentRunId`/`parentStepId` set) plus a step, somewhere in the parent
// run, that is durably waiting on it. "Durably" is the operative word: the
// parent step does not hold its worker lease for however long the child
// takes (see 0004_orchestration.sql's rationale for `status = 'blocked'`) —
// it releases back to storage the same way a sleeping step does, and is
// woken by `resolveBlockedStepForChildRun` once the child finishes.

// Every run spawned as a child of `parentRunId`, in creation order —
// observability and the "await all children" shape both want this.
export async function getChildRuns(sql: Db, parentRunId: string): Promise<RunRow[]> {
  return sql<RunRow[]>`
    select * from run where parent_run_id = ${parentRunId} order by created_at
  `
}

// The step-side half of spawning a child run: park the step that spawned it
// in `blocked` and record which child it's waiting on, releasing the lease
// exactly like `sleepStep` (0003) does for a sleep — same fencing (only the
// current lease holder may do this), same "give the worker back" shape, so
// a long-running child (which may itself sleep, retry, or fan out) never
// pins a worker slot for its whole lifetime.
export async function blockStepOnChildRun(
  sql: Db,
  args: { stepId: string; workerId: string; childRunId: string }
): Promise<StepRow | undefined> {
  const rows = await sql<StepRow[]>`
    update step set
      status = 'blocked',
      awaited_child_run_id = ${args.childRunId},
      lease_owner = null,
      lease_expires_at = null,
      updated_at = now()
    where id = ${args.stepId} and status = 'running' and lease_owner = ${args.workerId}
    returning *
  `
  return rows[0]
}

// The wake side: called once a child run has reached a terminal status.
// Finds the `blocked` step (if any) waiting on exactly this child — via
// `awaited_child_run_id`, backed by `step_awaited_child_run_id_idx` — and
// releases it back to `ready`, clearing the link. No lease to re-check
// here: nobody holds one while a step is `blocked`, the same way nobody
// holds one while a step is asleep. The step function replays from the top
// on its next claim (the established replay contract — see 0003's
// `sleepStep`) and reads the child's outcome via `getRun(childRunId)` or
// `getChildRuns(parentRunId)`, exactly as a woken sleep re-reads
// `sleep_seq` to know it's already served its sleep. The `exists` guard
// means calling this before the child is actually terminal is a safe no-op,
// not a premature release.
export async function resolveBlockedStepForChildRun(
  sql: Db,
  childRunId: string
): Promise<StepRow | undefined> {
  const rows = await sql<StepRow[]>`
    update step set
      status = 'ready',
      awaited_child_run_id = null,
      updated_at = now()
    where awaited_child_run_id = ${childRunId}
      and status = 'blocked'
      and exists (
        select 1 from run
        where id = ${childRunId} and status in ('completed', 'failed', 'cancelled')
      )
    returning *
  `
  return rows[0]
}

// ---- step ----------------------------------------------------------------
//
// Step `result` is persisted as an encoded `Result<T>` (`{ ok: true, value }`
// via `encodeResult(ok(value))`), never the raw value — this lets a reader
// distinguish "step completed with value X" from any other shape, and keeps
// the door open for a future `{ ok: false, error }` variant sharing the same
// column. `error` is a `SerializedError` (`serializeError(e)`), independent
// of `result`, so a failed step's failure reason survives the jsonb round
// trip (raw `Error` objects are not JSON-safe).

export interface NewStep {
  name: string
  dependsOn: string[]
  maxAttempts: number
  timeoutMs?: number
  priority: number
  status: StepStatus
}

export async function insertSteps(sql: Db, runId: string, steps: NewStep[]): Promise<StepRow[]> {
  if (steps.length === 0) return []
  const rows = steps.map((step) => ({
    run_id: runId,
    name: step.name,
    status: step.status,
    max_attempts: step.maxAttempts,
    timeout_ms: step.timeoutMs ?? null,
    priority: step.priority,
    depends_on: step.dependsOn,
  }))
  return sql<StepRow[]>`
    insert into step ${sql(rows)}
    returning *
  `
}

export async function getStepsByRun(sql: Db, runId: string): Promise<StepRow[]> {
  return sql<StepRow[]>`
    select * from step where run_id = ${runId} order by created_at, name
  `
}

export async function markStepRunning(sql: Db, stepId: string): Promise<StepRow> {
  const rows = await sql<StepRow[]>`
    update step set status = 'running', attempt = attempt + 1, updated_at = now()
    where id = ${stepId}
    returning *
  `
  const row = rows[0]
  if (!row) throw new Error(`markStepRunning: no step found for id "${stepId}"`)
  return row
}

export async function markStepReady(sql: Db, stepId: string): Promise<StepRow> {
  const rows = await sql<StepRow[]>`
    update step set status = 'ready', updated_at = now()
    where id = ${stepId}
    returning *
  `
  const row = rows[0]
  if (!row) throw new Error(`markStepReady: no step found for id "${stepId}"`)
  return row
}

export async function completeStep(sql: Db, stepId: string, result: unknown): Promise<StepRow> {
  const encoded = encodeResult(ok(result) as Result<unknown>)
  const rows = await sql<StepRow[]>`
    update step set status = 'completed', result = ${sql.json(toJson(encoded))}, updated_at = now()
    where id = ${stepId}
    returning *
  `
  const row = rows[0]
  if (!row) throw new Error(`completeStep: no step found for id "${stepId}"`)
  return row
}

export async function failStep(
  sql: Db,
  stepId: string,
  error: SerializedError
): Promise<StepRow> {
  const rows = await sql<StepRow[]>`
    update step set status = 'failed', error = ${sql.json(toJson(error))}, updated_at = now()
    where id = ${stepId}
    returning *
  `
  const row = rows[0]
  if (!row) throw new Error(`failStep: no step found for id "${stepId}"`)
  return row
}

// Crash recovery: a step left `running` when the process died is
// interrupted, not in progress — flip it back to `ready` so the driver
// loop picks it up again (single process, no leasing yet in Phase 1).
export async function resetRunningSteps(sql: Db, runId: string): Promise<void> {
  await sql`
    update step set status = 'ready', updated_at = now()
    where run_id = ${runId} and status = 'running'
  `
}

// ---- DAG: dependencies, fan-in, conditional branching (#15/#16/#17/#19) --
//
// engine/scheduler.ts's `newlyReadySteps` (Phase 1) already computes
// readiness correctly by re-reading a run's whole step set under `lockRun`
// — that stays the default path and this file does not change it. What's
// added here is a second, narrower primitive for the case that rescan
// approach makes expensive: a step with many fan-in parents, each
// completing in its own worker's transaction. `recordDependencySatisfied`
// lets each parent's completion touch only the one dependent row, and is
// safe under concurrency without the run-level lock (see the migration
// comment on `satisfied_deps` for why).

// Every sibling step, in the same run, that `stepId` names in its
// `depends_on` — i.e. its dependency set, resolved to full rows so a caller
// can read their current status. Returns them in `depends_on` order isn't
// guaranteed (the join has no ordering guarantee across dependency names),
// so callers that care about order should re-sort by name themselves.
export async function getDependencySteps(sql: Db, stepId: string): Promise<StepRow[]> {
  return sql<StepRow[]>`
    select dep.* from step s
    join step dep on dep.run_id = s.run_id and dep.name = any(s.depends_on)
    where s.id = ${stepId}
  `
}

// Record that one of `stepId`'s named dependencies (`depName`) has
// resolved — because it completed, or because it was skipped and the
// caller has decided a skip counts as "satisfied" for this edge; this
// function doesn't judge why, it just tracks which names have resolved and
// releases the step the instant every name in `depends_on` is among them.
//
// One UPDATE statement, not a chain of two CTEs writing the same table:
// Postgres data-modifying CTEs all execute against the snapshot taken at
// the start of the command, so a second CTE cannot see a first CTE's
// write to the very same row within one statement (empirically: it
// matches zero rows, silently, rather than erroring — this was caught by
// this file's own tests, not by the docs). Computing the new
// `satisfied_deps` once via a scalar subquery and reusing it for both the
// SET and the readiness CASE keeps this a single write to a single row,
// which is exactly what makes it race-safe: the whole
// read-append-maybe-release sequence is one row-level lock acquired and
// released by Postgres itself, no application-level read-then-write, so
// there is no window for two concurrent callers to both observe "not yet
// satisfied" and neither one flip the step to `ready` (the fan-in bug this
// exists to rule out), and no deadlock (each call only ever touches the
// one row it's updating, for the lifetime of this one statement).
//
// Returns the step's current row whether or not this call was the one that
// released it — check `.status === 'ready'` to tell those apart. Returns
// undefined if the step wasn't `pending` (already released by an earlier
// call, or not a dependency-gated step at all) — a safe no-op, not an
// error, since a duplicate delivery of the same dependency's resolution
// should not be able to do anything.
export async function recordDependencySatisfied(
  sql: Db,
  stepId: string,
  depName: string
): Promise<StepRow | undefined> {
  const rows = await sql<StepRow[]>`
    update step
    set
      satisfied_deps = (
        select array_agg(distinct d) from unnest(satisfied_deps || array[${depName}]::text[]) as d
      ),
      status = case
        when depends_on <@ (
          select array_agg(distinct d) from unnest(satisfied_deps || array[${depName}]::text[]) as d
        ) then 'ready'
        else status
      end,
      updated_at = now()
    where id = ${stepId} and status = 'pending'
    returning *
  `
  return rows[0]
}

// Feature #17: mark a step as never going to run because the conditional
// branch it belongs to was not taken. Terminal, but distinct from
// `failed`/`cancelled` (see 0004_orchestration.sql) — nothing went wrong,
// nothing was asked to stop, the workflow's own logic decided this path.
// Allowed from `pending` or `ready` (a step can be skipped either before or
// after its dependencies resolved, depending on when the branch decision
// itself becomes known) but not from `running`/terminal states — those
// need their own resolution path, not to be silently overwritten.
export async function skipStep(
  sql: Db,
  stepId: string,
  reason?: string
): Promise<StepRow | undefined> {
  const rows = await sql<StepRow[]>`
    update step set
      status = 'skipped',
      skip_reason = ${reason ?? null},
      updated_at = now()
    where id = ${stepId} and status in ('pending', 'ready')
    returning *
  `
  return rows[0]
}

// ---- queue: claim / lease / retry -----------------------------------------
//
// Phase 2 turns the step table into a claimable queue. Everything here is
// still just SQL behind the storage boundary — the *policy* (backoff maths,
// DAG readiness, the poison-pill ceiling decision) lives in engine/ and
// queue/claim.ts + queue/lease.ts, which call these functions rather than
// embedding SQL of their own.

export interface ClaimStepOptions {
  workerId: string
  leaseTtlMs: number
  namespace?: string
}

// The queue claim, straight from the build plan: SKIP LOCKED lets any
// number of workers race this query concurrently — each either walks away
// with a distinct row or finds the queue empty, none of them ever block
// waiting on a row another worker is about to take. The select and the
// flip-to-running update run in one transaction so the row stays locked
// (FOR UPDATE) from "we found it" to "we own it"; a second worker's SELECT
// simply skips it for the whole transaction, never sees it as claimable in
// between. `attempt` is incremented here, at claim time, because it counts
// real execution attempts — retryStep (below) only resets state, it never
// touches attempt.
export async function claimNextStep(
  sql: Db,
  options: ClaimStepOptions
): Promise<StepRow | undefined> {
  return withTransaction(sql, async (tx) => {
    const namespaceFilter = options.namespace ? tx`and r.namespace = ${options.namespace}` : tx``
    const candidates = await tx<StepRow[]>`
      select s.* from step s
      join run r on r.id = s.run_id
      where s.status = 'ready'
        and s.run_after <= now()
        and (s.lease_expires_at is null or s.lease_expires_at < now())
        and r.status in ('queued', 'running')
        ${namespaceFilter}
      order by s.priority desc, s.run_after
      for update of s skip locked
      limit 1
    `
    const candidate = candidates[0]
    if (!candidate) return undefined

    const claimed = await tx<StepRow[]>`
      update step set
        status = 'running',
        attempt = attempt + 1,
        lease_owner = ${options.workerId},
        lease_expires_at = now() + (${options.leaseTtlMs} * interval '1 millisecond'),
        updated_at = now()
      where id = ${candidate.id}
      returning *
    `
    const row = claimed[0]
    if (!row) throw new Error(`claimNextStep: claimed row "${candidate.id}" vanished before update`)
    return row
  })
}

// Fencing check lives in the WHERE clause, not a separate read-then-write:
// the update only takes effect if the row is still `running` under this
// exact `workerId`, so a worker whose lease was already reclaimed by
// someone else (see reclaimStep) gets back zero rows here instead of
// silently re-extending a lease it no longer owns.
export async function heartbeatStep(
  sql: Db,
  stepId: string,
  workerId: string,
  leaseTtlMs: number
): Promise<StepRow | undefined> {
  const rows = await sql<StepRow[]>`
    update step set
      lease_expires_at = now() + (${leaseTtlMs} * interval '1 millisecond'),
      updated_at = now()
    where id = ${stepId} and status = 'running' and lease_owner = ${workerId}
    returning *
  `
  return rows[0]
}

// Clears lease ownership without touching status — deliberately decoupled
// from completeStep/failStep/retryStep (which already carry their own
// status transitions) so it composes into whichever transaction the caller
// is already using to persist a step's outcome, rather than forcing a
// second round trip.
export async function releaseStep(sql: Db, stepId: string): Promise<void> {
  await sql`
    update step set lease_owner = null, lease_expires_at = null, updated_at = now()
    where id = ${stepId}
  `
}

// What the reclaim sweep scans: every `running` step whose lease has
// expired, i.e. its worker went silent (crash, network partition, GC
// pause past the TTL) without heartbeating or committing an outcome.
// Backed by step_lease_idx (0002) — a partial index on this exact
// predicate, so the sweep doesn't degrade into a full table scan as the
// step table grows.
export async function findExpiredLeases(sql: Db): Promise<StepRow[]> {
  return sql<StepRow[]>`
    select * from step where status = 'running' and lease_expires_at < now()
  `
}

// Sends an expired-lease step back to the queue. The WHERE clause re-checks
// `status = 'running' and lease_expires_at < now()` at update time (not
// just at scan time in findExpiredLeases) so a lease that a heartbeat
// rescued in the gap between the scan and this write is left alone instead
// of being yanked out from under a worker that's actually still alive —
// returns undefined in that case rather than a row, which the caller
// treats as "not actually stuck, skip it."
export async function reclaimStep(sql: Db, stepId: string): Promise<StepRow | undefined> {
  const rows = await sql<StepRow[]>`
    update step set
      status = 'ready',
      lease_owner = null,
      lease_expires_at = null,
      reclaim_count = reclaim_count + 1,
      updated_at = now()
    where id = ${stepId} and status = 'running' and lease_expires_at < now()
    returning *
  `
  return rows[0]
}

// The poison-pill terminus: a step that has blown through the reclaim
// ceiling is failed outright (not sent back to `ready` again) so it stops
// eating worker capacity. Kept separate from failStep (which is Phase 1's
// generic "the step function threw" path) because this failure comes from
// leasing, not from the step's own code, and it also needs to bump
// reclaim_count for the historical record of how many times this step
// killed a worker.
export async function poisonStep(
  sql: Db,
  stepId: string,
  error: SerializedError
): Promise<StepRow> {
  const rows = await sql<StepRow[]>`
    update step set
      status = 'failed',
      error = ${sql.json(toJson(error))},
      reclaim_count = reclaim_count + 1,
      lease_owner = null,
      lease_expires_at = null,
      updated_at = now()
    where id = ${stepId}
    returning *
  `
  const row = rows[0]
  if (!row) throw new Error(`poisonStep: no step found for id "${stepId}"`)
  return row
}

// The fencing check a worker runs INSIDE the transaction that will commit a
// step's outcome, right before writing anything. `FOR UPDATE` locks the row
// for the rest of the transaction, and the WHERE clause only matches if
// `workerId` still holds the lease and the step is still `running` — if a
// reclaim sweep (or another worker) already took the step back, this
// returns undefined and the caller must commit nothing: double-committing
// an outcome for a step two workers both think they own would corrupt the
// step log (e.g. two `step.completed` history rows, or a stale worker's
// stale result clobbering a fresher one).
export async function lockStepIfOwner(
  sql: Db,
  stepId: string,
  workerId: string
): Promise<StepRow | undefined> {
  const rows = await sql<StepRow[]>`
    select * from step
    where id = ${stepId} and lease_owner = ${workerId} and status = 'running'
    for update
  `
  return rows[0]
}

// Persists a retry decision computed by engine/retry.ts: back to `ready`,
// due at `runAfter` (the backoff delay), with the failure recorded and the
// lease cleared. `attempt` is untouched — it already moved at claim time,
// and this row will pick up its next attempt number the next time it's
// claimed, not now.
export async function retryStep(
  sql: Db,
  stepId: string,
  error: SerializedError,
  runAfter: Date
): Promise<StepRow> {
  const rows = await sql<StepRow[]>`
    update step set
      status = 'ready',
      error = ${sql.json(toJson(error))},
      run_after = ${runAfter},
      lease_owner = null,
      lease_expires_at = null,
      updated_at = now()
    where id = ${stepId}
    returning *
  `
  const row = rows[0]
  if (!row) throw new Error(`retryStep: no step found for id "${stepId}"`)
  return row
}

// Called when a run is being failed/cancelled so its remaining unclaimed
// steps stop being claimable — otherwise a worker could pick one up after
// the run is already decided, doing wasted (or worse, order-dependent)
// work on a run that's over. Only pending/ready are touched; running steps
// are left for their worker (or the reclaim sweep) to resolve on its own.
export async function cancelPendingSteps(sql: Db, runId: string): Promise<StepRow[]> {
  return sql<StepRow[]>`
    update step set status = 'cancelled', updated_at = now()
    where run_id = ${runId} and status in ('pending', 'ready')
    returning *
  `
}

// Small aggregate used by tests/observability to assert on run shape
// without pulling every step row across the wire.
export async function countStepsByStatus(
  sql: Db,
  runId: string
): Promise<Record<StepStatus, number>> {
  const rows = await sql<{ status: StepStatus; count: string }[]>`
    select status, count(*)::text as count from step where run_id = ${runId} group by status
  `
  const counts = {} as Record<StepStatus, number>
  for (const row of rows) counts[row.status] = Number(row.count)
  return counts
}

// ---- execution control: sleep / cancellation -------------------------------
//
// Phase 3. Two mechanisms that both refuse to hold a worker hostage:
// `ctx.sleep()` gives the worker back for the duration of the sleep instead
// of blocking it, and cancellation is a *request* another process observes
// rather than a kill it can't survive. Both are pure storage here — the
// duration parsing, the wake bookkeeping and the "where is it safe to
// observe a cancel" policy live above this boundary.

// Suspend a running step for a sleep, fenced on lease ownership exactly like
// commitOutcome's lockStepIfOwner path: the WHERE clause only matches while
// this `workerId` still owns a `running` lease, so a worker whose lease was
// reclaimed underneath it (see reclaimStep) gets undefined back and must
// write nothing — otherwise a zombie worker could put a step to sleep that
// its new owner is already executing.
//
// The step goes back to `ready` with `run_after = wakeAt`, which is all the
// scheduling there is: claimNextStep's existing `run_after <= now()` gate
// makes the row invisible to every worker until the sleep is up, then
// ordinarily claimable. No timer, no in-memory state, nothing to lose in a
// crash — the wake time is a durable column, so a sleep survives every
// worker in the fleet dying and restarting.
//
// Two subtleties in the SET list:
//   - `sleep_seq + 1` records that this sleep has now been served. The step
//     function re-runs from the top on wake (we cannot freeze a JS stack
//     across a restart), so without the counter it would suspend on the same
//     sleep() call forever; the re-executing context skips the first
//     `sleep_seq` calls.
//   - `attempt - 1` gives back the attempt that claiming consumed. `attempt`
//     is incremented at claim time and counted against `max_attempts`, but a
//     sleep is not a failed attempt — a step with maxAttempts: 1 that sleeps
//     twice must still be allowed to finish, not die of "out of retries" on
//     its first wake. `greatest(..., 0)` keeps the column non-negative if it
//     is ever called on a step claimed by some path that didn't increment.
export async function sleepStep(
  sql: Db,
  args: { stepId: string; workerId: string; wakeAt: Date }
): Promise<StepRow | undefined> {
  const rows = await sql<StepRow[]>`
    update step set
      status = 'ready',
      run_after = ${args.wakeAt},
      sleeping_until = ${args.wakeAt},
      sleep_seq = sleep_seq + 1,
      attempt = greatest(attempt - 1, 0),
      lease_owner = null,
      lease_expires_at = null,
      updated_at = now()
    where id = ${args.stepId} and status = 'running' and lease_owner = ${args.workerId}
    returning *
  `
  return rows[0]
}

// Clear the observability marker once a woken step is actually running
// again. Unfenced and status-blind on purpose: this column is never read by
// the claim path, so the worst a stale write can do is make a dashboard
// briefly wrong, and demanding a lease here would mean a reclaimed step
// stayed marked "asleep" while its new owner ran it.
export async function clearSleepMarker(sql: Db, stepId: string): Promise<void> {
  await sql`
    update step set sleeping_until = null, updated_at = now()
    where id = ${stepId} and sleeping_until is not null
  `
}

// Record intent to cancel. The WHERE clause is the whole no-op story: a run
// that already finished has nothing left to cancel, and a second request
// must not move the timestamp (the first ask is the one worth reporting
// latency against), so both cases match zero rows and return undefined —
// callers use that to distinguish "cancellation accepted" from "nothing to
// do" without a read-then-write race.
export async function requestRunCancellation(
  sql: Db,
  runId: string
): Promise<RunRow | undefined> {
  const rows = await sql<RunRow[]>`
    update run set cancel_requested_at = now()
    where id = ${runId}
      and status in ('queued', 'running')
      and cancel_requested_at is null
    returning *
  `
  return rows[0]
}

// Polled by workers between steps, so it returns a single boolean computed
// in Postgres rather than dragging the whole run row (input/output jsonb
// included) across the wire on every tick.
export async function isCancellationRequested(sql: Db, runId: string): Promise<boolean> {
  const rows = await sql<{ requested: boolean }[]>`
    select exists (
      select 1 from run where id = ${runId} and cancel_requested_at is not null
    ) as requested
  `
  return rows[0]?.requested ?? false
}

// The terminal half of cancellation, run once no step of this run is still
// in flight. In one transaction so a reader never sees the run already
// 'cancelled' while its steps are still claimable:
//   1. lock the run (`for update`) — the same serialization point advanceRun
//      uses, so this can't interleave with a concurrent readiness pass that
//      would flip freshly-cancelled steps back to `ready`;
//   2. flip pending/ready steps to 'cancelled' so nothing else gets claimed;
//   3. flip the run itself.
// `running` steps are deliberately left alone. The worker holding one owns
// its outcome and will finalize it cooperatively (cancelRunningStep); a
// write from here would be exactly the unfenced double-write that Phase 2's
// commit fencing exists to make impossible. Returns run: undefined without
// touching anything if the run is already terminal.
export async function finalizeCancelledRun(
  sql: Db,
  runId: string
): Promise<{ run: RunRow | undefined; cancelledSteps: StepRow[] }> {
  return withTransaction(sql, async (tx) => {
    const locked = await tx<RunRow[]>`
      select * from run where id = ${runId} for update
    `
    const current = locked[0]
    if (!current || (current.status !== 'queued' && current.status !== 'running')) {
      return { run: undefined, cancelledSteps: [] }
    }

    const cancelledSteps = await tx<StepRow[]>`
      update step set status = 'cancelled', updated_at = now()
      where run_id = ${runId} and status in ('pending', 'ready')
      returning *
    `

    const runs = await tx<RunRow[]>`
      update run set status = 'cancelled', finished_at = now()
      where id = ${runId}
      returning *
    `
    const run = runs[0]
    if (!run) throw new Error(`finalizeCancelledRun: run "${runId}" vanished mid-transaction`)
    return { run, cancelledSteps }
  })
}

// The step-side half: what a worker does when it notices a cancel request
// while it owns a `running` step. Same fence as sleepStep/lockStepIfOwner —
// only the current lease holder may write this outcome, so a worker whose
// lease was already reclaimed gets undefined and commits nothing instead of
// cancelling a step its successor is midway through.
export async function cancelRunningStep(
  sql: Db,
  args: { stepId: string; workerId: string }
): Promise<StepRow | undefined> {
  const rows = await sql<StepRow[]>`
    update step set
      status = 'cancelled',
      lease_owner = null,
      lease_expires_at = null,
      updated_at = now()
    where id = ${args.stepId} and status = 'running' and lease_owner = ${args.workerId}
    returning *
  `
  return rows[0]
}

// What the cancellation sweep scans: requested but not yet finalized. Backed
// by run_cancel_requested_idx (0003), a partial index on exactly this
// predicate. Oldest request first so a cancel can't be starved by newer ones.
export async function getCancelRequestedRuns(sql: Db): Promise<RunRow[]> {
  return sql<RunRow[]>`
    select * from run
    where cancel_requested_at is not null and status in ('queued', 'running')
    order by cancel_requested_at
  `
}

// Observability + tests: which steps are asleep right now, as opposed to
// sitting in retry backoff (both are `ready` with a future run_after — the
// sleeping_until marker is the only thing that tells them apart).
export async function getSleepingSteps(sql: Db, runId?: string): Promise<StepRow[]> {
  const runFilter = runId ? sql`and run_id = ${runId}` : sql``
  return sql<StepRow[]>`
    select * from step
    where status = 'ready' and sleeping_until > now()
      ${runFilter}
    order by sleeping_until
  `
}

// ---- history -------------------------------------------------------------

export async function insertHistory(
  sql: Db,
  input: { runId: string; stepId?: string; type: string; data?: unknown }
): Promise<HistoryRow> {
  const rows = await sql<HistoryRow[]>`
    insert into history (run_id, step_id, type, data)
    values (
      ${input.runId},
      ${input.stepId ?? null},
      ${input.type},
      ${input.data === undefined ? null : sql.json(toJson(input.data))}
    )
    returning *
  `
  const row = rows[0]
  if (!row) throw new Error('insertHistory: insert returned no row')
  return row
}
