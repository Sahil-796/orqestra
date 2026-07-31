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
  created_at: Date
  updated_at: Date
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
export async function createRun(
  sql: Db,
  input: {
    workflowId: string
    namespace?: string
    priority?: number
    input?: unknown
    idempotencyKey?: string
  }
): Promise<{ run: RunRow; created: boolean }> {
  if (input.idempotencyKey === undefined) {
    const rows = await sql<RunRow[]>`
      insert into run (workflow_id, namespace, priority, input, idempotency_key)
      values (
        ${input.workflowId},
        ${input.namespace ?? 'default'},
        ${input.priority ?? 0},
        ${input.input === undefined ? null : sql.json(toJson(input.input))},
        ${null}
      )
      returning *
    `
    const row = rows[0]
    if (!row) throw new Error('createRun: insert returned no row')
    return { run: row, created: true }
  }

  const inserted = await sql<RunRow[]>`
    insert into run (workflow_id, namespace, priority, input, idempotency_key)
    values (
      ${input.workflowId},
      ${input.namespace ?? 'default'},
      ${input.priority ?? 0},
      ${input.input === undefined ? null : sql.json(toJson(input.input))},
      ${input.idempotencyKey}
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
