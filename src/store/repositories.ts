// Typed query functions — the only layer (besides client.ts/migrate.ts)
// that talks to Postgres. engine/ and define/ call through here, never
// through `postgres` directly, so storage can be swapped later.

import type postgres from 'postgres'
import type { Db } from './client.ts'
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
