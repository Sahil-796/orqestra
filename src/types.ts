// core types shared across the engine + a jsonb-safe Result codec

// Phase 7 adds two terminal values (0008_failure_handling.sql):
//   'dead_letter' — a run that exhausted its retries (or failed unrecoverably)
//   and has been parked for inspection / manual retry rather than discarded.
//   'completed_with_errors' — a run that finished under the `continue_on_error`
//   failure policy with at least one failed step (partial success).
export type RunStatus =
  | 'queued'
  | 'running'
  | 'completed'
  | 'failed'
  | 'cancelled'
  | 'dead_letter'
  | 'completed_with_errors'

// How a run reacts to a step exhausting its retries (#25), mirrored by the
// `run.failure_policy` CHECK (0008_failure_handling.sql):
//   'fail_fast'         — stop the run at the first exhausted step (default).
//   'continue_on_error' — keep running independent steps, ending in
//                         `completed_with_errors` if any step failed.
export type FailurePolicy = 'fail_fast' | 'continue_on_error'

export const FAILURE_POLICIES: readonly FailurePolicy[] = ['fail_fast', 'continue_on_error']

// Phase 4 adds two values (0004_orchestration.sql):
//   'skipped' — an untaken conditional branch (#17); terminal, not an error.
//   'blocked' — a step durably awaiting a child run's outcome (#20); like a
//   sleeping step, it holds no lease while in this state.
export type StepStatus =
  | 'pending'
  | 'ready'
  | 'running'
  | 'completed'
  | 'failed'
  | 'cancelled'
  | 'skipped'
  | 'blocked'

export const RUN_STATUSES: readonly RunStatus[] = [
  'queued',
  'running',
  'completed',
  'failed',
  'cancelled',
  'dead_letter',
  'completed_with_errors',
]

export const STEP_STATUSES: readonly StepStatus[] = [
  'pending',
  'ready',
  'running',
  'completed',
  'failed',
  'cancelled',
  'skipped',
  'blocked',
]

// ---- signals & triggers (Phase 5) -----------------------------------------
//
// `ScheduleKind` mirrors the `schedules.kind` CHECK constraint
// (0005_signals_triggers.sql) the same way `RunStatus`/`StepStatus` mirror
// theirs — add a value in both places or the DB and the types drift.

export type ScheduleKind = 'cron' | 'once'

export const SCHEDULE_KINDS: readonly ScheduleKind[] = ['cron', 'once']

/**
 * A declarative trigger a workflow carries in its definition so the trigger
 * daemon (Agent 3) can start runs of it without the workflow being invoked
 * by hand. This is data only — declaring a trigger implements nothing; the
 * daemon reads these and acts on them. A workflow may carry several.
 *
 *   - `event`: start a run whenever a matching event is published. An
 *     optional `correlationKey` narrows which events count (same matching
 *     rule as `waitForEvent`).
 *   - `cron`: start a run on a cron schedule.
 */
export type WorkflowTrigger =
  | { type: 'event'; event: string; correlationKey?: string }
  | { type: 'cron'; cron: string }

/** Options accepted by `ctx.waitForEvent(name, opts?)`. */
export interface WaitForEventOptions {
  /**
   * Narrow the wait to events carrying this correlation value. Omit to be
   * woken by any event of the given name.
   */
  correlationKey?: string
}

// ---- workflow / step definitions -----------------------------------------
//
// A `WorkflowDefinition` is the serializable DAG persisted into
// `workflow.dag`. It carries no functions — step implementations live only
// in-process (registered via defineWorkflow), keyed by step name, so the
// engine can look them up when it replays a run.

// ---- flow control at scale (Phase 6) --------------------------------------
//
// A step may declare a concurrency *key* and a *limit* (#12): at claim time it
// is only claimable if fewer than `limit` steps sharing that key are currently
// `running`. This is data on the step definition — the enforcement lives in the
// claim query (repositories.ts `claimNextStep`) and its policy helpers in
// control/concurrency.ts. A step with no `concurrency` is unlimited (the common
// case), and the un-keyed claim path is unaffected.
export interface ConcurrencyLimit {
  /** Steps sharing this key contend for the same limit, across all runs. */
  key: string
  /** Max steps with this key allowed `running` at once. Must be >= 1. */
  limit: number
}

// Rate limiting (#13): at most `limit` steps sharing `key` may *start* within
// any one fixed `windowMs` window, across all runs. Distinct from concurrency
// (#12): concurrency caps how many run *at once*, rate limiting caps how many
// *start per window*. Enforced in the claim query (repositories.ts
// `claimNextStep`) under a per-key advisory lock, with window math + validation
// helpers in control/ratelimit.ts. A step with no `rateLimit` is unlimited.
export interface RateLimit {
  /** Steps sharing this key contend for the same window budget, across all runs. */
  key: string
  /** Max starts allowed per window. Must be >= 1. */
  limit: number
  /** Window length in milliseconds. Must be >= 1. */
  windowMs: number
}

export interface StepDefinition {
  name: string
  dependsOn: string[]
  maxAttempts: number
  timeoutMs?: number
  priority: number
  /** Concurrency cap for this step (#12). Absent = unlimited. */
  concurrency?: ConcurrencyLimit
  /** Rate cap for this step (#13). Absent = unlimited. */
  rateLimit?: RateLimit
}

export interface WorkflowDefinition {
  name: string
  version: number
  steps: StepDefinition[]
}

// ---- Result codec ---------------------------------------------------------
//
// Step results/errors are stored in jsonb columns. `Error` instances aren't
// JSON-safe on their own (message/stack are non-enumerable), so we codec
// them to/from a plain shape that survives a jsonb round-trip.

export interface SerializedError {
  name: string
  message: string
  stack?: string
  cause?: unknown
}

export type Result<T> =
  | { ok: true; value: T }
  | { ok: false; error: SerializedError }

export function ok<T>(value: T): Result<T> {
  return { ok: true, value }
}

export function err<T = never>(error: unknown): Result<T> {
  return { ok: false, error: serializeError(error) }
}

export function serializeError(error: unknown): SerializedError {
  if (error instanceof Error) {
    const serialized: SerializedError = {
      name: error.name,
      message: error.message,
      stack: error.stack,
    }
    if ('cause' in error) serialized.cause = error.cause
    return serialized
  }
  return {
    name: 'NonErrorThrown',
    message: typeof error === 'string' ? error : JSON.stringify(error),
  }
}

export function deserializeError(serialized: SerializedError): Error {
  const error = new Error(serialized.message)
  error.name = serialized.name
  if (serialized.stack) error.stack = serialized.stack
  if ('cause' in serialized) (error as { cause?: unknown }).cause = serialized.cause
  return error
}

/** Serialize a Result<T> to a plain JSON-safe value for a jsonb column. */
export function encodeResult<T>(result: Result<T>): unknown {
  return result
}

/** Parse a value read back from a jsonb column into a Result<T>. */
export function decodeResult<T>(raw: unknown): Result<T> {
  if (
    raw !== null &&
    typeof raw === 'object' &&
    'ok' in raw &&
    typeof (raw as { ok: unknown }).ok === 'boolean'
  ) {
    return raw as Result<T>
  }
  throw new Error(`decodeResult: value is not a valid Result: ${JSON.stringify(raw)}`)
}

// ---- observability read model (Phase 8) ------------------------------------
//
// These are the shapes the dashboard's read API + UI consume directly, so
// they live here (shared across units) rather than as private row interfaces
// in repositories.ts. DB-column-shaped rows (snake_case, matching the table
// exactly) stay in repositories.ts as usual; these are the camelCase,
// already-joined/derived views the repository functions return.

/** Mirrors the `worker_health.status` CHECK (0009_observability.sql). */
export type WorkerHealthStatus = 'running' | 'draining' | 'stopped'

export const WORKER_HEALTH_STATUSES: readonly WorkerHealthStatus[] = [
  'running',
  'draining',
  'stopped',
]

/** One row of `listRuns` (#30) — a run joined to its workflow name, with a
 * computed wall-clock duration when the run has both started and finished. */
export interface RunListItem {
  id: string
  workflowId: string
  workflowName: string
  namespace: string
  status: RunStatus
  priority: number
  createdAt: Date
  startedAt: Date | null
  finishedAt: Date | null
  /** `finishedAt - startedAt` in milliseconds, or null if either is missing. */
  durationMs: number | null
}

/** One row of `listWorkerHealth` (#34) — a worker's last-reported heartbeat
 * plus the derived `alive` flag (last_heartbeat_at within the staleness
 * window the caller asked for). */
export interface WorkerHealthView {
  workerId: string
  hostname: string | null
  status: WorkerHealthStatus
  leasedSteps: number
  concurrency: number | null
  startedAt: Date
  lastHeartbeatAt: Date
  /** False when `last_heartbeat_at` is older than the caller's `staleAfterMs`. */
  alive: boolean
}

/** One row of `getRunMetrics` (#32) — aggregates over runs/steps matching a
 * filter, optionally scoped to a single workflow when `groupByWorkflow` is
 * set. Step duration is approximated as `step.updated_at - step.created_at`
 * for terminal steps: the schema has no separate "step started running"
 * timestamp, so this measures the step's whole creation-to-terminal wall
 * time (queue wait + every attempt + retry backoff), not pure execution
 * time. `avgRunQueueWaitMs` is the more precise queue-wait figure, computed
 * from `run.created_at`/`run.started_at`, both of which the schema does
 * carry. */
export interface RunMetrics {
  /** Null when this row aggregates across all workflows (no grouping requested). */
  workflowName: string | null
  runCount: number
  statusCounts: Partial<Record<RunStatus, number>>
  avgStepDurationMs: number | null
  p50StepDurationMs: number | null
  p95StepDurationMs: number | null
  avgRunQueueWaitMs: number | null
  totalAttempts: number
  totalReclaims: number
}
