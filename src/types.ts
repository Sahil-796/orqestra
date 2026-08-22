// core types shared across the engine + a jsonb-safe Result codec

export type RunStatus = 'queued' | 'running' | 'completed' | 'failed' | 'cancelled'

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

export interface StepDefinition {
  name: string
  dependsOn: string[]
  maxAttempts: number
  timeoutMs?: number
  priority: number
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
