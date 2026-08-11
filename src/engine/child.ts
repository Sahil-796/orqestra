// Phase 4 #20: child workflows — the pure half. No I/O, no `postgres`
// import, unit-testable without a database (same posture as
// engine/sleep.ts and engine/timeout.ts). This module owns *policy*: given
// a child run's terminal status and what it produced, decide what the
// parent should see. *Persisting* anything (spawning the child row,
// reading it back) is control/child.ts's job — the same split as
// engine/retry.ts (decides the backoff) vs repositories.ts (writes it).

import { deserializeError, type RunStatus, type SerializedError } from '../types.ts'

const CHILD_ERROR_BRAND = '__orqestraChildWorkflowError'

/**
 * Thrown by `runChildWorkflow` (control/child.ts) when the child run it
 * spawned ended `failed` or `cancelled` and the caller used the
 * default propagation policy (see control/child.ts's module doc): a
 * failed/cancelled child fails the parent step, exactly the way any other
 * thrown error would — the parent step's existing retry/catch handling
 * applies completely unchanged, nothing here or in control/child.ts adds
 * special-casing for it. Carries the child's id/status/cause so a `catch`
 * block (or the persisted `step.error`) can tell "my child failed" apart
 * from any other error.
 */
export class ChildWorkflowError extends Error {
  /** Brand, not `instanceof` — see isChildWorkflowError for why. */
  readonly [CHILD_ERROR_BRAND] = true as const
  readonly childRunId: string
  readonly childStatus: 'failed' | 'cancelled'

  constructor(childRunId: string, childStatus: 'failed' | 'cancelled', cause?: unknown) {
    const causeMessage = cause instanceof Error ? `: ${cause.message}` : ''
    super(`child run ${childRunId} ended as '${childStatus}'${causeMessage}`)
    this.name = 'ChildWorkflowError'
    this.childRunId = childRunId
    this.childStatus = childStatus
    if (cause !== undefined) this.cause = cause
  }
}

/**
 * Brand check rather than `instanceof`, same reasoning as isSleepSignal /
 * isStepTimeoutError: a module loaded through two different specifiers
 * produces two distinct classes, and `instanceof` across them is false.
 */
export function isChildWorkflowError(value: unknown): value is ChildWorkflowError {
  if (typeof value !== 'object' || value === null) return false
  if ((value as Record<string, unknown>)[CHILD_ERROR_BRAND] === true) return true
  return value instanceof ChildWorkflowError
}

const TERMINAL_RUN_STATUSES: readonly RunStatus[] = ['completed', 'failed', 'cancelled']

export function isTerminalRunStatus(status: RunStatus): boolean {
  return TERMINAL_RUN_STATUSES.includes(status)
}

/** A child run's outcome, once it has reached a terminal status. */
export type ChildOutcome =
  | { ok: true; value: unknown }
  | { ok: false; status: 'failed' | 'cancelled'; error: SerializedError | undefined }

/**
 * Classify a terminal child run into a `ChildOutcome`. `completed` carries
 * `run.output` (already the plain `{ [stepName]: value }` shape
 * `advanceRun`/`advanceDag` build — nothing to decode further). `failed`/
 * `cancelled` carry whatever `SerializedError` the caller found on the
 * child's own step rows (the `run` row itself has no error column — see
 * control/child.ts's `getChildOutcome` for where that error is sourced).
 * Throws if `status` isn't terminal — callers must check first
 * (`isTerminalRunStatus`); this function never polls or waits.
 */
export function classifyChildRun(
  status: RunStatus,
  output: unknown,
  failedStepError: SerializedError | undefined
): ChildOutcome {
  if (status === 'completed') return { ok: true, value: output }
  if (status === 'failed' || status === 'cancelled') {
    return { ok: false, status, error: failedStepError }
  }
  throw new Error(`classifyChildRun: run status "${status}" is not terminal`)
}

/** Turn a failed/cancelled ChildOutcome into the Error a caller should see. */
export function toChildWorkflowError(
  childRunId: string,
  outcome: Extract<ChildOutcome, { ok: false }>
): ChildWorkflowError {
  const cause = outcome.error ? deserializeError(outcome.error) : undefined
  return new ChildWorkflowError(childRunId, outcome.status, cause)
}
