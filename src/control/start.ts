// The public face of "start a run by workflow name" (#21 API triggers, #24
// delayed starts). Both the HTTP layer (src/triggers/http.ts) and any future
// programmatic caller funnel through here rather than hitting
// `startRunForWorkflowName` / `createSchedule` directly — same shape as
// control/signal.ts's relationship to `publishEvent`.
//
// The split between the two functions mirrors the two ways a caller can ask
// for a run to exist: "now" (startRun, an immediate run row + materialized
// steps) or "later" (scheduleRun, a `once` schedule row that Agent 3's poller
// promotes to a run when it comes due). Neither function decides which one to
// call — the HTTP router does that based on whether the request carried a
// future start time — so this file stays a thin, testable wrapper over the
// repository functions, not a place where request-parsing policy leaks in.

import type { Db } from '../store/client.ts'
import {
  createSchedule,
  startRunForWorkflowName,
  type ScheduleRow,
  type StartRunByNameResult,
} from '../store/repositories.ts'

export interface StartRunInput {
  workflowName: string
  version?: number
  input?: unknown
  namespace?: string
  priority?: number
  idempotencyKey?: string
}

/**
 * Start a run immediately for a registered workflow name. Idempotent when
 * `idempotencyKey` is supplied: a redelivery with the same key returns the
 * original run (`created: false`) instead of starting a second one.
 */
export async function startRun(db: Db, input: StartRunInput): Promise<StartRunByNameResult> {
  return startRunForWorkflowName(db, {
    workflowName: input.workflowName,
    version: input.version,
    input: input.input,
    namespace: input.namespace,
    priority: input.priority,
    idempotencyKey: input.idempotencyKey,
  })
}

export interface ScheduleRunInput {
  workflowName: string
  /** When the run should actually start. */
  runAt: Date
  input?: unknown
  namespace?: string
  priority?: number
}

/**
 * Register a one-shot delayed start (#24): a `once` schedule row due at
 * `runAt`. This function does not itself start anything — Agent 3's poller
 * (`claimDueSchedules`) picks it up once it's due and promotes it to a real
 * run via `startRun`'s repository path. No idempotency key here: schedules
 * don't carry one in the current schema, so a repeated request creates a
 * second schedule row — callers that care should dedupe before calling this.
 */
export async function scheduleRun(db: Db, input: ScheduleRunInput): Promise<ScheduleRow> {
  return createSchedule(db, {
    workflowName: input.workflowName,
    kind: 'once',
    nextRunAt: input.runAt,
    input: input.input,
    namespace: input.namespace,
    priority: input.priority,
  })
}
