// Public API surface for orqestra.

import { loadConfig } from './config.ts'
import { createLogger, type Logger } from './observability/logger.ts'
import { createDb, type Db } from './store/client.ts'
import { migrate } from './store/migrate.ts'
import * as repositories from './store/repositories.ts'
import { cancelRun, type CancelResult } from './control/cancel.ts'
import { retryDeadLetterRun, type RetryDeadLetterResult } from './control/retry.ts'

export { defineWorkflow, getRegisteredWorkflow, getWorkflowTriggers } from './define/workflow.ts'
export type { WorkflowBuilder, WorkflowHandle, StepFn, StepOptions } from './define/workflow.ts'
export type { WorkflowContext } from './define/context.ts'
export { createWorkflowContext } from './define/context.ts'

export { startRun, executeRun, resumeRun, resumeAll, enqueueRun, advanceRun } from './engine/executor.ts'
export type { StartRunOptions, RunResult, EnqueueRunResult, AdvanceResult } from './engine/executor.ts'

export { SleepSignal, isSleepSignal, parseDuration } from './engine/sleep.ts'
export { StepTimeoutError, isStepTimeoutError, withTimeout } from './engine/timeout.ts'

// Phase 4 #20: child workflows. The wait is event-driven — the parent step
// suspends once into the `blocked` status and is woken by the child run's
// terminal transition. See src/control/child.ts's module doc for the full
// path and the propagation policy.
export {
  ChildWorkflowError,
  isChildWorkflowError,
  ChildBlockSignal,
  isChildBlockSignal,
  classifyChildRun,
  isTerminalRunStatus,
  type ChildOutcome,
} from './engine/child.ts'
export {
  spawnChildRun,
  getChildOutcome,
  awaitChildRun,
  runChildWorkflow,
  runChildWorkflowResult,
  type SpawnChildOptions,
  type SpawnChildResult,
  type AwaitChildOptions,
  type ChildRunResult,
} from './control/child.ts'
export { nextChildCallSeq } from './define/context.ts'
export { advanceDag, maybeFinalizeRun, wakeParentAwaiting, sweepBlockedChildAwaits } from './engine/dag.ts'

export { createWorker } from './worker/worker.ts'
export type { Worker, WorkerOptions } from './worker/worker.ts'

export { cancelRun, isRunCancelled, sweepCancelledRuns } from './control/cancel.ts'
export type { CancelResult } from './control/cancel.ts'

// Phase 7 — Failure handling. A run that exhausts its retry budget under the
// default fail_fast policy is parked in the dead-letter queue (#26) instead of
// vanishing; an operator re-drives it with retryDeadLetterRun (#27). A workflow
// can opt into continue-on-error via defineWorkflow's `failurePolicy` (#28), and
// steps register saga rollbacks with `ctx.compensate(fn)` which run in reverse
// on terminal failure, exactly once (#29).
export { retryDeadLetterRun, listDeadLetteredRuns } from './control/retry.ts'
export type { RetryDeadLetterResult } from './control/retry.ts'
export type { CompensationFn } from './define/context.ts'

// Phase 5 — Signals & triggers. Runs pause on ctx.waitForEvent and resume when
// an event is published; they also start five ways — direct API call, internal
// event, cron, a future timestamp, or an inbound webhook.
export { publishSignal } from './control/signal.ts'
export type { PublishSignalInput, PublishSignalResult } from './control/signal.ts'
export { startRun as startRunByName, scheduleRun } from './control/start.ts'
export type { StartRunInput, ScheduleRunInput } from './control/start.ts'

export { startServer, createServerHandler } from './server.ts'
export type { StartServerOptions } from './server.ts'
export { createTriggerHandler } from './triggers/http.ts'
export { mapWebhookToEvent } from './triggers/webhook.ts'

export { startTriggerRunner, runTriggerTick } from './triggers/runner.ts'
export type { TriggerRunner, TriggerRunnerOptions, TriggerTickResult } from './triggers/runner.ts'
export { pollDueSchedules } from './triggers/scheduled.ts'
export { pollUndispatchedEvents, eventTriggerIdempotencyKey } from './triggers/events.ts'
export {
  computeNextRun,
  validateCronExpression,
  parseCronExpression,
  syncCronSchedules,
} from './triggers/cron.ts'

// Phase 6 — Flow control at scale. Concurrency limits (#12): a step declares a
// concurrency key + limit and the claim query enforces "at most N running for
// this key" atomically under a flood of concurrent claims. Priority aging
// (#14): the claim orders by an effective priority that grows with wait time,
// so normal-priority work is not starved by high-priority floods.
export { validateConcurrency, isConcurrencyLimited } from './control/concurrency.ts'
export { effectivePriority, ageBoost } from './control/priority.ts'
export type { PriorityAgingConfig } from './control/priority.ts'
// Rate limiting (#13): a step declares a rate key + limit + window and the claim
// query enforces "at most N starts per window for this key" atomically under a
// flood of concurrent claims, deferring exhausted steps to the next window.
export { validateRateLimit, isRateLimited, windowStartMs, nextWindowStartMs } from './control/ratelimit.ts'
// `ConcurrencyLimit` and `RateLimit` (the step-declaration types) are already
// re-exported via `export * from './types.ts'` below.

export type { OrqConfig, LogLevel } from './config.ts'
export { loadConfig } from './config.ts'

export * from './types.ts'

export { createDb, getDb, withTransaction, closeDb } from './store/client.ts'
export type { Db } from './store/client.ts'
export { migrate } from './store/migrate.ts'
export { repositories }

export { createLogger } from './observability/logger.ts'
export type { Logger, LogFields } from './observability/logger.ts'

/**
 * Orquestra — the Postgres-backed client. Phase 0 only wires up config,
 * storage, and logging; running workflows lands in Phase 1+.
 */
export class Orquestra {
  readonly db: Db
  readonly logger: Logger

  constructor(options: { databaseUrl?: string } = {}) {
    const config = loadConfig(
      options.databaseUrl ? { ...process.env, DATABASE_URL: options.databaseUrl } : process.env
    )
    this.db = createDb(config)
    this.logger = createLogger(config.logLevel)
  }

  /** Run pending migrations against this instance's database. */
  async migrate(): Promise<string[]> {
    return migrate(this.db)
  }

  /**
   * Request cancellation of a run. Cooperative: see cancelRun — the result
   * tells you whether the run is already `cancelled` or whether a worker
   * still has to observe the request and finalize it.
   */
  async cancel(runId: string): Promise<CancelResult> {
    return cancelRun(this.db, runId)
  }

  /**
   * Re-drive a dead-lettered run (Phase 7 #27). Resets the parked run and its
   * failed steps so a worker claims and re-runs it; a no-op result comes back
   * if the id is not currently in the dead-letter queue.
   */
  async retry(runId: string): Promise<RetryDeadLetterResult> {
    return retryDeadLetterRun(this.db, runId)
  }

  async close(): Promise<void> {
    await this.db.end()
  }
}

/** Create an Orquestra client. Defaults to DATABASE_URL / the local docker db. */
export function orquestra(options: { databaseUrl?: string } = {}): Orquestra {
  return new Orquestra(options)
}
