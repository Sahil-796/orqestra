// Public API surface for orqestra.

import { loadConfig } from './config.ts'
import { createLogger, type Logger } from './observability/logger.ts'
import { createDb, type Db } from './store/client.ts'
import { migrate } from './store/migrate.ts'
import * as repositories from './store/repositories.ts'
import { cancelRun, type CancelResult } from './control/cancel.ts'

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

// Phase 5 — Signals & triggers. Runs pause on ctx.waitForEvent and resume when
// an event is published; they also start five ways — direct API call, internal
// event, cron, a future timestamp, or an inbound webhook.
export { publishSignal } from './control/signal.ts'
export type { PublishSignalInput, PublishSignalResult } from './control/signal.ts'
export { startRun as startRunByName, scheduleRun } from './control/start.ts'
export type { StartRunInput, ScheduleRunInput } from './control/start.ts'

export { startServer } from './server.ts'
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

  async close(): Promise<void> {
    await this.db.end()
  }
}

/** Create an Orquestra client. Defaults to DATABASE_URL / the local docker db. */
export function orquestra(options: { databaseUrl?: string } = {}): Orquestra {
  return new Orquestra(options)
}
