// Public API surface for orqestra.

import { loadConfig } from './config.ts'
import { createLogger, type Logger } from './observability/logger.ts'
import { createDb, type Db } from './store/client.ts'
import { migrate } from './store/migrate.ts'
import * as repositories from './store/repositories.ts'

export { defineWorkflow, getRegisteredWorkflow } from './define/workflow.ts'
export type { WorkflowBuilder, WorkflowHandle, StepFn, StepOptions } from './define/workflow.ts'
export type { WorkflowContext } from './define/context.ts'
export { createWorkflowContext } from './define/context.ts'

export { startRun, executeRun, resumeRun, resumeAll, enqueueRun, advanceRun } from './engine/executor.ts'
export type { StartRunOptions, RunResult, EnqueueRunResult, AdvanceResult } from './engine/executor.ts'

export { createWorker } from './worker/worker.ts'
export type { Worker, WorkerOptions } from './worker/worker.ts'

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

  async close(): Promise<void> {
    await this.db.end()
  }
}

/** Create an Orquestra client. Defaults to DATABASE_URL / the local docker db. */
export function orquestra(options: { databaseUrl?: string } = {}): Orquestra {
  return new Orquestra(options)
}
