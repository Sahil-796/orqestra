// The Phase 1 driver: single-process, durable, data-driven execution of a
// workflow DAG. The `step` table IS the state machine — this file never
// "replays" workflow code, it reads step rows, runs whichever are `ready`,
// and persists the outcome before moving on. That's what makes a crash
// mid-run recoverable: on restart, `executeRun` reloads the same rows and
// picks up exactly where they left off.

import type { Db } from '../store/client.ts'
import { withTransaction } from '../store/client.ts'
import {
  completeStep,
  createRun,
  failStep,
  getIncompleteRuns,
  getRun,
  getStepsByRun,
  getWorkflowByName,
  insertHistory,
  insertSteps,
  markStepReady,
  markStepRunning,
  resetRunningSteps,
  updateRunStatus,
  type NewStep,
  type RunRow,
  type StepRow,
} from '../store/repositories.ts'
import { decodeResult, serializeError, type RunStatus } from '../types.ts'
import { createWorkflowContext } from '../define/context.ts'
import type { WorkflowHandle } from '../define/workflow.ts'
import { createLogger } from '../observability/logger.ts'

const logger = createLogger()

export interface StartRunOptions {
  input?: unknown
  idempotencyKey?: string
  namespace?: string
  priority?: number
}

export interface RunResult {
  runId: string
  status: RunStatus
  output?: unknown
}

function isTerminal(status: RunStatus): boolean {
  return status === 'completed' || status === 'failed' || status === 'cancelled'
}

/**
 * Start a workflow run. Idempotent when `options.idempotencyKey` is given:
 * a second `startRun` with the same key never re-materializes steps — if
 * the existing run already finished, its result is returned as-is; if it's
 * still in flight, execution resumes instead of starting over (feature #8).
 */
export async function startRun(
  db: Db,
  handle: WorkflowHandle,
  options: StartRunOptions = {}
): Promise<RunResult> {
  const workflow = await handle.register(db)

  const { run, created } = await createRun(db, {
    workflowId: workflow.id,
    namespace: options.namespace,
    priority: options.priority,
    input: options.input,
    idempotencyKey: options.idempotencyKey,
  })

  if (!created) {
    if (isTerminal(run.status)) {
      return { runId: run.id, status: run.status, output: run.output }
    }
    return executeRun(db, handle, run.id)
  }

  // One step row per DAG step: no deps -> ready immediately, otherwise
  // pending until executeRun's loop flips it once its deps complete.
  const steps: NewStep[] = handle.definition.steps.map((step) => ({
    name: step.name,
    dependsOn: step.dependsOn,
    maxAttempts: step.maxAttempts,
    timeoutMs: step.timeoutMs,
    priority: step.priority,
    status: step.dependsOn.length === 0 ? 'ready' : 'pending',
  }))

  await withTransaction(db, async (tx) => {
    await insertSteps(tx, run.id, steps)
    await insertHistory(tx, { runId: run.id, type: 'run.created', data: { input: options.input } })
  })

  return executeRun(db, handle, run.id)
}

/**
 * Run (or resume) a workflow run to completion or first failure. This is
 * BOTH the normal execution path and the crash-recovery path — calling it
 * again on a run that already made partial progress simply continues from
 * the step rows as they are; `running` steps left behind by a crash are
 * reset to `ready` first (single process, no leasing yet in Phase 1).
 */
export async function executeRun(db: Db, handle: WorkflowHandle, runId: string): Promise<RunResult> {
  const run = await getRun(db, runId)
  if (!run) throw new Error(`executeRun: no run found for id "${runId}"`)

  if (isTerminal(run.status)) {
    return { runId: run.id, status: run.status, output: run.output }
  }

  // Steps `running` at this point were interrupted, not in progress.
  await resetRunningSteps(db, runId)

  if (run.status === 'queued') {
    await withTransaction(db, async (tx) => {
      await updateRunStatus(tx, runId, 'running', { startedAt: new Date() })
      await insertHistory(tx, { runId, type: 'run.started' })
    })
  }

  while (true) {
    const steps = await getStepsByRun(db, runId)

    // Flip any pending step whose deps are all completed to ready.
    for (let i = 0; i < steps.length; i++) {
      const step = steps[i]!
      if (step.status !== 'pending') continue
      const depsCompleted = step.depends_on.every(
        (depName) => steps.find((s) => s.name === depName)?.status === 'completed'
      )
      if (depsCompleted) steps[i] = await markStepReady(db, step.id)
    }

    if (steps.every((s) => s.status === 'completed')) {
      return finalize(db, runId, steps)
    }

    const readySteps = steps.filter((s) => s.status === 'ready')
    if (readySteps.length === 0) {
      // A well-formed DAG always has something ready or completed; getting
      // here means every remaining step is blocked on a dep that will
      // never complete (e.g. a cycle, or a dep name typo) — surface it
      // loudly instead of spinning forever.
      throw new Error(`executeRun: run "${runId}" is stuck — no ready steps and not all completed`)
    }

    for (const step of readySteps) {
      const result = await runStep(db, handle, run, step)
      if (result) return result // fail-fast: first failure stops the run
    }
  }
}

/** Run one step to completion or failure, persisting the outcome atomically. */
async function runStep(
  db: Db,
  handle: WorkflowHandle,
  run: RunRow,
  step: StepRow
): Promise<RunResult | undefined> {
  await markStepRunning(db, step.id)

  const fn = handle.stepFns.get(step.name)
  if (!fn) {
    const error = serializeError(
      new Error(`executeRun: no step function registered for step "${step.name}"`)
    )
    await withTransaction(db, async (tx) => {
      await failStep(tx, step.id, error)
      await insertHistory(tx, { runId: run.id, stepId: step.id, type: 'step.failed', data: { error } })
      await updateRunStatus(tx, run.id, 'failed', { finishedAt: new Date() })
    })
    return { runId: run.id, status: 'failed' }
  }

  const ctx = createWorkflowContext({ runId: run.id, input: run.input })

  // Run the user function OUTSIDE a transaction (it may be slow / call out
  // to the world); only the persistence of its outcome is atomic.
  try {
    const value = await fn(ctx)
    await withTransaction(db, async (tx) => {
      await completeStep(tx, step.id, value)
      await insertHistory(tx, {
        runId: run.id,
        stepId: step.id,
        type: 'step.completed',
        data: { result: value },
      })
    })
    return undefined
  } catch (e) {
    const error = serializeError(e)
    await withTransaction(db, async (tx) => {
      await failStep(tx, step.id, error)
      await insertHistory(tx, { runId: run.id, stepId: step.id, type: 'step.failed', data: { error } })
      await updateRunStatus(tx, run.id, 'failed', { finishedAt: new Date() })
    })
    return { runId: run.id, status: 'failed' }
  }
}

/** All steps completed: persist the run's output and mark it completed. */
async function finalize(db: Db, runId: string, steps: StepRow[]): Promise<RunResult> {
  const output: Record<string, unknown> = {}
  for (const step of steps) {
    const decoded = decodeResult<unknown>(step.result)
    output[step.name] = decoded.ok ? decoded.value : undefined
  }

  await withTransaction(db, async (tx) => {
    await updateRunStatus(tx, runId, 'completed', { output, finishedAt: new Date() })
    await insertHistory(tx, { runId, type: 'run.completed', data: { output } })
  })

  return { runId, status: 'completed', output }
}

/** Alias for executeRun — resuming a run IS executing it from where the step rows left off. */
export async function resumeRun(db: Db, handle: WorkflowHandle, runId: string): Promise<RunResult> {
  return executeRun(db, handle, runId)
}

/**
 * Crash recovery across a whole process restart: find every run still
 * `queued`/`running`, resolve each to a registered workflow handle from
 * `handles` (keyed by workflow name), and resume it. Runs whose workflow
 * isn't registered in this process are skipped with a warning — a Phase 1
 * single-process deployment can only resume workflows it knows about.
 */
export async function resumeAll(db: Db, handles: Map<string, WorkflowHandle>): Promise<RunResult[]> {
  const incompleteRuns = await getIncompleteRuns(db)
  if (incompleteRuns.length === 0) return []

  const idToHandle = new Map<string, WorkflowHandle>()
  for (const handle of handles.values()) {
    const workflow = await getWorkflowByName(db, handle.name)
    if (workflow) idToHandle.set(workflow.id, handle)
  }

  const results: RunResult[] = []
  for (const run of incompleteRuns) {
    const handle = idToHandle.get(run.workflow_id)
    if (!handle) {
      logger.warn('resumeAll: skipping run — no registered handle for its workflow', {
        runId: run.id,
        workflowId: run.workflow_id,
      })
      continue
    }
    results.push(await executeRun(db, handle, run.id))
  }
  return results
}
