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
  lockRun,
  markStepReady,
  markStepRunning,
  resetRunningSteps,
  skipStep,
  updateRunStatus,
  type NewStep,
  type RunRow,
  type StepRow,
} from '../store/repositories.ts'
import { decodeResult, serializeError, type RunStatus } from '../types.ts'
import { createWorkflowContext, getSkipRequests } from '../define/context.ts'
import type { WorkflowHandle } from '../define/workflow.ts'
import { createLogger } from '../observability/logger.ts'
import { wakeParentAwaiting } from './dag.ts'
import { isRunComplete, newlyReadySteps } from './scheduler.ts'

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

export interface EnqueueRunResult {
  runId: string
  created: boolean
}

// Shared by startRun (inline) and enqueueRun (durable): register the
// workflow, idempotently create the run row, and — only on first creation —
// materialize one step row per DAG step (no deps -> ready immediately,
// otherwise pending until the readiness sweep in `advanceRun` flips it).
// Never runs anything; that's the caller's job (executeRun, or a worker
// claiming rows off the queue).
async function registerAndCreateRun(
  db: Db,
  handle: WorkflowHandle,
  options: StartRunOptions
): Promise<{ run: RunRow; created: boolean }> {
  const workflow = await handle.register(db)

  const { run, created } = await createRun(db, {
    workflowId: workflow.id,
    namespace: options.namespace,
    priority: options.priority,
    input: options.input,
    idempotencyKey: options.idempotencyKey,
  })

  if (!created) return { run, created }

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

  return { run, created }
}

/**
 * Start a workflow run. Idempotent when `options.idempotencyKey` is given:
 * a second `startRun` with the same key never re-materializes steps — if
 * the existing run already finished, its result is returned as-is; if it's
 * still in flight, execution resumes instead of starting over (feature #8).
 *
 * INLINE mode: this drives the run to completion (or first failure) on the
 * calling process before returning. See `enqueueRun` for the DURABLE mode
 * that hands the run to the queue instead.
 */
export async function startRun(
  db: Db,
  handle: WorkflowHandle,
  options: StartRunOptions = {}
): Promise<RunResult> {
  const { run } = await registerAndCreateRun(db, handle, options)
  return executeRun(db, handle, run.id)
}

/**
 * Register the workflow, idempotently create the run (same semantics as
 * `startRun`), and return IMMEDIATELY without executing anything — the step
 * rows just inserted (or already present) are the queue from here on; any
 * worker draining this namespace picks them up. This is the DURABLE
 * counterpart to `startRun`'s INLINE mode.
 */
export async function enqueueRun(
  db: Db,
  handle: WorkflowHandle,
  options: StartRunOptions = {}
): Promise<EnqueueRunResult> {
  const { run, created } = await registerAndCreateRun(db, handle, options)
  return { runId: run.id, created }
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
    const { steps, result } = await advanceRun(db, runId)
    if (result) return result // every step completed — advanceRun already finalized the run

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
      // This run may itself be some other run's child (#20) — a run that
      // ends `failed` must wake whoever is blocked on it just as surely as
      // one that completes. Same transaction as the status write; see
      // dag.ts's wakeParentAwaiting for why that ordering is required.
      await wakeParentAwaiting(tx, run.id)
    })
    return { runId: run.id, status: 'failed' }
  }

  const ctx = createWorkflowContext({ runId: run.id, input: run.input, stepId: step.id })

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

      // Feature #17, inline-driver half: apply any `ctx.skip(...)` requests
      // made by this step. Deliberately simpler than worker.ts's
      // advanceDag — no cascade-skip through a chain of branch-only steps,
      // just the direct names this step named — because the next loop
      // iteration's `advanceRun` rescan (scheduler.ts's
      // `dependenciesSatisfied`, which now treats `skipped` as satisfied
      // exactly like `completed`) picks up everything downstream from
      // there. The inline path is single-process/sequential; the worker
      // path (this phase's shipping bar) is where the full cascade policy
      // lives.
      const skipNames = getSkipRequests(ctx)
      if (skipNames.length > 0) {
        const siblings = await getStepsByRun(tx, run.id)
        for (const name of skipNames) {
          const target = siblings.find((s) => s.name === name)
          if (target) await skipStep(tx, target.id, 'branch not taken')
        }
      }
    })
    return undefined
  } catch (e) {
    const error = serializeError(e)
    await withTransaction(db, async (tx) => {
      await failStep(tx, step.id, error)
      await insertHistory(tx, { runId: run.id, stepId: step.id, type: 'step.failed', data: { error } })
      await updateRunStatus(tx, run.id, 'failed', { finishedAt: new Date() })
      await wakeParentAwaiting(tx, run.id)
    })
    return { runId: run.id, status: 'failed' }
  }
}

export interface AdvanceResult {
  /** The run's steps, reflecting any pending -> ready flips this call made. */
  steps: StepRow[]
  /** Set once the run just finished — the caller should stop driving it. */
  result: RunResult | undefined
}

/**
 * The one piece of "what happens after a step's outcome lands" logic,
 * shared by the inline executor's loop and the worker (src/worker/worker.ts)
 * so there is exactly one implementation of DAG advancement, not two that
 * could drift. Given `sql`, which may be a bare `db` (executeRun's loop,
 * matching Phase 1's original never-wrapped-in-a-tx ready-flip behavior) or
 * an open transaction (a worker committing a step's outcome and advancing
 * the run atomically in the same tx — see the worker's `commitOutcome`):
 *
 *   1. Re-read the run's steps and flip every `pending` step whose deps are
 *      all `completed` to `ready` (the readiness rule lives once, in
 *      scheduler.ts's `newlyReadySteps` — not re-implemented here).
 *   2. If every step is now `completed`, finalize the run: build `output`
 *      as `{ [stepName]: decodedResultValue }` and mark it `completed`.
 *
 * Returns the (possibly updated) steps either way, plus a `result` that's
 * only set when this call was the one that finished the run.
 *
 * Concurrency note: this locks the run row (`lockRun`) before reading the
 * steps. Without that, two sibling fan-in steps completing in overlapping
 * worker transactions can each read the *other's* not-yet-committed
 * completion as still `running` — neither observes both deps satisfied, so
 * neither flips the downstream step to `ready`, and it's stranded `pending`
 * forever (no third event ever re-triggers the check). Locking the run row
 * makes the second transaction to reach this point wait for the first to
 * commit, so its subsequent read of the steps sees the first's completion
 * too. Cheap and correct in the common uncontended case; only matters when
 * two sibling steps finish in the same narrow window.
 */
export async function advanceRun(sql: Db, runId: string): Promise<AdvanceResult> {
  await lockRun(sql, runId)
  const steps = await getStepsByRun(sql, runId)

  for (const step of newlyReadySteps(steps)) {
    const updated = await markStepReady(sql, step.id)
    const i = steps.findIndex((s) => s.id === step.id)
    if (i !== -1) steps[i] = updated
  }

  if (!isRunComplete(steps)) return { steps, result: undefined }

  const output: Record<string, unknown> = {}
  for (const step of steps) {
    // Phase 4: a `skipped` step (#17's untaken branch) never ran and has no
    // `result` to decode — `decodeResult` would throw on its null column.
    // Its contribution to `output` is simply absent-of-a-value, same as
    // what a completed-but-void step would produce.
    if (step.status === 'skipped') {
      output[step.name] = undefined
      continue
    }
    const decoded = decodeResult<unknown>(step.result)
    output[step.name] = decoded.ok ? decoded.value : undefined
  }

  await updateRunStatus(sql, runId, 'completed', { output, finishedAt: new Date() })
  await insertHistory(sql, { runId, type: 'run.completed', data: { output } })
  // #20: wake any step blocked on this run as a child. The worker path's
  // equivalent lives in dag.ts's maybeFinalizeRun; this is the inline
  // driver's copy, so a child run driven by executeRun still releases a
  // parent that a worker pool is holding blocked.
  await wakeParentAwaiting(sql, runId)

  return { steps, result: { runId, status: 'completed', output } }
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
