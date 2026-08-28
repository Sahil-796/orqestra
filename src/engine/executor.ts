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
  deadLetterRun,
  failStep,
  findMatchingEventSince,
  getIncompleteRuns,
  getRun,
  getStepsByRun,
  getWorkflowByName,
  hasCompensationRun,
  insertHistory,
  insertSteps,
  lockRun,
  markStepReady,
  markStepRunning,
  recordCompensation,
  registerStepEventWait,
  resetRunningSteps,
  setRunFailurePolicy,
  skipStep,
  updateRunStatus,
  wakeStepsWaitingForEvent,
  type NewStep,
  type RunRow,
  type StepRow,
} from '../store/repositories.ts'
import { decodeResult, serializeError, type RunStatus, type SerializedError } from '../types.ts'
import {
  createWorkflowContext,
  getCompensations,
  getSkipRequests,
  type CompensationFn,
} from '../define/context.ts'
import { shouldRetry } from './retry.ts'
import type { WorkflowHandle } from '../define/workflow.ts'
import { createLogger } from '../observability/logger.ts'
import { isEventWaitSignal } from './event.ts'
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
  return (
    status === 'completed' ||
    status === 'failed' ||
    status === 'cancelled' ||
    // Phase 7: a dead-lettered run is done running until an operator revives it
    // (resetRunForRetry), and a `completed_with_errors` run has finished under
    // `continue_on_error`. Both are terminal for the driver loop — resuming
    // either is a no-op that just echoes the stored outcome.
    status === 'dead_letter' ||
    status === 'completed_with_errors'
  )
}

// A completed step's registered saga compensations (#29), captured in the
// order steps completed so a `fail_fast` rollback can replay them in reverse.
interface CompletedCompensation {
  stepName: string
  stepId: string
  fns: readonly CompensationFn[]
}

// The outcome of running one step, so `executeRun` — not `runStep` — owns the
// run-level decision (finalize, dead-letter, or keep going) that Phase 7's
// failure policies made policy-dependent.
type StepOutcome =
  | { kind: 'completed'; compensations: readonly CompensationFn[] }
  // An event-wait parked the step in `blocked`; the run stays `running` and is
  // resumed from outside once the event lands. Not a failure.
  | { kind: 'suspended' }
  // The step exhausted its retry budget — a terminal step failure. The step row
  // is already `failed`; what happens to the RUN is the caller's policy call.
  | { kind: 'failed'; error: SerializedError }

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
    concurrencyKey: step.concurrency?.key,
    concurrencyLimit: step.concurrency?.limit,
    rateKey: step.rateLimit?.key,
    rateLimit: step.rateLimit?.limit,
    rateWindowMs: step.rateLimit?.windowMs,
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

  // #28: the failure policy is read from the in-process handle — authoritative
  // even after a crash-recovery resume, where the persisted row might predate a
  // code change. Stamp it onto the run row too, purely for observability.
  const policy = handle.failurePolicy
  await setRunFailurePolicy(db, runId, policy)

  // #29: saga compensations of steps that COMPLETED during this drive, in
  // completion order. Replayed in reverse on a `fail_fast` terminal failure.
  // (Steps completed on a PRIOR drive aren't re-run, so aren't collected here;
  // their compensations already ran when they first failed the run — the
  // compensation log's idempotency is what keeps that correct across re-drives.)
  const completed: CompletedCompensation[] = []

  while (true) {
    const { steps, result } = await advanceRun(db, runId)
    if (result) return result // every step completed — advanceRun already finalized the run

    const readySteps = steps.filter((s) => s.status === 'ready')
    if (readySteps.length === 0) {
      // #18: a step suspended on `ctx.waitForEvent` sits in `blocked` with no
      // ready siblings — the run is not stuck, it is parked until a matching
      // event is published. Return without finalizing; a later resumeRun
      // (after the event lands and the step is back to `ready`) continues it.
      // This is the inline-driver analogue of the worker parking the step and
      // giving its slot back. (The inline driver still cannot itself PUBLISH
      // the event mid-loop — it is single-process and sequential — so the
      // publish must come from outside, then resumeRun.)
      if (steps.some((s) => s.status === 'blocked')) {
        return { runId, status: 'running' }
      }
      // #28 continue_on_error: a failed step never becomes `completed`, so its
      // dependents stay `pending` forever and there is nothing left to run.
      // That is not a stuck DAG — it is the terminal state of a
      // partially-successful run. Finish it as `completed_with_errors` (no
      // rollback, no DLQ), keeping the successful steps' side effects.
      if (steps.some((s) => s.status === 'failed')) {
        return finalizeWithErrors(db, runId, steps)
      }
      // A well-formed DAG always has something ready or completed; getting
      // here means every remaining step is blocked on a dep that will
      // never complete (e.g. a cycle, or a dep name typo) — surface it
      // loudly instead of spinning forever.
      throw new Error(`executeRun: run "${runId}" is stuck — no ready steps and not all completed`)
    }

    for (const step of readySteps) {
      // Phase 4 #17: cascade-skip — a step whose every dependency resolved by
      // being skipped (none completed) has no real input, so skip it rather
      // than run dead code, matching the worker path's cascadeIfAllDepsSkipped.
      if (step.depends_on.length > 0) {
        const deps = steps.filter((s) => step.depends_on.includes(s.name))
        if (deps.length > 0 && !deps.some((d) => d.status === 'completed')) {
          await skipStep(db, step.id, `all dependencies skipped: ${deps.map((d) => d.name).join(', ')}`)
          continue
        }
      }

      const outcome = await runStep(db, handle, run, step)

      if (outcome.kind === 'suspended') {
        // Event-wait parked the step; the run is not done, just waiting.
        return { runId, status: 'running' }
      }

      if (outcome.kind === 'completed') {
        if (outcome.compensations.length > 0) {
          completed.push({ stepName: step.name, stepId: step.id, fns: outcome.compensations })
        }
        continue
      }

      // outcome.kind === 'failed' — a step exhausted its retries.
      if (policy === 'fail_fast') {
        // #29 then #26: unwind completed steps in reverse, then route the run to
        // the dead-letter queue. Compensations run BEFORE the status flips so
        // the run is still `running`/live while rollback executes; deadLetterRun
        // closes it out.
        await runCompensations(db, runId, completed)
        const reason = `step "${step.name}" failed after ${step.max_attempts} attempt(s): ${errorMessage(outcome.error)}`
        return deadLetterAndReturn(db, runId, reason)
      }

      // #28 continue_on_error: the step row is already `failed`; do NOT stop.
      // Fall through to the next ready sibling, then re-advance the DAG — steps
      // that don't depend on this one keep going.
    }
  }
}

/** Extract a human-readable message from a serialized error for a DLQ reason. */
export function errorMessage(error: SerializedError): string {
  if (error && typeof error === 'object' && 'message' in error && typeof error.message === 'string') {
    return error.message
  }
  return 'unknown error'
}

/**
 * #26: park a terminally-failed run in the dead-letter queue and return the
 * `dead_letter` result. Also wakes any parent step blocked on this run as a
 * child (#20), mirroring the `failed`/`completed` finalizers — a run that dies
 * must release whoever awaits it just as surely as one that succeeds.
 */
async function deadLetterAndReturn(db: Db, runId: string, reason: string): Promise<RunResult> {
  const dead = await deadLetterRunAndWake(db, runId, reason)
  return { runId, status: dead?.status ?? 'dead_letter' }
}

/**
 * #26: the shared dead-letter write — flip the run to `dead_letter`, log it,
 * and wake any parent step blocked on this run as a child (#20). Shared by the
 * inline driver (via `deadLetterAndReturn`) and the durable worker path
 * (worker.ts's `commitOutcome`), so the two agree on exactly what "route the
 * run to the DLQ" means. `sql` may be a bare `db` (inline) or an open
 * transaction (the worker, committing the failing step's outcome and the
 * dead-letter transition atomically). Returns the updated run row, if any.
 *
 * Note it does NOT cancel the run's still-pending steps — the inline driver
 * has no concurrent claimers so it doesn't need to, and the worker calls
 * `cancelPendingSteps` itself right after (its siblings can be claimed by
 * other workers, so stopping them is worker-path-specific and left to the
 * caller).
 */
export async function deadLetterRunAndWake(
  sql: Db,
  runId: string,
  reason: string
): Promise<RunRow | undefined> {
  const dead = await deadLetterRun(sql, runId, reason)
  await insertHistory(sql, { runId, type: 'run.dead_lettered', data: { reason } })
  await wakeParentAwaiting(sql, runId)
  return dead
}

/**
 * #28: finalize a `continue_on_error` run that has at least one failed step as
 * `completed_with_errors`. Output is built exactly like the clean-completion
 * path (advanceRun), except failed steps — which have no decodable result —
 * contribute `undefined`, same as a skipped step.
 */
export async function finalizeWithErrors(db: Db, runId: string, steps: StepRow[]): Promise<RunResult> {
  const output: Record<string, unknown> = {}
  for (const step of steps) {
    if (step.status === 'completed') {
      const decoded = decodeResult<unknown>(step.result)
      output[step.name] = decoded.ok ? decoded.value : undefined
    } else {
      // skipped / failed / pending-behind-a-failed-dep: no committed value.
      output[step.name] = undefined
    }
  }

  await updateRunStatus(db, runId, 'completed_with_errors', { output, finishedAt: new Date() })
  await insertHistory(db, { runId, type: 'run.completed_with_errors', data: { output } })
  await wakeParentAwaiting(db, runId)
  return { runId, status: 'completed_with_errors', output }
}

/**
 * #29: run the saga compensations for the given completed steps in REVERSE
 * completion order (LIFO — the last side effect performed is the first undone).
 *
 * Idempotency: each step's rollback is gated by `hasCompensationRun`, so a
 * re-driven run (revived out of dead-letter, then failing again) never re-issues
 * a refund that a prior drive already issued. The outcome is then written to the
 * compensation log via `recordCompensation`, whose `on conflict do nothing`
 * makes the whole thing safe even if two unwinds race.
 *
 * NOTE (deviation from strict insert-then-act): the committed compensation log
 * is insert-only — there is no UPDATE — so recording an accurate `'failed'`
 * status when a compensation THROWS requires knowing the outcome before the
 * insert (act-then-record). We therefore run the action first, then record. The
 * exactly-once property the shipping example needs is preserved for the
 * sequential drive/re-drive path by the `hasCompensationRun` gate; the only
 * window act-then-record leaves open is a crash strictly between a compensation's
 * side effect and its log write, which an insert-only log cannot close without
 * losing the ability to record failure status.
 */
async function runCompensations(
  db: Db,
  runId: string,
  completed: CompletedCompensation[]
): Promise<void> {
  for (let i = completed.length - 1; i >= 0; i--) {
    const c = completed[i]!
    if (c.fns.length === 0) continue

    // A prior drive already rolled this step back — never run it twice.
    if (await hasCompensationRun(db, runId, c.stepName)) continue

    let status: 'executed' | 'failed' = 'executed'
    let error: SerializedError | undefined
    let result: unknown
    try {
      // Several compensations on one step unwind LIFO too.
      for (let j = c.fns.length - 1; j >= 0; j--) {
        result = await c.fns[j]!()
      }
    } catch (e) {
      status = 'failed'
      error = serializeError(e)
    }

    await recordCompensation(db, {
      runId,
      stepName: c.stepName,
      stepId: c.stepId,
      status,
      result: status === 'executed' ? result : undefined,
      error,
    })
    await insertHistory(db, {
      runId,
      stepId: c.stepId,
      type: status === 'executed' ? 'compensation.executed' : 'compensation.failed',
      data: { step: c.stepName, error: error ?? null },
    })
  }
}

/**
 * Run one step, RETRYING up to its `max_attempts` budget, and report the
 * outcome. This function owns the step row and the STEP-level persistence
 * (completeStep / failStep) plus the retry loop (#26's "exhausts its retry
 * budget" is exactly this loop running out); it deliberately does NOT touch the
 * RUN status — that decision is policy-dependent (#28) and lives in the caller,
 * `executeRun`. Returns:
 *   - `completed` (+ the step's registered saga compensations, #29),
 *   - `suspended` (an event-wait parked the step), or
 *   - `failed` (the retry budget is spent — a terminal step failure).
 */
async function runStep(
  db: Db,
  handle: WorkflowHandle,
  run: RunRow,
  step: StepRow
): Promise<StepOutcome> {
  const fn = handle.stepFns.get(step.name)
  if (!fn) {
    // A missing implementation is not retryable — fail the step once and let
    // the caller apply the run's failure policy.
    const error = serializeError(
      new Error(`executeRun: no step function registered for step "${step.name}"`)
    )
    await withTransaction(db, async (tx) => {
      await failStep(tx, step.id, error)
      await insertHistory(tx, { runId: run.id, stepId: step.id, type: 'step.failed', data: { error } })
    })
    return { kind: 'failed', error }
  }

  while (true) {
    // markStepRunning bumps `attempt` (attempts used, 1-based) and returns the
    // fresh row, so `running.attempt` is what we test the retry budget against.
    const running = await markStepRunning(db, step.id)

    const ctx = createWorkflowContext({
      runId: run.id,
      input: run.input,
      stepId: step.id,
      // #18: replay an already-satisfied waitForEvent from its delivered
      // payload rather than re-suspending. Set once a prior wait was woken.
      eventSeq: step.event_seq,
      eventPayloads: step.event_payloads,
    })

    // Run the user function OUTSIDE a transaction (it may be slow / call out
    // to the world); only the persistence of its outcome is atomic.
    try {
      const value = await fn(ctx)
      // #29: capture the step's rollback registrations BEFORE committing, so
      // the caller can unwind them if a LATER step fails the run under
      // fail_fast. A step that completes cleanly never runs its own
      // compensation — it only runs if a sibling downstream later dooms the run.
      const compensations = getCompensations(ctx)
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
      return { kind: 'completed', compensations }
    } catch (e) {
      // #18: an event-wait is control flow, not a failure. Park the step in
      // `blocked` (no lease to fence in this single-process path) and stop
      // driving the run — executeRun's caller resumes it after the event is
      // published. A publish that races the block (e.g. from the HTTP trigger
      // server while this step was executing) is handled by the same backstop
      // the worker path uses: after writing the block, re-check for a matching
      // event and wake in place if one already landed.
      if (isEventWaitSignal(e)) {
        await withTransaction(db, async (tx) => {
          const blocked = await registerStepEventWait(tx, {
            stepId: step.id,
            eventName: e.eventName,
            correlationKey: e.correlationKey,
          })
          if (blocked) {
            await insertHistory(tx, {
              runId: run.id,
              stepId: step.id,
              type: 'step.waiting_for_event',
              data: { event: e.eventName, correlationKey: e.correlationKey ?? null, seq: e.seq },
            })
            // Throw->block race backstop (same as worker.ts commitEventWait): an event
            // published while this step was executing would have found the step still
            // 'running' and woken nothing. After writing the block, check for a match
            // at or after this attempt began and wake in place if one already landed.
            const already = await findMatchingEventSince(tx, {
              name: e.eventName,
              correlationKey: e.correlationKey,
              since: step.updated_at,
            })
            if (already) {
              await wakeStepsWaitingForEvent(tx, {
                name: already.name,
                correlationKey: already.correlation_key ?? undefined,
                payload: already.payload,
              })
            }
          }
        })
        return { kind: 'suspended' }
      }

      const error = serializeError(e)

      // #26 retry budget: if attempts remain, put the step back to `ready` and
      // try again on the next loop turn. The inline driver retries IMMEDIATELY
      // (no backoff sleep) — it is single-process and sequential, and retry.ts's
      // own note reserves durable backoff for the worker path. Only when
      // `shouldRetry` is false is this a terminal step failure.
      if (shouldRetry(running.attempt, step.max_attempts)) {
        await markStepReady(db, step.id)
        await insertHistory(db, {
          runId: run.id,
          stepId: step.id,
          type: 'step.retrying',
          data: { error, attempt: running.attempt, maxAttempts: step.max_attempts },
        })
        continue
      }

      await withTransaction(db, async (tx) => {
        await failStep(tx, step.id, error)
        await insertHistory(tx, { runId: run.id, stepId: step.id, type: 'step.failed', data: { error } })
      })
      return { kind: 'failed', error }
    }
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
