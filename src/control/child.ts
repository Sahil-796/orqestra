// Phase 4 #20: child workflows — the impure half. Spawning and awaiting a
// child run needs Db access that `WorkflowContext` deliberately does not
// carry (ctx stays a plain, serializable-safe object — same posture as
// engine/sleep.ts and engine/timeout.ts), so everything here is a plain
// function that takes `db` explicitly, the same way a step author would
// pass their own `db` to any other side-effecting call. A step spawns and
// awaits a child like this:
//
//   builder.step('fan-to-child', async (ctx) => {
//     return runChildWorkflow(db, ctx, childHandle, { input: ctx.input })
//   })
//
// where `db` is whatever `Db` the surrounding application already has in
// scope (the same one passed to `createWorker`).
//
// *** How the wait works: event-driven, not polled ***
//
// A parent step awaiting a child suspends EXACTLY ONCE and is woken by the
// child's terminal transition — there is no timer and no poll interval in
// this path at all. The three moving parts:
//
//   1. `awaitChildRun` reads the child's outcome once. If the child is not
//      terminal yet it throws a `ChildBlockSignal` (engine/child.ts) — the
//      same species of control-flow signal as Phase 3's `SleepSignal`, and
//      handled the same way.
//   2. src/worker/worker.ts classifies that signal in its Attempt switch and
//      calls `commitChildBlock`, which persists `blockStepOnChildRun` —
//      status `'blocked'`, `awaited_child_run_id` set, lease cleared —
//      fenced on `lease_owner` inside the transaction, exactly as
//      `commitSleep` fences `sleepStep`. The worker slot is given back; a
//      `blocked` step is invisible to `claimNextStep` (it only looks at
//      `ready`) and to the lease reaper (`findExpiredLeases` only looks at
//      `running`), so nothing can resurrect it early.
//   3. Every path that drives a run to a terminal status calls
//      `resolveBlockedStepForChildRun(sql, childRunId)` — engine/dag.ts's
//      `maybeFinalizeRun`, the worker's failure and cancellation commits,
//      the inline executor's finalize/fail paths, plus a reconciliation
//      sweep (`sweepBlockedChildAwaits`) the worker runs on the same tick as
//      the reclaim sweep. That flips the parent step back to `ready`, and
//      the next worker to claim it replays the step function from the top,
//      where `getChildOutcome` now returns and the signal is never thrown
//      again. Resolution is a property of the child run's row, not of the
//      process that spawned it: whichever worker finishes the child wakes
//      the parent, even if the spawning process is long dead.
//
// The ordering race — "the child finishes between step 1's read and step 2's
// write, so the resolver looks for a `blocked` step that isn't there yet" —
// is closed with a lock, not a retry: `commitChildBlock` takes `FOR UPDATE`
// on the CHILD run row before it writes, and every resolver runs its
// `resolveBlockedStepForChildRun` in the same transaction that flipped the
// child run's status (i.e. while holding that same row lock) or strictly
// after it committed. So the two are serialized: either the block lands
// first and the finalizer sees it, or the finalizer lands first and the
// block transaction re-reads the now-terminal status and un-blocks itself
// in place before committing.
//
// Note the inline driver (engine/executor.ts's `startRun`/`executeRun`) does
// not support child workflows, the same way it does not support `ctx.sleep`:
// it is single-process and sequential, so nothing would ever run the child.
// A `ChildBlockSignal` there is treated as an ordinary step failure.

import { withTransaction, type Db } from '../store/client.ts'
import {
  createRun,
  getRun,
  getStepsByRun,
  insertHistory,
  insertSteps,
  type NewStep,
} from '../store/repositories.ts'
import type { WorkflowContext } from '../define/context.ts'
import { getContextStepId, nextChildCallSeq } from '../define/context.ts'
import type { WorkflowHandle } from '../define/workflow.ts'
import {
  ChildBlockSignal,
  classifyChildRun,
  isTerminalRunStatus,
  toChildWorkflowError,
  type ChildOutcome,
} from '../engine/child.ts'
import type { SerializedError } from '../types.ts'

export interface SpawnChildOptions {
  input?: unknown
  namespace?: string
  priority?: number
  /**
   * Distinguishes multiple children spawned by the same parent step, so a
   * replay (the step function re-running from the top once the child it was
   * blocked on resolves) spawns each child at most once. Defaults to an
   * auto-incrementing per-context counter (`nextChildCallSeq`) keyed on
   * call order — supply your own explicit key when a step's children
   * aren't spawned in a fixed order across attempts (e.g. inside a loop
   * whose iteration count can vary).
   */
  key?: string
}

export interface SpawnChildResult {
  runId: string
  created: boolean
}

/**
 * Idempotently create a child run linked to the parent (`run.parent_run_id`
 * via `createRun`'s Phase 4 fields). Mirrors engine/executor.ts's internal
 * `registerAndCreateRun` (register the workflow, idempotently create the
 * run, materialize one step row per DAG step) — duplicated here rather than
 * imported because that function isn't exported and executor.ts is outside
 * this unit's allowlist; keep the two in sync if either changes.
 *
 * The idempotency key is derived from the parent run + a caller/auto
 * -assigned `key`, not used as a Phase 1 #8 business idempotency key — its
 * only job is making sure a step that re-runs after being woken from a
 * child block doesn't spawn a second child.
 */
export async function spawnChildRun(
  db: Db,
  parentCtx: WorkflowContext,
  childHandle: WorkflowHandle,
  options: SpawnChildOptions = {}
): Promise<SpawnChildResult> {
  const key = options.key ?? `#${nextChildCallSeq(parentCtx)}`
  const idempotencyKey = `child:${parentCtx.runId}:${childHandle.name}:${key}`

  // Default the child to the PARENT's namespace, not `createRun`'s own
  // 'default' fallback: a worker pool is scoped to a namespace
  // (claimStep/queue/claim.ts filters on it), and a child that landed in
  // 'default' while its parent's pool watches some other namespace would
  // spawn a row nothing in that pool ever claims — the parent would then
  // block forever waiting on a child no worker can see. Only look this
  // up when the caller didn't already pick a namespace.
  const namespace = options.namespace ?? (await getRun(db, parentCtx.runId))?.namespace

  const workflow = await childHandle.register(db)
  const { run, created } = await createRun(db, {
    workflowId: workflow.id,
    namespace,
    priority: options.priority,
    input: options.input,
    idempotencyKey,
    parentRunId: parentCtx.runId,
    // Undefined outside a worker (the context carries it only when
    // worker.ts built it), which is exactly when there is no step row to
    // point at anyway — the column stays null, as it did before.
    parentStepId: getContextStepId(parentCtx),
  })

  if (!created) return { runId: run.id, created: false }

  const steps: NewStep[] = childHandle.definition.steps.map((step) => ({
    name: step.name,
    dependsOn: step.dependsOn,
    maxAttempts: step.maxAttempts,
    timeoutMs: step.timeoutMs,
    priority: step.priority,
    status: step.dependsOn.length === 0 ? 'ready' : 'pending',
  }))

  await withTransaction(db, async (tx) => {
    await insertSteps(tx, run.id, steps)
    await insertHistory(tx, {
      runId: run.id,
      type: 'run.created',
      data: { input: options.input, parentRunId: parentCtx.runId },
    })
  })

  return { runId: run.id, created: true }
}

/**
 * Read a child run's outcome. Returns `undefined` while it's still in
 * flight — nothing here blocks or waits; `awaitChildRun` calls this once
 * per execution of the parent step and suspends the step if it's still
 * undefined, rather than holding a connection open waiting.
 */
export async function getChildOutcome(db: Db, childRunId: string): Promise<ChildOutcome | undefined> {
  const run = await getRun(db, childRunId)
  if (!run) throw new Error(`getChildOutcome: no run found for id "${childRunId}"`)
  if (!isTerminalRunStatus(run.status)) return undefined

  if (run.status === 'completed') {
    return classifyChildRun(run.status, run.output, undefined)
  }

  // failed/cancelled: `run` itself carries no error column (only `output`
  // on success), so find the step(s) that ended the run and surface the
  // first one's `error` — the same `SerializedError` worker.ts's own
  // commitOutcome/commitCancellation persist onto the step row.
  const steps = await getStepsByRun(db, childRunId)
  const failedStep = steps.find((s) => s.status === 'failed' || s.status === 'cancelled')
  const error = failedStep?.error as SerializedError | undefined
  return classifyChildRun(run.status, undefined, error)
}

export interface AwaitChildOptions {
  /**
   * @deprecated Accepted and ignored. The wait is event-driven — the parent
   * step suspends once and is woken by the child's terminal transition, so
   * there is no poll interval to tune. Kept on the type so callers written
   * against the original polling implementation still compile.
   */
  pollIntervalMs?: number
}

/**
 * Wait for a child run to reach a terminal outcome, releasing the parent
 * step's worker while it waits.
 *
 * There is no loop and no timer: read the outcome once, and if the child is
 * still in flight throw a `ChildBlockSignal`, which the worker turns into a
 * fenced `blockStepOnChildRun` commit (see this file's module doc for the
 * full path and the race analysis). The step is re-claimed only after the
 * child actually reaches a terminal status, at which point this same call
 * returns instead of throwing — so on the resumed execution the step
 * function replays straight through to the outcome.
 *
 * Crash-proofing comes from the same place `ctx.sleep`'s does: the parent's
 * state is a row (`status = 'blocked'`, `awaited_child_run_id`), not a timer
 * or a held connection, and the wake is written by whichever worker finishes
 * the child. Nothing about resolution depends on the process that called
 * `spawnChildRun` still being alive.
 */
export async function awaitChildRun(
  db: Db,
  ctx: WorkflowContext,
  childRunId: string,
  _options: AwaitChildOptions = {}
): Promise<ChildOutcome> {
  const outcome = await getChildOutcome(db, childRunId)
  if (outcome) return outcome
  throw new ChildBlockSignal(childRunId)
}

/** `runChildWorkflowResult`'s return shape — never throws on child failure. */
export type ChildRunResult<T = unknown> =
  | { ok: true; childRunId: string; value: T }
  | { ok: false; childRunId: string; status: 'failed' | 'cancelled'; error: SerializedError | undefined }

/**
 * Spawn (idempotently) and await a child workflow, returning its raw
 * outcome instead of throwing on failure — use this when the parent step
 * wants to inspect or compensate for a failed/cancelled child itself
 * rather than have it fail the parent step. See `runChildWorkflow` for the
 * default (throwing) policy.
 */
export async function runChildWorkflowResult<T = unknown>(
  db: Db,
  ctx: WorkflowContext,
  childHandle: WorkflowHandle,
  options: SpawnChildOptions & AwaitChildOptions = {}
): Promise<ChildRunResult<T>> {
  const { runId: childRunId } = await spawnChildRun(db, ctx, childHandle, options)
  const outcome = await awaitChildRun(db, ctx, childRunId, options)
  return outcome.ok
    ? { ok: true, childRunId, value: outcome.value as T }
    : { ok: false, childRunId, status: outcome.status, error: outcome.error }
}

/**
 * Spawn and await a child workflow, resolving with its output.
 *
 * **Propagation policy (default): a failed or cancelled child fails the
 * parent step.** `runChildWorkflow` throws `ChildWorkflowError` when the
 * child ends `failed`/`cancelled`, exactly like any other error a step
 * body might throw — it goes through the parent step's ordinary
 * retry/backoff or permanent-failure handling unchanged, with no special
 * casing anywhere in the commit path. This matches how every other
 * dependency a step calls out to already behaves (a thrown error is a
 * thrown error), and keeps "a child failed" from silently vanishing.
 *
 * Callers who want to handle a failed child themselves (compensate,
 * fall back, aggregate several children's results) should use
 * `runChildWorkflowResult` instead, which never throws.
 */
export async function runChildWorkflow<T = unknown>(
  db: Db,
  ctx: WorkflowContext,
  childHandle: WorkflowHandle,
  options: SpawnChildOptions & AwaitChildOptions = {}
): Promise<T> {
  const result = await runChildWorkflowResult<T>(db, ctx, childHandle, options)
  if (result.ok) return result.value
  throw toChildWorkflowError(result.childRunId, { ok: false, status: result.status, error: result.error })
}
