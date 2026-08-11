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
// *** Design note — read this before wiring the 'blocked' step status ***
//
// Unit A's storage layer (0004_orchestration.sql) built a dedicated
// `'blocked'` step status plus `awaited_child_run_id`, `blockStepOnChildRun`,
// and `resolveBlockedStepForChildRun`, deliberately mirroring Phase 3's
// `sleepStep` exactly: `blockStepOnChildRun` is fenced on
// `status = 'running' AND lease_owner = workerId`, the same way `sleepStep`
// is. Using it for real needs the step's own `stepId` and the *worker's*
// `workerId` at the exact moment the step suspends — and those two values
// only exist inside src/worker/worker.ts's `runClaimedStep` closure, which
// is the sole call site that builds a `WorkflowContext` (via
// `createWorkflowContext`). That call site does not pass `stepId`/
// `workerId` into the context today, and recognizing a new "spawn blocked
// on a child" signal in its Attempt classification switch (a
// `commitChildBlock` mirroring `commitSleep`) is what would actually call
// `blockStepOnChildRun` with proper fencing. `src/worker/worker.ts` is
// outside this unit's file allowlist, so that hook is NOT wired — see the
// Phase 4 report for the precise, minimal change it would take.
//
// Rather than reach into that file, child-await here is built entirely out
// of the ALREADY fully-wired `ctx.sleep()` primitive (Phase 3): spawn the
// child once — idempotently, see `spawnChildRun` — then poll
// `getChildOutcome` on a sleep backoff until the child reaches a terminal
// status. Every property this feature has to prove is already true of
// `ctx.sleep()`/`sleepStep`: the parent's lease is genuinely released while
// the child runs (sleepStep clears `lease_owner`/`lease_expires_at`), a
// sleeping step's `run_after` survives the spawning process dying (it's a
// row, not a timer — Phase 3 doc §1), and sleeping costs no retry attempt
// (`sleepStep`'s `attempt = greatest(attempt - 1, 0)`). This file adds zero
// new crash-proofing machinery; it composes an already-proven one. The
// `'blocked'` status / `awaited_child_run_id` column are consequently not
// exercised by this implementation — a parent awaiting a child currently
// shows up as an ordinary sleeping step, not a distinctly-labelled
// "blocked" one.

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
import { nextChildCallSeq } from '../define/context.ts'
import type { WorkflowHandle } from '../define/workflow.ts'
import {
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
   * replay (the step function re-running from the top after each poll
   * wakes it) spawns each child at most once. Defaults to an
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
 * only job is making sure a step that re-runs after waking from a poll
 * sleep doesn't spawn a second child.
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
  // poll-sleep forever waiting on a child no worker can see. Only look this
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
 * flight — nothing here blocks; callers poll this (see `awaitChildRun`)
 * rather than holding a connection open waiting.
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
  /** Backoff between polls while the child is still in flight. Default 1s. */
  pollIntervalMs?: number
}

/**
 * Wait for a child run to reach a terminal outcome, releasing the parent
 * step's lease between checks via `ctx.sleep` (see this file's module doc
 * for why that, and not the dedicated `'blocked'` status, is the
 * suspension mechanism used here). This loop is what makes the wait
 * crash-proof: on every replay it re-checks the outcome BEFORE sleeping
 * again, so a child that finished while the parent process was dead (or
 * simply between polls) is observed on the very next claim, by whichever
 * worker picks the row up — nothing about resolution depends on the
 * process that called `spawnChildRun` still being alive.
 *
 * Each loop iteration that finds the child still in flight calls
 * `ctx.sleep(pollIntervalMs)` — a NEW sleep call, distinct from whichever
 * ones this step already served (Phase 3's `sleep_seq` replay contract),
 * so it only ever actually suspends once per execution: every previously
 * -served sleep in this loop resolves in place near-instantly on replay,
 * and the loop keeps checking the outcome each time before deciding
 * whether it needs to ask for a new one.
 */
export async function awaitChildRun(
  db: Db,
  ctx: WorkflowContext,
  childRunId: string,
  options: AwaitChildOptions = {}
): Promise<ChildOutcome> {
  const pollIntervalMs = options.pollIntervalMs ?? 1_000
  while (true) {
    const outcome = await getChildOutcome(db, childRunId)
    if (outcome) return outcome
    // Throws (suspends) unless this poll's sequence number was already
    // served on a prior execution of this step, in which case it resolves
    // immediately and the loop re-checks the outcome before asking again.
    await ctx.sleep(pollIntervalMs)
  }
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
