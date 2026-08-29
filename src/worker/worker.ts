// The long-running process loop: claim a step off the queue, run it,
// commit its outcome. This is the Phase 2 counterpart to Phase 1's
// executeRun loop — the difference is everything here has to survive N
// *other* processes doing the exact same thing at the same time, which is
// why every write that matters is fenced (see `commitOutcome` below) rather
// than assumed safe because "we're the only ones touching this row".
//
// A worker never talks to `postgres` directly — only through
// repositories.ts / queue/claim.ts / queue/lease.ts, same storage boundary
// as engine/.
//
// Phase 3 adds three ways a claimed step can end other than "returned" or
// "threw", all of them landing in this loop because this is the only place
// that owns a lease while user code runs:
//   #9  sleep     — the step asks to be suspended; we hand the row back to the
//                   queue with a future run_after and release the worker.
//   #10 timeout   — the step blows step.timeout_ms; it goes through the normal
//                   failure/retry path, labelled so the timeline can tell why.
//   #11 cancel    — the run was cancelled; the worker holding the lease is the
//                   one that closes out its own step, cooperatively.
// None of them weaken Phase 2's fencing: each is a fenced write in its own
// transaction, and a worker that lost its lease writes nothing at all.
//
// Phase 4 adds a fourth (#20, child workflows): a step that awaits a child
// run suspends into `blocked` (commitChildBlock, the sibling of commitSleep)
// and is woken by the child's terminal transition rather than by a clock.
// The wake side lives in every path here that drives a run terminal — see
// wakeParentAwaiting's call sites in commitOutcome/commitCancellation and in
// engine/dag.ts's maybeFinalizeRun.

import { hostname } from 'node:os'
import { loadConfig } from '../config.ts'
import type { Db } from '../store/client.ts'
import { withTransaction } from '../store/client.ts'
import { claimStep } from '../queue/claim.ts'
import { heartbeatLease, releaseLease, reclaimExpiredLeases } from '../queue/lease.ts'
import { advanceDag, sweepBlockedChildAwaits, wakeParentAwaiting } from '../engine/dag.ts'
import {
  blockStepOnChildRun,
  cancelPendingSteps,
  cancelRunningStep,
  clearSleepMarker,
  completeStep,
  failStep,
  finalizeCancelledRun,
  findMatchingEventSince,
  getRun,
  getStepsByRun,
  getWorkflowById,
  isCancellationRequested,
  lockRun,
  lockStepIfOwner,
  markRunStarted,
  markStepReady,
  registerStepEventWait,
  retryStep,
  insertHistory,
  sleepStep,
  upsertWorkerHeartbeat,
  wakeStepsWaitingForEvent,
  type RunRow,
  type StepRow,
} from '../store/repositories.ts'
import { serializeError, type FailurePolicy } from '../types.ts'
import { deadLetterRunAndWake, errorMessage, finalizeWithErrors } from '../engine/executor.ts'
import { createWorkflowContext, getSkipRequests, type WorkflowContext } from '../define/context.ts'
import type { WorkflowHandle } from '../define/workflow.ts'
import { DEFAULT_RETRY_POLICY, nextRunAfter, shouldRetry, type RetryPolicy } from '../engine/retry.ts'
import { isChildBlockSignal, isTerminalRunStatus, type ChildBlockSignal } from '../engine/child.ts'
import { isEventWaitSignal, type EventWaitSignal } from '../engine/event.ts'
import { isSleepSignal, type SleepSignal } from '../engine/sleep.ts'
import { isStepTimeoutError, withTimeout } from '../engine/timeout.ts'
import { createLogger, createRunLogger, type Logger } from '../observability/logger.ts'

export interface WorkerOptions {
  db: Db
  /** Workflows this worker knows how to run. Accepts either shape; internally keyed by workflow name. */
  handles: Map<string, WorkflowHandle> | WorkflowHandle[]
  /** Defaults to `${hostname}-${pid}-${short random}` — must be unique per running worker process. */
  workerId?: string
  /** Max steps this worker runs at once. Defaults to `ORQ_WORKER_CONCURRENCY` (1). */
  concurrency?: number
  /** How long a claim holds before it's presumed crashed and reclaimable. Defaults to `ORQ_LEASE_TTL_MS` (30s). */
  leaseTtlMs?: number
  /** How often an in-flight step's lease is renewed. Default leaseTtlMs / 3. */
  heartbeatIntervalMs?: number
  /** How long to sleep after finding the queue empty. Defaults to `ORQ_POLL_INTERVAL_MS` (200ms). */
  pollIntervalMs?: number
  /** How often this worker scans for other workers' expired leases. Default 5s. */
  reclaimIntervalMs?: number
  retryPolicy?: RetryPolicy
  namespace?: string
}

export interface Worker {
  readonly id: string
  /** Non-blocking: kicks off the claim/run loop in the background. */
  start(): void
  /** Graceful: stop claiming, await whatever's in flight, then resolve. */
  stop(): Promise<void>
  /** How many steps this worker is currently running. */
  readonly inFlight: number
  /** Number of claim queries issued so far (bench-only counter; zero behavior change). */
  readonly claimAttempts: number
  /** Number of claim queries that actually returned a step to run (bench-only counter). */
  readonly claimsFound: number
}

function defaultWorkerId(): string {
  return `${hostname()}-${process.pid}-${crypto.randomUUID().slice(0, 8)}`
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

function normalizeHandles(
  input: Map<string, WorkflowHandle> | WorkflowHandle[]
): Map<string, WorkflowHandle> {
  if (input instanceof Map) return input
  const map = new Map<string, WorkflowHandle>()
  for (const handle of input) map.set(handle.name, handle)
  return map
}

// What resolveRunAndHandle found for a run_id, cached for the lifetime of
// one outer-loop iteration so a burst of claims against the same run (e.g.
// fan-out steps that all became ready together) doesn't re-query the run
// and workflow row once per step.
interface ResolvedRun {
  run: RunRow
  handle: WorkflowHandle | undefined
}

type StepOutcome =
  // `skipNames` (Phase 4 #17): sibling step names this step's function
  // handed to `ctx.skip(...)` before returning — the untaken branch,
  // applied atomically alongside this step's own completion.
  | { kind: 'success'; value: unknown; skipNames: readonly string[] }
  // `timedOut` only changes the history label — a timeout is an ordinary
  // failure as far as the retry/backoff decision is concerned (Phase 3 #10).
  | { kind: 'failure'; error: unknown; forcePermanent?: boolean; timedOut?: boolean }

// #32/#31 support: what commitOutcome actually did with a step, so the caller
// can emit a structured metrics/log line without commitOutcome itself having
// to know about logging (it stays focused on the fenced write). 'discarded'
// covers the lease-fencing-lost case — nothing was written, so nothing should
// be logged either.
type CommitResult = 'discarded' | 'success' | 'retried' | 'failed'

// What one execution of a step function turned into, before any of it is
// persisted. Phase 2 only had success/failure; Phase 3 adds two outcomes that
// are NOT failures and must never be routed through the retry path:
// `sleep` (the step asked to be suspended) and `cancelled` (the run was
// cancelled underneath it).
type Attempt =
  | { kind: 'success'; value: unknown; skipNames: readonly string[] }
  | { kind: 'sleep'; signal: SleepSignal }
  // Phase 4 #20: the step is awaiting a child run that hasn't finished. Like
  // `sleep` this is a suspension, not a failure — the difference is what
  // wakes it (the child's terminal transition, not a clock).
  | { kind: 'child-block'; signal: ChildBlockSignal }
  // Phase 5 #18: the step is waiting for a published event. Same family as
  // `sleep`/`child-block` — a suspension, not a failure; woken by a matching
  // publishEvent rather than a clock or a child's terminal transition.
  | { kind: 'event-wait'; signal: EventWaitSignal; since: Date }
  | { kind: 'cancelled' }
  | { kind: 'timeout'; error: unknown }
  | { kind: 'failure'; error: unknown }

export function createWorker(options: WorkerOptions): Worker {
  const db = options.db
  const handles = normalizeHandles(options.handles)
  const workerId = options.workerId ?? defaultWorkerId()
  // Explicit options win; otherwise fall back to the env-parsed defaults so
  // ORQ_LEASE_TTL_MS / ORQ_POLL_INTERVAL_MS / ORQ_WORKER_CONCURRENCY can tune
  // a deployed worker pool without a code change. config.ts owns the literal
  // defaults and the fail-fast parsing — never duplicate them here.
  const config = loadConfig()
  const concurrency = options.concurrency ?? config.workerConcurrency
  const leaseTtlMs = options.leaseTtlMs ?? config.leaseTtlMs
  const heartbeatIntervalMs = options.heartbeatIntervalMs ?? Math.floor(leaseTtlMs / 3)
  const pollIntervalMs = options.pollIntervalMs ?? config.pollIntervalMs
  const reclaimIntervalMs = options.reclaimIntervalMs ?? 5_000
  const retryPolicy = options.retryPolicy ?? DEFAULT_RETRY_POLICY
  const namespace = options.namespace
  const log: Logger = createLogger().child({ workerId })

  let started = false
  let stopping = false
  let loopDone: Promise<void> | undefined
  const inFlight = new Set<Promise<void>>()
  // Bench-only counters (#claimAttempts/#claimsFound on the returned Worker):
  // additive, no effect on control flow, timing, or the claim SQL itself.
  let claimAttempts = 0
  let claimsFound = 0

  async function resolveRunAndHandle(
    runId: string,
    cache: Map<string, ResolvedRun | undefined>
  ): Promise<ResolvedRun | undefined> {
    if (cache.has(runId)) return cache.get(runId)

    const run = await getRun(db, runId)
    if (!run) {
      cache.set(runId, undefined)
      return undefined
    }
    const workflow = await getWorkflowById(db, run.workflow_id)
    const handle = workflow ? handles.get(workflow.name) : undefined
    const resolved: ResolvedRun = { run, handle }
    cache.set(runId, resolved)
    return resolved
  }

  // The fenced write: re-assert this worker still owns the step's lease
  // (inside the tx, under FOR UPDATE) before writing anything. If ownership
  // moved on — a reclaim sweep or another worker got there first — nothing
  // is written and the outcome is discarded; that's correct, not a bug: the
  // step is someone else's problem now, and committing our stale outcome on
  // top of theirs would corrupt the log.
  async function commitOutcome(
    step: StepRow,
    run: RunRow,
    outcome: StepOutcome,
    failurePolicy: FailurePolicy
  ): Promise<CommitResult> {
    return withTransaction(db, async (tx): Promise<CommitResult> => {
      const owned = await lockStepIfOwner(tx, step.id, workerId)
      if (!owned) {
        log.warn('lease fencing failed at commit time — discarding outcome, another worker owns this step now', {
          stepId: step.id,
          runId: run.id,
        })
        return 'discarded'
      }

      // Lock the run row FIRST, before any statement that references
      // run_id (insertHistory, advanceRun's own insertHistory for
      // run.completed) — those inserts take an implicit FOR KEY SHARE lock
      // on the parent run row via the FK, and if two sibling steps of the
      // same run commit concurrently, each would pick up that weaker lock
      // before requesting advanceRun's FOR UPDATE, producing a textbook
      // lock-upgrade deadlock (each waiting on the other's FOR KEY SHARE).
      // Taking FOR UPDATE up front avoids the upgrade entirely: the second
      // transaction to reach this line simply waits here for the first to
      // commit, and its later reads then see the first's writes.
      await lockRun(tx, run.id)

      if (outcome.kind === 'success') {
        await completeStep(tx, step.id, outcome.value)
        await insertHistory(tx, {
          runId: run.id,
          stepId: step.id,
          type: 'step.completed',
          data: { result: outcome.value },
        })
        await releaseLease(tx, step.id)

        // Phase 4: fan-out/fan-in/dependency release (#15/#16/#19) via the
        // narrow per-row primitive, plus any conditional-branch skips this
        // step declared via ctx.skip() (#17) — see engine/dag.ts's module
        // doc for why this replaces the old advanceRun(tx, run.id) rescan
        // call here specifically (not in executor.ts's inline path, which
        // keeps using advanceRun).
        const { skippedSteps, run: finalized } = await advanceDag(tx, run.id, {
          completedName: step.name,
          skipNames: outcome.skipNames,
        })
        for (const skipped of skippedSteps) {
          await insertHistory(tx, {
            runId: run.id,
            stepId: skipped.id,
            type: 'step.skipped',
            data: { reason: skipped.skip_reason },
          })
        }
        // #28 continue_on_error: advanceDag only finalizes a run whose every
        // step is completed/skipped. When an EARLIER step of this run failed,
        // that never happens — so this successful commit may instead have been
        // the one that drained the last runnable work, leaving only
        // failed + completed/skipped steps. Finish it as
        // completed_with_errors. (fail_fast never reaches a drained-with-errors
        // state — a terminal failure dead-letters and cancels the rest inline.)
        if (!finalized && failurePolicy === 'continue_on_error') {
          await finalizeIfDrainedWithErrors(tx, run.id)
        }
        return 'success'
      }

      const error = serializeError(outcome.error)

      // Logged before the retry/fail decision so the timeline can tell "the
      // step blew its budget" apart from "the step threw", even though both
      // take exactly the same path from here on. Deliberately additive: the
      // step.retry_scheduled / step.failed row that follows is unchanged, so
      // nothing reading Phase 2's history shape breaks.
      if (outcome.timedOut) {
        await insertHistory(tx, {
          runId: run.id,
          stepId: step.id,
          type: 'step.timed_out',
          data: { attempt: owned.attempt, timeoutMs: owned.timeout_ms, error },
        })
      }

      if (!outcome.forcePermanent && shouldRetry(owned.attempt, owned.max_attempts)) {
        const runAfter = nextRunAfter(owned.attempt, new Date(), retryPolicy)
        await retryStep(tx, step.id, error, runAfter)
        await insertHistory(tx, {
          runId: run.id,
          stepId: step.id,
          type: 'step.retry_scheduled',
          data: { attempt: owned.attempt, nextRunAfter: runAfter, error },
        })
        return 'retried'
      }

      // Retries exhausted (or unretryable): a terminal step failure. What
      // happens to the RUN is policy-dependent (Phase 7 #28) — the same
      // decision the inline path (executor.ts's executeRun) makes, brought to
      // the durable worker path here. The step row itself is `failed` either
      // way; only the run-level transition differs by policy.
      await failStep(tx, step.id, error)
      await insertHistory(tx, { runId: run.id, stepId: step.id, type: 'step.failed', data: { error } })
      await releaseLease(tx, step.id)

      if (failurePolicy === 'continue_on_error') {
        // #28: do NOT fail or cancel the whole run on one terminal step
        // failure. Steps that don't depend on this one keep progressing via
        // their own commits' advanceDag. This failure may itself have drained
        // the last runnable work (a failed step never satisfies a dependent,
        // so its dependents stay `pending` forever) — if so, finish the run as
        // completed_with_errors (no rollback, no DLQ). Otherwise leave the run
        // `running`; a later sibling's commit finalizes it.
        await finalizeIfDrainedWithErrors(tx, run.id)
        return 'failed'
      }

      // fail_fast (#26): route the run to the dead-letter queue (status
      // `dead_letter`) instead of a bare `failed`. deadLetterRunAndWake also
      // wakes any parent step blocked on this run as a child (#20) — the wake
      // path already treats `dead_letter` as terminal — so a dead-lettered
      // child releases its parent exactly as a `failed` one did. Inside this
      // transaction on purpose (see dag.ts's wakeParentAwaiting).
      //
      // #29 (saga compensation) is deliberately NOT run here. Compensations are
      // in-memory closures registered by `ctx.compensate(...)` during a step's
      // execution; the worker drives ONE step in isolation and never replays
      // the workflow definition, so an earlier COMPLETED step's compensation
      // closure is not present in this process when a later step fails. Wiring
      // it would need a durable descriptor for `ctx.compensate` (genuine
      // distributed-saga work), out of scope for this change — see the report.
      const reason = `step "${step.name}" failed after ${step.max_attempts} attempt(s): ${errorMessage(error)}`
      await deadLetterRunAndWake(tx, run.id, reason)
      // cancelPendingSteps stops other workers from picking up the rest of a
      // run that's already dead — worker-path-specific (the inline driver has
      // no concurrent claimers), so it lives here rather than in the shared
      // deadLetterRunAndWake helper.
      await cancelPendingSteps(tx, run.id)
      return 'failed'
    })
  }

  // #31/#32: emits a best-effort structured log line describing what
  // commitOutcome just did — step id, run id, status, attempt, and wall-clock
  // duration of the attempt. Deliberately called AFTER commitOutcome's
  // transaction has already committed (or discarded), so a logging hiccup can
  // never affect run correctness — it only ever describes a write that
  // already happened. 'discarded' (lease lost) logs nothing: nothing was
  // actually written for this attempt.
  async function logStepCommit(
    step: StepRow,
    run: RunRow,
    result: CommitResult,
    outcome: StepOutcome,
    attemptStartedAt: number
  ): Promise<void> {
    if (result === 'discarded') return
    const durationMs = Date.now() - attemptStartedAt
    const status = result === 'success' ? 'completed' : result === 'retried' ? 'retry_scheduled' : 'failed'
    const runLog = createRunLogger(db, run.id, { workerId, stepId: step.id })
    const fields = {
      stepName: step.name,
      attempt: step.attempt,
      maxAttempts: step.max_attempts,
      durationMs,
      timedOut: outcome.kind === 'failure' ? Boolean(outcome.timedOut) : false,
    }
    if (result === 'failed') {
      runLog.warn(`step ${status}`, fields)
    } else {
      runLog.info(`step ${status}`, fields)
    }
  }

  // #28 continue_on_error, worker-path finalizer. A run under this policy never
  // dies on a single terminal step failure; it finishes as
  // completed_with_errors once no runnable work remains and at least one step
  // failed. "No runnable work" = no step is `ready`/`running`/`blocked` and no
  // `pending` step is satisfiable (all its deps completed/skipped) — the latter
  // guard is defensive: advanceDag flips satisfiable pendings to `ready` as
  // their deps resolve, so any lingering pending is behind a failed dep, but
  // checking keeps us from ever finalizing a run that still has a step to
  // release. Reuses the inline driver's finalizeWithErrors (which builds the
  // same output and wakes any awaiting parent) so both paths agree on what a
  // completed_with_errors run looks like. Call inside the committing tx.
  async function finalizeIfDrainedWithErrors(tx: Db, runId: string): Promise<void> {
    const steps = await getStepsByRun(tx, runId)

    const active = steps.some(
      (s) => s.status === 'ready' || s.status === 'running' || s.status === 'blocked'
    )
    if (active) return

    const statusByName = new Map(steps.map((s) => [s.name, s.status]))
    const satisfiablePending = steps.some(
      (s) =>
        s.status === 'pending' &&
        s.depends_on.every((d) => {
          const st = statusByName.get(d)
          return st === 'completed' || st === 'skipped'
        })
    )
    if (satisfiablePending) return

    if (!steps.some((s) => s.status === 'failed')) return

    await finalizeWithErrors(tx, runId, steps)
  }

  // Sleep (#9). Same fencing discipline as commitOutcome — a sleep is a write
  // that decides an outcome (it hands the step back to the queue with a future
  // due time), so a worker whose lease was reclaimed mid-flight must write
  // nothing rather than suspend a step its new owner is already running.
  //
  // What makes this cheap is what it does NOT do: no timer is set, no worker
  // is parked, no connection is held. sleepStep parks the *row* — `ready` with
  // `run_after = wakeAt` — and claimNextStep's existing `run_after <= now()`
  // gate hides it from the whole fleet until it's due. A 24h sleep costs a
  // timestamp in a column; every worker in the pool can be restarted during it
  // and the wake time survives, because it was never in anyone's memory.
  async function commitSleep(step: StepRow, run: RunRow, signal: SleepSignal): Promise<void> {
    await withTransaction(db, async (tx) => {
      const owned = await lockStepIfOwner(tx, step.id, workerId)
      if (!owned) {
        log.warn('lease fencing failed at sleep time — not suspending, another worker owns this step now', {
          stepId: step.id,
          runId: run.id,
        })
        return
      }
      await lockRun(tx, run.id)

      // sleepStep re-asserts the same fence in its own WHERE clause; under the
      // FOR UPDATE above it cannot miss, but it is the fence of record and a
      // missing row still means "write nothing".
      const slept = await sleepStep(tx, { stepId: step.id, workerId, wakeAt: signal.wakeAt })
      if (!slept) return

      await insertHistory(tx, {
        runId: run.id,
        stepId: step.id,
        type: 'step.sleeping',
        data: { wakeAt: signal.wakeAt, durationMs: signal.durationMs, seq: signal.seq },
      })
      // No releaseLease: sleepStep already cleared lease_owner/expires_at as
      // part of handing the row back to the queue.
    })
  }

  // Child block (#20). The sibling of commitSleep: same fencing, same
  // "hand the worker back" shape, different wake condition — a sleeping step
  // is `ready` with a future `run_after` and wakes on a clock, a blocked step
  // is `blocked` with `awaited_child_run_id` set and wakes only when the
  // child run it names reaches a terminal status. Neither is claimable in the
  // meantime, and neither is visible to the lease reaper (findExpiredLeases
  // only scans `running`), so nothing resurrects a blocked step as if its
  // worker had crashed.
  //
  // ## The lock order here is load-bearing
  //
  // The CHILD run row is locked FIRST — before this step's own fence, before
  // the parent run. That is what makes the block and the wake mutually
  // exclusive: every finalizer calls `wakeParentAwaiting` while holding (or
  // just after releasing) that same child-run row lock, so the two orderings
  // are the only two possible outcomes, and both are correct:
  //
  //   * block first  — the finalizer waits for this transaction, then sees a
  //                    `blocked` step and flips it back to `ready`.
  //   * finalize first — `lockRun` below waits for it and then reads the
  //                    now-terminal status, so this transaction blocks the
  //                    step and immediately un-blocks it in place, leaving it
  //                    `ready` for the next claim.
  //
  // Locking the child before the parent step (and the parent step before the
  // parent run, matching commitOutcome's existing step -> run order) also
  // means no transaction ever holds one of these rows while waiting on the
  // other in the opposite direction — i.e. no deadlock cycle.
  async function commitChildBlock(step: StepRow, run: RunRow, signal: ChildBlockSignal): Promise<void> {
    await withTransaction(db, async (tx) => {
      const child = await lockRun(tx, signal.childRunId)
      if (!child) {
        // The child run row is gone — nothing to wait for and nothing that
        // could ever wake us. Leave the step `running` and let the lease
        // expire into the reclaim sweep, which replays it; getChildOutcome
        // then throws and the step fails honestly instead of blocking on a
        // ghost.
        log.error('step blocked on a child run that does not exist — refusing to block', {
          stepId: step.id,
          runId: run.id,
          childRunId: signal.childRunId,
        })
        return
      }

      const owned = await lockStepIfOwner(tx, step.id, workerId)
      if (!owned) {
        log.warn('lease fencing failed at child-block time — not blocking, another worker owns this step now', {
          stepId: step.id,
          runId: run.id,
        })
        return
      }
      await lockRun(tx, run.id)

      // Re-asserts the same fence in its own WHERE clause (status running +
      // lease_owner), exactly like sleepStep: a missing row means "write
      // nothing".
      const blocked = await blockStepOnChildRun(tx, {
        stepId: step.id,
        workerId,
        childRunId: signal.childRunId,
      })
      if (!blocked) return

      await insertHistory(tx, {
        runId: run.id,
        stepId: step.id,
        type: 'step.blocked',
        data: { childRunId: signal.childRunId },
      })

      if (isTerminalRunStatus(child.status)) {
        // The child finished in the window between the step reading its
        // outcome and this commit. Resolve the block we just wrote rather
        // than leaving a step waiting on an event that already happened.
        const woken = await wakeParentAwaiting(tx, signal.childRunId)
        if (woken) {
          log.info('child was already terminal at block time — step is ready again immediately', {
            stepId: step.id,
            runId: run.id,
            childRunId: signal.childRunId,
          })
        }
      }
      // No releaseLease: blockStepOnChildRun already cleared
      // lease_owner/lease_expires_at as part of parking the row.
    })
  }

  // Event wait (#18). The sibling of commitSleep/commitChildBlock: same
  // fencing, same "hand the worker back" shape. A waiting step is `blocked`
  // with `waiting_event_name` set and wakes only when a matching
  // `publishEvent` arrives — never a clock, never the reclaim sweep
  // (findExpiredLeases only scans `running`).
  //
  // ## Closing the throw->block race
  //
  // `publishEvent` wakes only steps that are already `blocked`. An event
  // published in the window between the step throwing its signal and this
  // commit writing the `blocked` row would be missed by that live wake — the
  // step is still `running`. So after writing the block, this checks the
  // events log for a match that landed at or after this attempt began
  // (`signal.since` = the step's claim time, a DB timestamp — no clock skew)
  // and wakes itself in place if so.
  //
  // That backstop and `publishEvent`'s live wake are mutually exclusive per
  // event, which is what keeps the wake exactly-once. `registerStepEventWait`
  // holds this step's row lock from its write through this transaction's
  // commit, and `publishEvent`'s wake needs that same row lock, so the two
  // serialize: either the publish committed first (its event is visible to
  // the backstop, which wakes; its own wake found the step not yet blocked
  // and did nothing), or it runs after this commit (it wakes the now-blocked
  // step; the backstop, having seen no committed event, did nothing).
  async function commitEventWait(step: StepRow, run: RunRow, signal: EventWaitSignal, since: Date): Promise<void> {
    await withTransaction(db, async (tx) => {
      const owned = await lockStepIfOwner(tx, step.id, workerId)
      if (!owned) {
        log.warn('lease fencing failed at event-wait time — not blocking, another worker owns this step now', {
          stepId: step.id,
          runId: run.id,
        })
        return
      }
      await lockRun(tx, run.id)

      const blocked = await registerStepEventWait(tx, {
        stepId: step.id,
        workerId,
        eventName: signal.eventName,
        correlationKey: signal.correlationKey,
      })
      if (!blocked) return

      await insertHistory(tx, {
        runId: run.id,
        stepId: step.id,
        type: 'step.waiting_for_event',
        data: { event: signal.eventName, correlationKey: signal.correlationKey ?? null, seq: signal.seq },
      })

      const already = await findMatchingEventSince(tx, {
        name: signal.eventName,
        correlationKey: signal.correlationKey,
        since,
      })
      if (already) {
        const woken = await wakeStepsWaitingForEvent(tx, {
          name: already.name,
          correlationKey: already.correlation_key ?? undefined,
          payload: already.payload,
        })
        if (woken.length > 0) {
          await insertHistory(tx, {
            runId: run.id,
            stepId: step.id,
            type: 'step.event_delivered',
            data: { event: already.name, eventId: already.id, raced: true },
          })
          log.info('event had already been published at block time — step is ready again immediately', {
            stepId: step.id,
            runId: run.id,
            event: signal.eventName,
          })
        }
      }
      // No releaseLease: registerStepEventWait already cleared the lease as
      // part of parking the row.
    })
  }

  // Cancellation (#11), the step half. Cancellation is cooperative by design:
  // the canceller only writes `cancel_requested_at` and never touches a
  // `running` step, because doing so would be exactly the unfenced double-write
  // Phase 2's commit fencing exists to make impossible. The worker that owns
  // the lease is the only process allowed to close out its own step — so this
  // runs here, fenced, and only then finalizes the run.
  async function commitCancellation(step: StepRow, run: RunRow, phase: 'pre-run' | 'in-flight'): Promise<void> {
    const cancelled = await withTransaction(db, async (tx) => {
      const owned = await lockStepIfOwner(tx, step.id, workerId)
      if (!owned) {
        log.warn('lease fencing failed at cancel time — leaving this step to its new owner', {
          stepId: step.id,
          runId: run.id,
        })
        return false
      }
      // Same lock-first ordering as commitOutcome: take the run's FOR UPDATE
      // before any insertHistory (which would otherwise acquire the weaker
      // FOR KEY SHARE via the FK and set up a lock-upgrade deadlock with a
      // sibling step committing concurrently).
      await lockRun(tx, run.id)

      const row = await cancelRunningStep(tx, { stepId: step.id, workerId })
      if (!row) return false

      await insertHistory(tx, {
        runId: run.id,
        stepId: step.id,
        type: 'step.cancelled',
        data: { phase, workerId },
      })
      return true
    })

    if (!cancelled) return

    // Separate transaction on purpose: finalizeCancelledRun opens its own (it
    // has to lock the run to flip the rest of the steps atomically), and the
    // step above is already durably out of `running` by the time we get here,
    // so there is nothing left in flight for this run from this worker.
    const { run: finalized, cancelledSteps } = await finalizeCancelledRun(db, run.id)
    if (finalized) {
      // #20: a cancelled run is terminal, so anything blocked on it as a
      // child wakes here. Strictly after finalizeCancelledRun's transaction
      // committed — which is exactly the ordering wakeParentAwaiting
      // requires, since a concurrent commitChildBlock could only have gone
      // either wholly before that transaction (its `blocked` write is
      // visible to us now) or wholly after it (it reads `cancelled` and
      // un-blocks itself).
      await wakeParentAwaiting(db, run.id)
      await insertHistory(db, {
        runId: run.id,
        type: 'run.cancelled',
        data: { reason: 'cancel_requested', stepId: step.id, cancelledSteps: cancelledSteps.length },
      })
      log.info('run cancelled', { runId: run.id, stepId: step.id, cancelledSteps: cancelledSteps.length })
    }
  }

  async function runClaimedStep(
    step: StepRow,
    cache: Map<string, ResolvedRun | undefined>
  ): Promise<void> {
    const resolved = await resolveRunAndHandle(step.run_id, cache)
    if (!resolved) {
      log.warn('claimed step belongs to a run row that no longer exists — releasing', {
        stepId: step.id,
        runId: step.run_id,
      })
      await releaseLease(db, step.id)
      return
    }
    const { run, handle } = resolved

    if (!handle) {
      // Not an error — just not our workflow. Hand it back so a worker in
      // this pool that DOES know it (or a future deploy of this one) can
      // pick it up; failing it here would kill a perfectly good run just
      // because this particular process hasn't registered that workflow.
      log.warn('no workflow handle registered in this process for claimed step — returning it to the queue', {
        stepId: step.id,
        runId: run.id,
        workflowId: run.workflow_id,
      })
      await releaseLease(db, step.id)
      await markStepReady(db, step.id)
      return
    }

    if (run.status !== 'queued' && run.status !== 'running') {
      // The run finished (or was cancelled) via another step/worker between
      // claim and now — nothing productive left to do with this step.
      log.info('run is no longer active — releasing lease without running the step', {
        stepId: step.id,
        runId: run.id,
        runStatus: run.status,
      })
      await releaseLease(db, step.id)
      return
    }

    // Cancellation checkpoint #1: right after claiming, before a single line
    // of the step function runs. A cancel request is not visible in
    // `run.status` (the run stays `running` until someone finalizes it), so it
    // takes its own read — and that read must be fresh, not the cached run row
    // above. Starting work on a doomed run is pure waste, and this is also the
    // gate that stops a step that fell asleep BEFORE the cancel from waking up
    // and running: it wakes, gets claimed like any due step, and lands here.
    if (await isCancellationRequested(db, run.id)) {
      log.info('run was cancelled before this step started — cancelling the step instead of running it', {
        stepId: step.id,
        runId: run.id,
      })
      await commitCancellation(step, run, 'pre-run')
      return
    }

    // A woken step is genuinely running again, so drop the "asleep" marker.
    // Unfenced and best-effort by design (see clearSleepMarker): the column is
    // observability only — nothing in the claim path reads it — so a stale
    // write here can make a dashboard briefly wrong and nothing else.
    if (step.sleeping_until !== null) {
      await clearSleepMarker(db, step.id)
    }

    if (run.status === 'queued') {
      // No-op-safe: markRunStarted only affects a row that's still
      // `queued`, so if several steps of a fresh run get claimed by
      // different workers in the same instant, exactly one of them writes
      // `run.started` — the rest see `undefined` back and do nothing.
      await withTransaction(db, async (tx) => {
        const flipped = await markRunStarted(tx, run.id)
        if (flipped) await insertHistory(tx, { runId: run.id, type: 'run.started' })
      })
    }

    const fn = handle.stepFns.get(step.name)
    if (!fn) {
      const error = serializeError(
        new Error(`worker: no step function registered for step "${step.name}" in workflow "${handle.name}"`)
      )
      const noFnOutcome: StepOutcome = { kind: 'failure', error, forcePermanent: true }
      const noFnResult = await commitOutcome(step, run, noFnOutcome, handle.failurePolicy)
      await logStepCommit(step, run, noFnResult, noFnOutcome, Date.now())
      return
    }

    // `abandoned` is set by the heartbeat if the lease turns out not to be
    // ours anymore. It's a fast-path skip, not the actual safety mechanism
    // — commitOutcome's lockStepIfOwner re-checks ownership in the DB
    // regardless, so even a race here (heartbeat fires *after* this check
    // but *before* the commit tx opens) is still caught correctly.
    let abandoned = false
    // Cancellation checkpoint #2, and the reason it lives on the heartbeat
    // tick: that timer already exists and already round-trips to Postgres
    // every heartbeatIntervalMs, so noticing a cancel costs one extra cheap
    // `exists(...)` on a connection we were using anyway — no new timer, no
    // new poll loop. Aborting the controller is all a worker can do from the
    // outside; whether the step actually stops is up to the step (see
    // engine/timeout.ts on why JS has no preemption).
    let cancelObserved = false
    const cancelController = new AbortController()
    const heartbeatTimer = setInterval(() => {
      heartbeatLease(db, step.id, workerId, leaseTtlMs)
        .then((ok) => {
          if (!ok) {
            abandoned = true
            log.warn('lease lost mid-step — outcome will not be committed', { stepId: step.id, runId: run.id })
            return
          }
          if (cancelObserved) return
          return isCancellationRequested(db, run.id).then((requested) => {
            if (!requested) return
            cancelObserved = true
            log.info('cancellation requested while step was in flight — aborting its signal', {
              stepId: step.id,
              runId: run.id,
            })
            cancelController.abort(new Error(`orqestra: run ${run.id} was cancelled`))
          })
        })
        .catch((e) => {
          log.error('heartbeat failed', { stepId: step.id, runId: run.id, error: serializeError(e) })
        })
    }, heartbeatIntervalMs)

    // Marks the start of this attempt, purely for the #32 duration figure
    // logged after the commit below — never used for any correctness
    // decision (timeouts are enforced by withTimeout/step.timeout_ms).
    const attemptStartedAt = Date.now()

    // Classify one execution of the step function. Everything here is decided
    // in memory; nothing is written until the commit* call below, so a lease
    // that moved on can still discard all of it.
    let attempt: Attempt
    // Hoisted so it survives past `withTimeout`'s callback — success needs
    // to read `ctx.skip()` requests (feature #17) back out via
    // `getSkipRequests` after `fn` has returned, not just its return value.
    let ctx: WorkflowContext | undefined
    try {
      // Run the user function OUTSIDE any transaction — it may be slow or
      // call out to the world. Only the outcome's persistence is atomic.
      //
      // The context is built INSIDE withTimeout so `ctx.signal` is the
      // combined signal (timeout ∪ cancellation), not one or the other:
      // a step that watches its signal bails on whichever fires first.
      // `sleepSeq` is what makes re-execution after a sleep converge — the
      // step function restarts from the top (a JS stack cannot be frozen
      // across a worker restart), and the context resolves the first
      // `sleep_seq` sleep() calls immediately instead of suspending again.
      const value = await withTimeout(
        (signal) => {
          ctx = createWorkflowContext({
            runId: run.id,
            input: run.input,
            sleepSeq: step.sleep_seq,
            // #18: how many event-waits this step has already served, and the
            // payloads they were woken with — so a replayed step returns from
            // an already-satisfied waitForEvent instead of re-suspending.
            eventSeq: step.event_seq,
            eventPayloads: step.event_payloads,
            signal,
            // #20: lets a spawned child record `run.parent_step_id`. The
            // block itself needs nothing from the context — this closure
            // already owns `step.id`/`workerId`.
            stepId: step.id,
          })
          return fn(ctx)
        },
        step.timeout_ms,
        cancelController.signal
      )
      attempt = { kind: 'success', value, skipNames: ctx ? getSkipRequests(ctx) : [] }
    } catch (e) {
      // Order matters. A SleepSignal is control flow, not a failure, and must
      // survive the timeout race untouched — withTimeout only ever *adds* a
      // StepTimeoutError of its own, it never rewrites what fn threw, so a
      // sleep that unwound before the budget expired arrives here intact.
      if (isSleepSignal(e)) {
        // ...unless the run is already cancelled, in which case suspending it
        // for 24h just to cancel it on wake is silly. Cancel it now.
        attempt = cancelObserved ? { kind: 'cancelled' } : { kind: 'sleep', signal: e }
      } else if (isChildBlockSignal(e)) {
        // Same shape as the sleep case, same reasoning: a child block is
        // control flow that survives the timeout race untouched — but if the
        // run is already cancelled, parking the step on a child nobody will
        // wait for is pointless, so cancel it now.
        attempt = cancelObserved ? { kind: 'cancelled' } : { kind: 'child-block', signal: e }
      } else if (isEventWaitSignal(e)) {
        // #18: same family as sleep/child-block. `since` is the step's claim
        // time (step.updated_at, frozen in the row we claimed) — the lower
        // bound for the throw->block race backstop in commitEventWait. If the
        // run is already cancelled, don't park it waiting for an event.
        attempt = cancelObserved
          ? { kind: 'cancelled' }
          : { kind: 'event-wait', signal: e, since: step.updated_at }
      } else if (cancelObserved) {
        // Cancellation outranks the timeout label deliberately: once we abort
        // the signal, a cooperative step throws (an AbortError, or its own
        // error) and an uncooperative one may well go on to blow its budget —
        // both are *consequences* of the cancel, and recording either as a
        // timeout would blame the step for something we did to it.
        attempt = { kind: 'cancelled' }
      } else if (isStepTimeoutError(e)) {
        attempt = { kind: 'timeout', error: e }
      } else {
        attempt = { kind: 'failure', error: e }
      }
    } finally {
      clearInterval(heartbeatTimer)
    }

    // The Phase 2 fast-path skip, unchanged and still not the safety
    // mechanism: every commit* below re-checks ownership inside its own
    // transaction, so losing the race between "the step settled" and "the
    // heartbeat noticed" is caught there regardless.
    if (abandoned) return

    switch (attempt.kind) {
      case 'success': {
        const outcome: StepOutcome = { kind: 'success', value: attempt.value, skipNames: attempt.skipNames }
        const result = await commitOutcome(step, run, outcome, handle.failurePolicy)
        await logStepCommit(step, run, result, outcome, attemptStartedAt)
        return
      }
      case 'sleep':
        await commitSleep(step, run, attempt.signal)
        return
      case 'child-block':
        await commitChildBlock(step, run, attempt.signal)
        return
      case 'event-wait':
        await commitEventWait(step, run, attempt.signal, attempt.since)
        return
      case 'cancelled':
        await commitCancellation(step, run, 'in-flight')
        return
      case 'timeout': {
        const outcome: StepOutcome = { kind: 'failure', error: attempt.error, timedOut: true }
        const result = await commitOutcome(step, run, outcome, handle.failurePolicy)
        await logStepCommit(step, run, result, outcome, attemptStartedAt)
        return
      }
      case 'failure': {
        const outcome: StepOutcome = { kind: 'failure', error: attempt.error }
        const result = await commitOutcome(step, run, outcome, handle.failurePolicy)
        await logStepCommit(step, run, result, outcome, attemptStartedAt)
        return
      }
    }
  }

  // #34 (write side): this worker's own row in worker_health. Best-effort and
  // outside any run/step transaction on purpose — a heartbeat is an
  // observability side-channel, never something a run's correctness can
  // depend on. Reused for both the periodic tick (below) and the final
  // "stopped" write in stop().
  async function sendHeartbeat(status: 'running' | 'draining' | 'stopped'): Promise<void> {
    try {
      await upsertWorkerHeartbeat(db, {
        workerId,
        hostname: hostname(),
        status,
        leasedSteps: inFlight.size,
        concurrency,
      })
    } catch (e) {
      log.error('worker heartbeat failed', { error: serializeError(e) })
    }
  }

  async function runLoop(): Promise<void> {
    let lastReclaimAt = 0
    // Reuses the existing per-step lease-heartbeat cadence rather than
    // inventing a new config knob (heartbeatIntervalMs is already derived
    // from ORQ_LEASE_TTL_MS) — this worker-level heartbeat just piggybacks on
    // the same tick rate.
    let lastWorkerHeartbeatAt = 0

    while (!stopping) {
      const now = Date.now()

      if (now - lastWorkerHeartbeatAt >= heartbeatIntervalMs) {
        lastWorkerHeartbeatAt = now
        await sendHeartbeat('running')
      }

      if (now - lastReclaimAt >= reclaimIntervalMs) {
        lastReclaimAt = now
        try {
          const result = await reclaimExpiredLeases(db)
          if (result.reclaimed.length > 0 || result.deadLettered.length > 0) {
            log.info('reclaim sweep', {
              reclaimed: result.reclaimed.length,
              deadLettered: result.deadLettered.length,
            })
          }
        } catch (e) {
          log.error('reclaim sweep failed', { error: serializeError(e) })
        }

        // #20's reconciliation net, on the same tick because it has the same
        // character as the reclaim sweep: a cheap periodic check that only
        // ever does something when an inline path failed to. See
        // sweepBlockedChildAwaits — it cannot wake a step whose child is
        // still in flight, so this is not polling, and a non-empty result is
        // a signal that some finalization path is missing its inline wake.
        try {
          const woken = await sweepBlockedChildAwaits(db)
          if (woken.length > 0) {
            log.warn('blocked-step sweep woke steps an inline finalization path should have woken', {
              steps: woken.length,
            })
          }
        } catch (e) {
          log.error('blocked-step sweep failed', { error: serializeError(e) })
        }
      }

      const cache = new Map<string, ResolvedRun | undefined>()
      let claimedAny = false

      while (!stopping && inFlight.size < concurrency) {
        claimAttempts++
        const step = await claimStep(db, { workerId, leaseTtlMs, namespace })
        if (!step) break
        claimsFound++
        claimedAny = true

        const promise = (async () => {
          try {
            await runClaimedStep(step, cache)
          } catch (e) {
            log.error('unhandled error running claimed step', { stepId: step.id, error: serializeError(e) })
          }
        })()
        inFlight.add(promise)
        promise.finally(() => inFlight.delete(promise))
      }

      // Never busy-spin on an empty (or fully-saturated) queue.
      if (!claimedAny) await sleep(pollIntervalMs)
    }
  }

  return {
    id: workerId,

    start(): void {
      if (started) return
      started = true
      loopDone = runLoop().catch((e) => {
        log.error('worker loop crashed', { error: serializeError(e) })
      })
    },

    async stop(): Promise<void> {
      stopping = true
      await sendHeartbeat('draining')
      if (loopDone) await loopDone
      // Every in-flight step releases its own lease as part of committing
      // its outcome (success, retry, or permanent failure) — awaiting them
      // here is what makes stop() "graceful": nothing is abandoned mid-air.
      await Promise.allSettled(Array.from(inFlight))
      // Final write so a dashboard reading worker_health sees this worker
      // leave cleanly rather than merely going stale.
      await sendHeartbeat('stopped')
    },

    get inFlight(): number {
      return inFlight.size
    },

    get claimAttempts(): number {
      return claimAttempts
    },

    get claimsFound(): number {
      return claimsFound
    },
  }
}
