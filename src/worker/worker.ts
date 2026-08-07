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

import { hostname } from 'node:os'
import { loadConfig } from '../config.ts'
import type { Db } from '../store/client.ts'
import { withTransaction } from '../store/client.ts'
import { claimStep } from '../queue/claim.ts'
import { heartbeatLease, releaseLease, reclaimExpiredLeases } from '../queue/lease.ts'
import { advanceRun } from '../engine/executor.ts'
import {
  cancelPendingSteps,
  cancelRunningStep,
  clearSleepMarker,
  completeStep,
  failStep,
  finalizeCancelledRun,
  getRun,
  getWorkflowById,
  isCancellationRequested,
  lockRun,
  lockStepIfOwner,
  markRunStarted,
  markStepReady,
  retryStep,
  insertHistory,
  sleepStep,
  updateRunStatus,
  type RunRow,
  type StepRow,
} from '../store/repositories.ts'
import { serializeError } from '../types.ts'
import { createWorkflowContext } from '../define/context.ts'
import type { WorkflowHandle } from '../define/workflow.ts'
import { DEFAULT_RETRY_POLICY, nextRunAfter, shouldRetry, type RetryPolicy } from '../engine/retry.ts'
import { isSleepSignal, type SleepSignal } from '../engine/sleep.ts'
import { isStepTimeoutError, withTimeout } from '../engine/timeout.ts'
import { createLogger, type Logger } from '../observability/logger.ts'

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
  | { kind: 'success'; value: unknown }
  // `timedOut` only changes the history label — a timeout is an ordinary
  // failure as far as the retry/backoff decision is concerned (Phase 3 #10).
  | { kind: 'failure'; error: unknown; forcePermanent?: boolean; timedOut?: boolean }

// What one execution of a step function turned into, before any of it is
// persisted. Phase 2 only had success/failure; Phase 3 adds two outcomes that
// are NOT failures and must never be routed through the retry path:
// `sleep` (the step asked to be suspended) and `cancelled` (the run was
// cancelled underneath it).
type Attempt =
  | { kind: 'success'; value: unknown }
  | { kind: 'sleep'; signal: SleepSignal }
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
  async function commitOutcome(step: StepRow, run: RunRow, outcome: StepOutcome): Promise<void> {
    await withTransaction(db, async (tx) => {
      const owned = await lockStepIfOwner(tx, step.id, workerId)
      if (!owned) {
        log.warn('lease fencing failed at commit time — discarding outcome, another worker owns this step now', {
          stepId: step.id,
          runId: run.id,
        })
        return
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
        await advanceRun(tx, run.id)
        return
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
        return
      }

      // Retries exhausted (or unretryable): the step and its run are done.
      // cancelPendingSteps stops other workers from picking up the rest of
      // a run that's already dead.
      await failStep(tx, step.id, error)
      await insertHistory(tx, { runId: run.id, stepId: step.id, type: 'step.failed', data: { error } })
      await releaseLease(tx, step.id)
      await updateRunStatus(tx, run.id, 'failed', { finishedAt: new Date() })
      await cancelPendingSteps(tx, run.id)
      await insertHistory(tx, { runId: run.id, type: 'run.failed', data: { reason: 'step.failed', stepId: step.id } })
    })
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
      await commitOutcome(step, run, { kind: 'failure', error, forcePermanent: true })
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

    // Classify one execution of the step function. Everything here is decided
    // in memory; nothing is written until the commit* call below, so a lease
    // that moved on can still discard all of it.
    let attempt: Attempt
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
        (signal) =>
          fn(
            createWorkflowContext({
              runId: run.id,
              input: run.input,
              sleepSeq: step.sleep_seq,
              signal,
            })
          ),
        step.timeout_ms,
        cancelController.signal
      )
      attempt = { kind: 'success', value }
    } catch (e) {
      // Order matters. A SleepSignal is control flow, not a failure, and must
      // survive the timeout race untouched — withTimeout only ever *adds* a
      // StepTimeoutError of its own, it never rewrites what fn threw, so a
      // sleep that unwound before the budget expired arrives here intact.
      if (isSleepSignal(e)) {
        // ...unless the run is already cancelled, in which case suspending it
        // for 24h just to cancel it on wake is silly. Cancel it now.
        attempt = cancelObserved ? { kind: 'cancelled' } : { kind: 'sleep', signal: e }
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
      case 'success':
        await commitOutcome(step, run, { kind: 'success', value: attempt.value })
        return
      case 'sleep':
        await commitSleep(step, run, attempt.signal)
        return
      case 'cancelled':
        await commitCancellation(step, run, 'in-flight')
        return
      case 'timeout':
        await commitOutcome(step, run, { kind: 'failure', error: attempt.error, timedOut: true })
        return
      case 'failure':
        await commitOutcome(step, run, { kind: 'failure', error: attempt.error })
        return
    }
  }

  async function runLoop(): Promise<void> {
    let lastReclaimAt = 0

    while (!stopping) {
      const now = Date.now()
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
      }

      const cache = new Map<string, ResolvedRun | undefined>()
      let claimedAny = false

      while (!stopping && inFlight.size < concurrency) {
        const step = await claimStep(db, { workerId, leaseTtlMs, namespace })
        if (!step) break
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
      if (loopDone) await loopDone
      // Every in-flight step releases its own lease as part of committing
      // its outcome (success, retry, or permanent failure) — awaiting them
      // here is what makes stop() "graceful": nothing is abandoned mid-air.
      await Promise.allSettled(Array.from(inFlight))
    },

    get inFlight(): number {
      return inFlight.size
    },
  }
}
