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

import { hostname } from 'node:os'
import { loadConfig } from '../config.ts'
import type { Db } from '../store/client.ts'
import { withTransaction } from '../store/client.ts'
import { claimStep } from '../queue/claim.ts'
import { heartbeatLease, releaseLease, reclaimExpiredLeases } from '../queue/lease.ts'
import { advanceRun } from '../engine/executor.ts'
import {
  cancelPendingSteps,
  completeStep,
  failStep,
  getRun,
  getWorkflowById,
  lockRun,
  lockStepIfOwner,
  markRunStarted,
  markStepReady,
  retryStep,
  insertHistory,
  updateRunStatus,
  type RunRow,
  type StepRow,
} from '../store/repositories.ts'
import { serializeError } from '../types.ts'
import { createWorkflowContext } from '../define/context.ts'
import type { WorkflowHandle } from '../define/workflow.ts'
import { DEFAULT_RETRY_POLICY, nextRunAfter, shouldRetry, type RetryPolicy } from '../engine/retry.ts'
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
  | { kind: 'failure'; error: unknown; forcePermanent?: boolean }

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
    const heartbeatTimer = setInterval(() => {
      heartbeatLease(db, step.id, workerId, leaseTtlMs)
        .then((ok) => {
          if (!ok) {
            abandoned = true
            log.warn('lease lost mid-step — outcome will not be committed', { stepId: step.id, runId: run.id })
          }
        })
        .catch((e) => {
          log.error('heartbeat failed', { stepId: step.id, runId: run.id, error: serializeError(e) })
        })
    }, heartbeatIntervalMs)

    try {
      const ctx = createWorkflowContext({ runId: run.id, input: run.input })
      try {
        // Run the user function OUTSIDE any transaction — it may be slow or
        // call out to the world. Only the outcome's persistence is atomic.
        const value = await fn(ctx)
        if (abandoned) return
        await commitOutcome(step, run, { kind: 'success', value })
      } catch (e) {
        if (abandoned) return
        await commitOutcome(step, run, { kind: 'failure', error: e })
      }
    } finally {
      clearInterval(heartbeatTimer)
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
