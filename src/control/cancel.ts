// The public face of cancellation. Everything here is deliberately thin:
// the storage layer already owns the atomicity (requestRunCancellation's
// no-op WHERE, finalizeCancelledRun's locked transaction), so this file's
// only real job is the *policy* — deciding whether a caller's cancel can be
// completed here and now, or whether it has to be handed to the worker that
// currently owns a running step.
//
// Cancellation is cooperative and this API says so out loud. We cannot kill
// a step function executing inside another process; the honest contract is
// "the request is durable, the finalization happens at the next safe point",
// and CancelResult exposes which of the two happened instead of pretending
// every cancel is synchronous.

import type { Db } from '../store/client.ts'
import {
  countStepsByStatus,
  finalizeCancelledRun,
  getCancelRequestedRuns,
  getRun,
  isCancellationRequested,
  requestRunCancellation,
  resolveBlockedStepForChildRun,
  type RunRow,
} from '../store/repositories.ts'

export interface CancelResult {
  /** A cancellation request was newly recorded. False if already requested or already terminal. */
  requested: boolean
  /** The run reached `cancelled` synchronously (nothing was in flight). */
  finalized: boolean
  /** True when a step is still running elsewhere and its owning worker will finalize the run. */
  pending: boolean
  run: RunRow | undefined
}

// A run can only be finalized from outside once nothing is executing on its
// behalf. `running` is the only step status that means "some worker's process
// is mid-call right now"; pending/ready steps are just rows, and
// finalizeCancelledRun cancels those itself under the run lock.
async function hasRunningStep(db: Db, runId: string): Promise<boolean> {
  const counts = await countStepsByStatus(db, runId)
  return (counts.running ?? 0) > 0
}

/**
 * Request cancellation of a run. If no step is currently `running`, the run
 * is finalized to `cancelled` immediately here. If a step IS running, we only
 * record the request — the worker that owns the lease observes it on its next
 * heartbeat tick and finalizes at a safe point. Stomping the running step from
 * here would race that worker's fenced commit and could produce two outcomes
 * for one step.
 */
export async function cancelRun(db: Db, runId: string): Promise<CancelResult> {
  const accepted = await requestRunCancellation(db, runId)
  const requested = accepted !== undefined

  // requestRunCancellation returns undefined for two very different reasons:
  // the run is terminal (nothing to do, and we must not touch it), or the
  // request was already recorded by an earlier call. Only the second is
  // still worth driving forward, so ask which one we're in. This is a read
  // *after* the write, so it cannot race a concurrent first request into
  // being ignored.
  const active = requested || (await isCancellationRequested(db, runId))
  if (!active) {
    return { requested: false, finalized: false, pending: false, run: await getRun(db, runId) }
  }

  if (await hasRunningStep(db, runId)) {
    return { requested, finalized: false, pending: true, run: accepted ?? (await getRun(db, runId)) }
  }

  // Racy in the benign direction: a worker can claim a `ready` step between
  // the count above and the transaction below. finalizeCancelledRun takes the
  // run lock and leaves `running` steps alone, so that worker still owns its
  // step's outcome and its fenced commit still wins — the run just reaches
  // `cancelled` a beat before that one step does.
  const { run } = await finalizeCancelledRun(db, runId)
  if (!run) {
    // Lost the finalize to whoever else was cancelling this run (a worker,
    // the sweep, a concurrent caller). Idempotent by construction: the run is
    // already cancelled, so report it rather than inventing a failure.
    return { requested, finalized: false, pending: false, run: await getRun(db, runId) }
  }
  // Cancelled is terminal, so a parent step blocked on this run as its child
  // has to be woken here too — same reason the worker's cancellation commit
  // wakes it. Missing this leaves the parent blocked until the sweep.
  await resolveBlockedStepForChildRun(db, runId)
  return { requested, finalized: true, pending: false, run }
}

/** Has cancellation been requested for this run (whether or not it's finalized)? */
export async function isRunCancelled(db: Db, runId: string): Promise<boolean> {
  return isCancellationRequested(db, runId)
}

/**
 * Sweep: finalize runs whose cancellation was requested but which have no
 * step still running — e.g. the owning worker crashed after the request
 * landed. Safe to call on a timer. Returns the run ids it finalized.
 */
export async function sweepCancelledRuns(db: Db): Promise<string[]> {
  const pending = await getCancelRequestedRuns(db)
  const finalized: string[] = []

  // Serial, not Promise.all: each finalize takes a row lock and the sweep is
  // a background janitor, not a latency path — there's nothing to gain from
  // holding N locks at once against a pool that workers are also using.
  for (const candidate of pending) {
    if (await hasRunningStep(db, candidate.id)) continue
    const { run } = await finalizeCancelledRun(db, candidate.id)
    if (!run) continue
    await resolveBlockedStepForChildRun(db, run.id)
    finalized.push(run.id)
  }

  return finalized
}
