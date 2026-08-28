// Lease lifecycle: extend (heartbeat), give up (release), and the reclaim
// sweep that gets a crashed worker's step back into the queue within the
// lease TTL. This is what makes "kill a worker mid-step" survivable
// without a human — the same property Phase 1 got from a single process
// re-reading its own step rows, now generalized to N processes that can't
// see each other's memory, only Postgres.

import type { Db } from '../store/client.ts'
import { withTransaction } from '../store/client.ts'
import {
  cancelPendingSteps,
  findExpiredLeases,
  heartbeatStep,
  insertHistory,
  poisonStep,
  reclaimStep,
  releaseStep,
  resolveBlockedStepForChildRun,
  deadLetterRun,
} from '../store/repositories.ts'
import { serializeError } from '../types.ts'

// Past this many reclaims, a step is presumed to be a poison pill (it kills
// every worker that touches it) rather than unlucky — see build-plan.html's
// "poison-pill detection" enhancement. 3 gives a step a couple of genuine
// second chances (a bad deploy, a transient host issue) without letting one
// bad step consume the pool forever.
const DEFAULT_MAX_RECLAIMS = 3

/**
 * Extend a step's lease. Returns false — instead of throwing — when the
 * lease is no longer this worker's to extend (already reclaimed by a sweep,
 * or claimed by someone else after this worker was presumed dead). That's
 * the fencing signal: a worker that gets `false` back must abandon the step
 * immediately and NOT commit its outcome, because ownership has moved on.
 */
export async function heartbeatLease(
  db: Db,
  stepId: string,
  workerId: string,
  leaseTtlMs: number
): Promise<boolean> {
  const row = await heartbeatStep(db, stepId, workerId, leaseTtlMs)
  return row !== undefined
}

/** Give up a lease without changing the step's status (see releaseStep). */
export async function releaseLease(db: Db, stepId: string): Promise<void> {
  await releaseStep(db, stepId)
}

export interface ReclaimResult {
  reclaimed: string[]
  deadLettered: string[]
}

/**
 * One pass of the reclaim sweep: find every `running` step whose lease has
 * expired and either send it back to `ready` (reclaimed) or, past the
 * poison-pill ceiling, fail it and its run outright (deadLettered). Meant
 * to be called on a timer by a worker/control process — this function is
 * one tick, not a loop.
 *
 * Ceiling check uses the reclaim_count already on the row from the scan
 * (findExpiredLeases), not a re-read, so the decision is made against a
 * single consistent snapshot even though the two branches below hit the DB
 * again to act on it.
 */
export async function reclaimExpiredLeases(
  db: Db,
  opts: { maxReclaims?: number } = {}
): Promise<ReclaimResult> {
  const maxReclaims = opts.maxReclaims ?? DEFAULT_MAX_RECLAIMS
  const expired = await findExpiredLeases(db)

  const reclaimed: string[] = []
  const deadLettered: string[] = []

  for (const step of expired) {
    if (step.reclaim_count + 1 > maxReclaims) {
      const error = serializeError(
        new Error(
          `reclaimExpiredLeases: step "${step.name}" (${step.id}) exceeded the poison-pill ceiling ` +
            `of ${maxReclaims} reclaims — its lease expired ${step.reclaim_count + 1} times without ` +
            `the step ever completing, so it's being failed instead of reclaimed again`
        )
      )
      await withTransaction(db, async (tx) => {
        const poisoned = await poisonStep(tx, step.id, error)
        await insertHistory(tx, {
          runId: poisoned.run_id,
          stepId: poisoned.id,
          type: 'step.poisoned',
          data: { error, reclaimCount: poisoned.reclaim_count },
        })
        // #26: a poison-pill run has burned through its reclaim budget (a step
        // whose lease kept expiring without ever completing — the crash-loop
        // case). Route it to the dead-letter queue rather than a bare `failed`,
        // so it's visible to the operator DLQ + manual retry, consistent with
        // the worker's own retry-exhaustion path. `deadLettered` (the result
        // array) has always named this outcome — now it truly dead-letters.
        await deadLetterRun(
          tx,
          poisoned.run_id,
          `step "${poisoned.name}" exceeded the poison-pill ceiling of ${maxReclaims} reclaims`
        )
        await insertHistory(tx, {
          runId: poisoned.run_id,
          type: 'run.dead_lettered',
          data: { reason: 'step.poisoned', stepId: poisoned.id },
        })
        // Stop other workers from picking up the rest of this now-dead run.
        await cancelPendingSteps(tx, poisoned.run_id)
        // This run just went terminal, so if it was somebody's child, the
        // parent step blocked on it has to be woken — otherwise the parent
        // waits for the sweep to notice a child that will never finish.
        await resolveBlockedStepForChildRun(tx, poisoned.run_id)
      })
      deadLettered.push(step.id)
      continue
    }

    // reclaimStep re-checks expiry at write time; undefined means a
    // heartbeat rescued it after the scan — not stuck, so not reported.
    const row = await reclaimStep(db, step.id)
    if (row) reclaimed.push(row.id)
  }

  return { reclaimed, deadLettered }
}
