// The worker-facing entrypoint into the queue. All the correctness lives in
// repositories.claimNextStep (the SKIP LOCKED transaction) — this file just
// gives it a typed, minimal surface so worker.ts doesn't need to know the
// repositories.ts import exists.

import type { Db } from '../store/client.ts'
import { claimNextStep, type StepRow } from '../store/repositories.ts'

export interface ClaimOptions {
  workerId: string
  leaseTtlMs: number
  namespace?: string
}

/**
 * Claim the next ready, due, unleased step for `workerId` and flip it to
 * `running` under a lease that expires in `leaseTtlMs`. Returns undefined
 * when there's nothing to claim right now (empty queue, everything ready
 * is still leased, or everything ready isn't due yet) — that's the normal
 * "poll again later" outcome, not an error.
 *
 * Safe to call from any number of workers at once: two concurrent callers
 * never receive the same step (see repositories.ts for why).
 */
export async function claimStep(db: Db, options: ClaimOptions): Promise<StepRow | undefined> {
  return claimNextStep(db, options)
}
