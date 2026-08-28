// The public face of "manually re-drive a dead-lettered run" (#27, operator
// manual retry). Like control/cancel.ts, this file is deliberately thin: the
// storage layer already owns the atomicity (resetRunForRetry does the whole
// run + steps revival in one transaction, guarded by a `status = 'dead_letter'`
// WHERE that makes a double-retry a no-op). This file's only real job is the
// *policy* gate — an operator can only retry a run that is genuinely parked in
// dead_letter, so we confirm that first and report a clear result when it isn't,
// rather than silently no-op'ing.
//
// After a successful reset the run is back in `queued` with its failed/cancelled
// steps revived to `ready`/`pending`; a worker's normal claim loop
// (queue/claim.ts's claimNextStep only considers steps whose run is in
// ('queued','running')) then picks it up again with no further prodding — the
// same path control/start.ts's fresh runs travel.

import type { Db } from '../store/client.ts'
import {
  getDeadLetterRun,
  listDeadLetterRuns,
  resetRunForRetry,
  type RunRow,
} from '../store/repositories.ts'

export interface RetryDeadLetterResult {
  /** A dead-lettered run was found and reset back to `queued` (now re-claimable). */
  retried: boolean
  /**
   * Why the retry didn't happen, when `retried` is false. `not_dead_letter`
   * covers both "no such run" and "run exists but isn't parked in dead_letter":
   * getDeadLetterRun conflates them on purpose — in either case there is
   * nothing to retry.
   */
  reason?: 'not_dead_letter'
  /** The run after reset when retried; undefined when there was nothing to retry. */
  run: RunRow | undefined
}

/**
 * Re-drive a dead-lettered run. Validates the run is actually parked in
 * `dead_letter` (via getDeadLetterRun) before touching it; if it isn't — no
 * such run, or a run in any other status — returns `{ retried: false,
 * reason: 'not_dead_letter' }` without mutating anything. Otherwise resets it
 * back to `queued` (reviving its failed/cancelled steps) so a worker's normal
 * claim loop re-runs it, and returns the revived run.
 */
export async function retryDeadLetterRun(db: Db, runId: string): Promise<RetryDeadLetterResult> {
  const parked = await getDeadLetterRun(db, runId)
  if (!parked) {
    return { retried: false, reason: 'not_dead_letter', run: undefined }
  }

  // Racy only in the benign direction: a concurrent retry could reset the run
  // between the read above and this call. resetRunForRetry's own
  // `status = 'dead_letter'` WHERE makes the loser a no-op (returns undefined),
  // which we report as "nothing to retry" rather than inventing a success.
  const run = await resetRunForRetry(db, runId)
  if (!run) {
    return { retried: false, reason: 'not_dead_letter', run: undefined }
  }
  return { retried: true, run }
}

/**
 * The operator's triage listing: every run currently parked in `dead_letter`,
 * most-recently-parked first. A thin pass-through to the repository so the HTTP
 * layer never touches storage directly. `limit` caps the page (repository
 * default: 100).
 */
export async function listDeadLetteredRuns(db: Db, limit?: number): Promise<RunRow[]> {
  return listDeadLetterRuns(db, limit)
}
