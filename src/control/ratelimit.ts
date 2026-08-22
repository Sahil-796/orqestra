// Feature #13 — rate limiting: the policy/validation half. The count-and-consume
// SQL lives behind the storage boundary in repositories.ts (`claimNextStep`) —
// this file holds only the pure pieces that don't belong in SQL: validating a
// declared rate limit at definition time, the small helper the claim layer uses
// to decide whether a step even needs the serialized keyed path, and the
// fixed-window boundary math.
//
// The correctness mechanism (why a naive read-then-increment is not enough, and
// how the per-key advisory lock closes the race) is documented at the claim
// site, not here — this module is deliberately storage-free.

import type { RateLimit } from '../types.ts'

/**
 * Validate (and normalize) a step's declared rate limit at definition time so a
 * bad limit/window fails loudly in `defineWorkflow` rather than silently
 * misbehaving at claim time. Returns `undefined` for the unlimited case, so a
 * caller can assign the result straight onto a `StepDefinition.rateLimit`.
 *
 * `stepName` only shapes the thrown message to point at the offending step.
 */
export function validateRateLimit(
  stepName: string,
  rateLimit: RateLimit | undefined
): RateLimit | undefined {
  if (rateLimit === undefined) return undefined
  const { key, limit, windowMs } = rateLimit
  if (typeof key !== 'string' || key.length === 0) {
    throw new Error(`defineWorkflow: step "${stepName}" rateLimit.key must be a non-empty string`)
  }
  if (!Number.isInteger(limit) || limit < 1) {
    throw new Error(
      `defineWorkflow: step "${stepName}" rateLimit.limit must be a positive integer, got: ${limit}`
    )
  }
  if (!Number.isInteger(windowMs) || windowMs < 1) {
    throw new Error(
      `defineWorkflow: step "${stepName}" rateLimit.windowMs must be a positive integer, got: ${windowMs}`
    )
  }
  return { key, limit, windowMs }
}

/**
 * True when a step declares a rate key and therefore needs the serialized
 * (advisory-locked) claim path; false for the un-keyed fast path.
 */
export function isRateLimited(rateKey: string | null): rateKey is string {
  return rateKey !== null
}

/**
 * The fixed-window boundary math, mirrored by the SQL in `claimNextStep` (which
 * computes the same value against the DB clock so the counter and the deferral
 * agree with the `run_after <= now()` gate). Given an instant in epoch
 * milliseconds, the start of the window it falls in: contiguous `windowMs`
 * buckets aligned to the epoch.
 */
export function windowStartMs(nowMs: number, windowMs: number): number {
  return Math.floor(nowMs / windowMs) * windowMs
}

/**
 * The start of the NEXT window after the one `nowMs` falls in — where an
 * exhausted step's `run_after` is pushed to, so a worker stops re-checking it
 * until fresh budget exists.
 */
export function nextWindowStartMs(nowMs: number, windowMs: number): number {
  return windowStartMs(nowMs, windowMs) + windowMs
}
