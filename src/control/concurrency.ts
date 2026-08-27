// Feature #12 — concurrency limits: the policy/validation half. The actual
// count-and-claim SQL lives behind the storage boundary in
// repositories.ts (`claimNextStep`) — this file holds only the pure pieces
// that don't belong in SQL: validating a declared limit, and the small helper
// the claim layer uses to decide whether a step even needs the serialized
// keyed path.
//
// The correctness mechanism (why a plain `count(*) < limit` subquery is not
// enough, and how the advisory lock closes the race) is documented at the
// claim site, not here — this module is deliberately storage-free.

import type { ConcurrencyLimit } from '../types.ts'

/**
 * Validate (and normalize) a step's declared concurrency at definition time so
 * a bad limit fails loudly in `defineWorkflow` rather than silently misbehaving
 * at claim time. Returns `undefined` for the unlimited (un-declared) case, so a
 * caller can assign the result straight onto a `StepDefinition.concurrency`.
 *
 * `stepName` is only used to make the thrown message point at the offending
 * step.
 */
export function validateConcurrency(
  stepName: string,
  concurrency: ConcurrencyLimit | undefined
): ConcurrencyLimit | undefined {
  if (concurrency === undefined) return undefined
  const { key, limit } = concurrency
  if (typeof key !== 'string' || key.length === 0) {
    throw new Error(`defineWorkflow: step "${stepName}" concurrency.key must be a non-empty string`)
  }
  if (!Number.isInteger(limit) || limit < 1) {
    throw new Error(
      `defineWorkflow: step "${stepName}" concurrency.limit must be a positive integer, got: ${limit}`
    )
  }
  return { key, limit }
}

/**
 * True when a step declares a concurrency key and therefore needs the
 * serialized (advisory-locked) claim path; false for the un-keyed fast path
 * that claims without any per-key coordination.
 */
export function isConcurrencyLimited(concurrencyKey: string | null): concurrencyKey is string {
  return concurrencyKey !== null
}
