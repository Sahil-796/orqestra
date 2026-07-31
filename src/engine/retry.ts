// Pure retry/backoff policy — no I/O, unit-testable without Postgres. This
// module only computes numbers; persisting a retry decision (writing the
// step back to `ready` with a due time) is repositories.retryStep, and
// deciding *when* to call either of these is the worker's job (out of
// scope here — see queue/claim.ts + queue/lease.ts for the leasing half of
// Phase 2).

export interface RetryPolicy {
  baseMs: number
  factor: number
  maxMs: number
  jitter: boolean
}

export const DEFAULT_RETRY_POLICY: RetryPolicy = {
  baseMs: 100,
  factor: 2,
  maxMs: 30_000,
  jitter: true,
}

// `attempt` throughout this file is 1-based and means "attempts already
// used" — the value step.attempt holds right after claimNextStep bumps it
// on claim. backoffMs(1, ...) is therefore the delay before the *second*
// attempt, not the first (the first attempt has no backoff, it just runs).
// Guard against callers passing 0/negative/NaN (e.g. an uninitialized
// counter) by clamping to the first attempt rather than producing NaN/
// negative delays.
function normalizeAttempt(attempt: number): number {
  if (!Number.isFinite(attempt) || attempt < 1) return 1
  return Math.floor(attempt)
}

export function backoffMs(attempt: number, policy: RetryPolicy = DEFAULT_RETRY_POLICY): number {
  const a = normalizeAttempt(attempt)
  const ceiling = Math.min(policy.maxMs, policy.baseMs * policy.factor ** (a - 1))
  if (!policy.jitter) return ceiling
  // Full jitter, factor in [0.5, 1]: always <= the unjittered ceiling (so
  // maxMs stays a real ceiling even after jitter) and always > 0 whenever
  // the ceiling is > 0 (a zero-delay retry storm is as bad as no backoff).
  const jitterFactor = 0.5 + Math.random() * 0.5
  return ceiling * jitterFactor
}

export function shouldRetry(attempt: number, maxAttempts: number): boolean {
  return normalizeAttempt(attempt) < maxAttempts
}

export function nextRunAfter(
  attempt: number,
  now: Date,
  policy: RetryPolicy = DEFAULT_RETRY_POLICY
): Date {
  return new Date(now.getTime() + backoffMs(attempt, policy))
}
