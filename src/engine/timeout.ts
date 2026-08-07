// Pure step-timeout primitives — no I/O, no Postgres import, unit-testable
// without a database (same posture as engine/retry.ts and engine/sleep.ts).
// This module races a step function against its budget and hands the worker
// a decision; *persisting* that decision (retry with backoff, or a permanent
// fail that kills the run) is the worker's job.
//
// Be honest about what a timeout can do in JavaScript: there is no preemption
// here and there cannot be. All this does is (a) abort an AbortSignal the step
// function may be watching, and (b) stop *waiting* for the step's promise, so
// the worker can commit a `timed_out` outcome and move on. A step function
// that ignores its signal keeps running — its promise stays pending (or
// settles later into a result nobody reads) and a genuinely runaway loop keeps
// burning CPU in this process until it returns on its own. The step row is
// marked timed out and the worker is freed; the *work* is only stopped if the
// step cooperates. Killing uncooperative code needs process isolation, which
// is not what this phase buys.

const TIMEOUT_BRAND = '__orqestraStepTimeoutError'

/** A step that blew its `timeout_ms` budget. Goes through the normal failure/retry path. */
export class StepTimeoutError extends Error {
  /** Brand, not `instanceof` — see isStepTimeoutError for why. */
  readonly [TIMEOUT_BRAND] = true as const
  readonly timeoutMs: number

  constructor(timeoutMs: number) {
    super(`step exceeded its ${timeoutMs}ms timeout`)
    this.name = 'StepTimeoutError'
    this.timeoutMs = timeoutMs
  }
}

/**
 * Brand check rather than `instanceof`, for the same reason as isSleepSignal:
 * a module loaded through two different specifiers produces two distinct
 * classes and `instanceof` across them is false. Misclassifying a timeout as
 * an ordinary throw only costs a history label, but the reverse habit is how
 * control-flow signals get silently swallowed, so both modules use the same
 * shape.
 */
export function isStepTimeoutError(value: unknown): value is StepTimeoutError {
  if (typeof value !== 'object' || value === null) return false
  if ((value as Record<string, unknown>)[TIMEOUT_BRAND] === true) return true
  return value instanceof StepTimeoutError
}

/**
 * Race a step function against its timeout, wiring an AbortSignal so a
 * cooperative step can bail early.
 *
 * - `timeoutMs` null/undefined/<= 0 means no timeout: `fn` is awaited
 *   unchanged, and it still gets a signal (so `parent` cancellation reaches it).
 * - `parent` lets an outside abort — cancellation, in the worker's case —
 *   propagate into the signal `fn` receives. A parent abort does NOT produce a
 *   StepTimeoutError: whatever `fn` throws on the way out is what surfaces, so
 *   the caller can tell "cancelled" from "out of time".
 * - Only this function's own timer rejects with StepTimeoutError.
 *
 * Every exit path clears the timer and unsubscribes from `parent`. A worker
 * runs this in a hot loop, and a leaked timer both keeps the event loop alive
 * (a process that will not exit on shutdown) and pins the closure it captured.
 */
export function withTimeout<T>(
  fn: (signal: AbortSignal) => Promise<T>,
  timeoutMs: number | null | undefined,
  parent?: AbortSignal
): Promise<T> {
  const controller = new AbortController()
  const onParentAbort = () => controller.abort(parent?.reason)

  if (parent) {
    // Already-aborted parents must not be missed: addEventListener would
    // never fire for an abort that happened before we subscribed.
    if (parent.aborted) controller.abort(parent.reason)
    else parent.addEventListener('abort', onParentAbort, { once: true })
  }

  const detachParent = () => {
    if (parent) parent.removeEventListener('abort', onParentAbort)
  }

  // No budget: nothing to race, so don't create a timer at all.
  if (timeoutMs === null || timeoutMs === undefined || !Number.isFinite(timeoutMs) || timeoutMs <= 0) {
    return (async () => {
      try {
        return await fn(controller.signal)
      } finally {
        detachParent()
      }
    })()
  }

  const budget = timeoutMs
  let timer: ReturnType<typeof setTimeout> | undefined

  return (async () => {
    try {
      const expired = new Promise<never>((_resolve, reject) => {
        timer = setTimeout(() => {
          const error = new StepTimeoutError(budget)
          // Reject BEFORE aborting, and the order is load-bearing. Aborting
          // first fires the step's own abort listener synchronously, and a
          // cooperative step that rejects from that listener would settle the
          // race first — so the caller would see the step's bail-out error and
          // never learn the step timed out. Settling `expired` first makes the
          // label deterministic: a timeout always reads as a timeout, however
          // fast the step reacts. The abort still lands immediately after, so
          // the step is told to stop either way.
          reject(error)
          controller.abort(error)
        }, budget)
      })
      // Promise.race attaches a handler to BOTH promises, so whichever loses
      // cannot surface as an unhandled rejection later — that matters here,
      // since the losing side routinely settles after the race is decided.
      return await Promise.race([fn(controller.signal), expired])
    } finally {
      if (timer !== undefined) clearTimeout(timer)
      detachParent()
    }
  })()
}
