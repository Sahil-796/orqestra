// WorkflowContext — the object step functions receive. Phase 0 only
// declared the shape; Phase 1 (this file) makes `now`/`random` real since
// they don't need any engine support. `sleep`/`waitForEvent` stay throwing
// stubs until the engine capability that backs each one lands.
//
// Determinism note: `now()`/`random()` are NOT yet replay-recorded — a step
// that re-runs after a crash will get a fresh Date/random value, not the
// one it saw before. That's fine for Phase 1: the memoization boundary is
// the whole step (a `completed` step is never re-run at all), so at-least-
// once re-execution of an *incomplete* step simply re-sampling `now()`/
// `random()` is still correct. Recording these for true deterministic
// replay inside a single step is a future refinement (relevant once steps
// can partially progress across a sleep/signal boundary).

function notImplemented(feature: string, phase: string): never {
  throw new Error(`ctx.${feature} is not implemented yet (lands in ${phase})`)
}

export interface WorkflowContext {
  /** The run's input, as passed to enqueue/run. */
  readonly input: unknown
  /** The run's stable id. */
  readonly runId: string

  /**
   * Deterministic clock — steps must use this instead of `Date.now()` so
   * replay produces the same result as the original run.
   */
  now(): Date

  /**
   * Deterministic PRNG — steps must use this instead of `Math.random()` so
   * replay produces the same result as the original run.
   */
  random(): number

  /** Suspend the run for a duration without holding a worker. Phase 3. */
  sleep(duration: string | number): Promise<void>

  /** Suspend the run until a named event arrives. Phase 5. */
  waitForEvent<T = unknown>(eventKey: string): Promise<T>
}

export function createWorkflowContext(input: { runId: string; input: unknown }): WorkflowContext {
  return {
    input: input.input,
    runId: input.runId,
    now: () => new Date(),
    random: () => Math.random(),
    sleep: () => notImplemented('sleep()', 'Phase 3'),
    waitForEvent: () => notImplemented('waitForEvent()', 'Phase 5'),
  }
}
