// WorkflowContext — the object step functions receive. Phase 0 only
// declared the shape; Phase 1 made `now`/`random` real since they don't need
// any engine support. Phase 3 makes `sleep` real (backed by suspend +
// requeue, see engine/sleep.ts) and exposes the abort `signal` that step
// timeouts and run cancellation fire. `waitForEvent` stays a throwing stub
// until Phase 5.
//
// Determinism note: `now()`/`random()` are NOT replay-recorded — they
// re-sample on every execution of a step. Through Phase 2 that was invisible:
// a `completed` step is never re-run, so only a *failed/crashed* attempt ever
// saw fresh values, and that attempt's results were discarded anyway. Phase 3
// changes this honestly — `ctx.sleep()` makes a step partially progress, get
// suspended, and re-run **from the top** when it wakes, so code before the
// sleep runs again and observes a different `now()`/`random()` than it did
// pre-sleep. Steps that need a value to survive a sleep must derive it from
// `ctx.input` (or persist it themselves) rather than from `now()`/`random()`.
// Recording these for true deterministic replay is a future refinement.

import { SleepSignal, parseDuration } from '../engine/sleep.ts'

function notImplemented(feature: string, phase: string): never {
  throw new Error(`ctx.${feature} is not implemented yet (lands in ${phase})`)
}

// A signal that is never aborted — the default when no timeout/cancellation
// is wired in, so `ctx.signal` is always a real AbortSignal and callers never
// have to null-check it.
function neverAbortedSignal(): AbortSignal {
  return new AbortController().signal
}

// Per-context accumulator for `ctx.skip()` requests, keyed by the ctx
// object itself so the public `WorkflowContext` type never has to expose an
// internal "read the requests back" method — `getSkipRequests` is the only
// way in, and it's meant for executor.ts/worker.ts, not step code.
const skipRequestsByContext = new WeakMap<WorkflowContext, string[]>()

/**
 * Read back the step names a just-finished step function passed to
 * `ctx.skip(...)`, in call order, deduplicated. Returns `[]` if `skip` was
 * never called (the overwhelmingly common case) or `ctx` wasn't built by
 * `createWorkflowContext`.
 */
export function getSkipRequests(ctx: WorkflowContext): readonly string[] {
  return skipRequestsByContext.get(ctx) ?? []
}

// Per-context (i.e. per-execution) call counter, mirroring `sleep`'s own
// internal `sleepCalls` and `skipRequestsByContext`'s WeakMap-per-ctx
// pattern. Phase 4's control/child.ts uses this to build a stable,
// replay-safe default key for `spawnChildRun` when the step author doesn't
// supply one: since `ctx` is rebuilt fresh on every execution and a step
// function's code path up to a given call site is assumed deterministic
// (the same assumption `sleep_seq` already relies on), calling this at the
// same point in a step's control flow yields the same sequence number on
// every replay.
const childCallSeqByContext = new WeakMap<WorkflowContext, { value: number }>()

/** Next 1-based call index for `ctx`, starting at 1. See the note above. */
export function nextChildCallSeq(ctx: WorkflowContext): number {
  let counter = childCallSeqByContext.get(ctx)
  if (!counter) {
    counter = { value: 0 }
    childCallSeqByContext.set(ctx, counter)
  }
  counter.value += 1
  return counter.value
}

// The id of the step row this context was built for, kept off the public
// `WorkflowContext` shape (same WeakMap-per-ctx trick as `getSkipRequests`
// and `nextChildCallSeq`) so adding it doesn't change the object step
// authors see. Phase 4 #20 uses it for one thing only: recording
// `run.parent_step_id` on a spawned child, so "which step is awaiting this
// child" is answerable from the child row alone. The *blocking* side needs
// nothing from here — the worker already owns `step.id`/`workerId` at the
// moment it commits the block.
const stepIdByContext = new WeakMap<WorkflowContext, string>()

/** The step id this context belongs to, if the caller supplied one. */
export function getContextStepId(ctx: WorkflowContext): string | undefined {
  return stepIdByContext.get(ctx)
}

export interface WorkflowContext {
  /** The run's input, as passed to enqueue/run. */
  readonly input: unknown
  /** The run's stable id. */
  readonly runId: string

  /**
   * Aborted when this step's timeout elapses or the run is cancelled. Pass it
   * to fetch/child work so an abandoned step stops burning resources.
   */
  readonly signal: AbortSignal

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

  /**
   * Suspend the step for a duration without holding a worker or a connection.
   * Never resolves in the suspend case — it throws a SleepSignal that the
   * worker catches (see the replay note on createWorkflowContext).
   */
  sleep(duration: string | number): Promise<void>

  /** Suspend the run until a named event arrives. Phase 5. */
  waitForEvent<T = unknown>(eventKey: string): Promise<T>

  /**
   * Feature #17, conditional branching: declare that the named sibling
   * steps (by `name`, within this run) are the untaken branch and should
   * never run. Call this before returning from a step function — the
   * request is only a note recorded on this context object; nothing is
   * written to storage here (this function is plain/deterministic, same as
   * `now`/`random`). The caller that runs the step (executor.ts / worker.ts)
   * reads the accumulated names back via `getSkipRequests(ctx)` once the
   * step function has returned, and turns them into `skipStep` calls as
   * part of persisting this step's own successful outcome — so a skip only
   * takes effect if the deciding step itself actually commits.
   *
   * Skipped steps still satisfy any downstream dependency that names them
   * (see engine/scheduler.ts's `dependenciesSatisfied` and
   * engine/dag.ts's `advanceDag`), so a fan-in join past an untaken branch
   * is never left waiting forever.
   */
  skip(...stepNames: string[]): void
}

/**
 * Build the context handed to a step function.
 *
 * **Sleep replay semantics — the subtle part of Phase 3.** A sleeping step
 * goes back to `ready` with a future `run_after`; nothing about the step
 * function's local state is saved. When the step is re-claimed it re-runs
 * **from the very top**, so it will hit the same `ctx.sleep()` call again. To
 * keep that from suspending forever, the durable row counts sleeps already
 * served (`step.sleep_seq`) and this context replays against it: `sleep` keeps
 * its own call counter, and any call whose 1-based index is `<= sleepSeq` has
 * already been served on a previous execution and resolves immediately. Only
 * the first call *beyond* `sleepSeq` actually suspends. So a step with two
 * sleeps runs three times total: seq 1 suspends, then seq 1 is skipped and
 * seq 2 suspends, then both are skipped and the step runs to completion.
 *
 * All new fields are optional — `createWorkflowContext({ runId, input })`
 * keeps its Phase 1/2 behaviour (no sleeps served, never-aborted signal).
 */
export function createWorkflowContext(input: {
  runId: string
  input: unknown
  /** How many sleeps this step has already served (step.sleep_seq). Default 0. */
  sleepSeq?: number
  /** Aborted on step timeout or run cancellation. Default: a never-aborted signal. */
  signal?: AbortSignal
  /** The step row this context runs for. Read back via `getContextStepId`. */
  stepId?: string
}): WorkflowContext {
  const alreadyServed = input.sleepSeq ?? 0
  // Per-context (i.e. per-execution) counter of ctx.sleep() calls made so far.
  let sleepCalls = 0
  const skipRequests: string[] = []

  const ctx: WorkflowContext = {
    input: input.input,
    runId: input.runId,
    signal: input.signal ?? neverAbortedSignal(),
    now: () => new Date(),
    random: () => Math.random(),
    sleep: async (duration: string | number): Promise<void> => {
      // Parse before the replay check so a malformed duration fails loudly on
      // every execution, not only the one that would have suspended.
      const durationMs = parseDuration(duration)
      const seq = ++sleepCalls
      if (seq <= alreadyServed) return
      throw new SleepSignal({
        wakeAt: new Date(Date.now() + durationMs),
        durationMs,
        seq,
      })
    },
    waitForEvent: () => notImplemented('waitForEvent()', 'Phase 5'),
    skip: (...stepNames: string[]): void => {
      for (const name of stepNames) {
        if (!skipRequests.includes(name)) skipRequests.push(name)
      }
    },
  }

  skipRequestsByContext.set(ctx, skipRequests)
  if (input.stepId !== undefined) stepIdByContext.set(ctx, input.stepId)
  return ctx
}
