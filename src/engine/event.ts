// Pure event-wait primitives — no I/O, no Postgres import, unit-testable
// without a database (same posture as engine/sleep.ts and engine/child.ts).
// This module only defines the control-flow signal `ctx.waitForEvent()`
// throws to suspend a step; *acting* on it (writing the step to `blocked`
// with its wait recorded via repositories.registerStepEventWait, and later
// waking it on a matching publishEvent) is the worker's / control layer's
// job. The split mirrors SleepSignal vs the worker's commitSleep exactly.

const EVENT_WAIT_BRAND = '__orqestraEventWaitSignal'

/**
 * Thrown by `ctx.waitForEvent(name)` to unwind the step function and suspend
 * it until a matching event is published. Not an error — control flow, the
 * same species as `SleepSignal` and `ChildBlockSignal`. The worker catches
 * it, commits `registerStepEventWait` (status `'blocked'`, the wait recorded,
 * lease cleared) fenced on the lease, and hands the worker slot back. The
 * step is woken by `publishEvent` matching its `name` (+ optional
 * `correlationKey`), never by a clock.
 *
 * `seq` is the 1-based index of this wait within the step's execution,
 * mirroring `SleepSignal.seq`: a woken step replays from the top, and the
 * context resolves the first `event_seq` waits from their delivered payloads
 * instead of throwing again, so a step with several waits converges the same
 * way a multi-sleep step does.
 */
export class EventWaitSignal extends Error {
  /** Brand, not `instanceof` — see isEventWaitSignal for why. */
  readonly [EVENT_WAIT_BRAND] = true as const
  readonly eventName: string
  readonly correlationKey: string | undefined
  readonly seq: number

  constructor(args: { eventName: string; correlationKey?: string; seq: number }) {
    const corr = args.correlationKey === undefined ? '' : ` [${args.correlationKey}]`
    super(`orqestra: step is waiting for event "${args.eventName}"${corr} (#${args.seq})`)
    this.name = 'EventWaitSignal'
    this.eventName = args.eventName
    this.correlationKey = args.correlationKey
    this.seq = args.seq
  }
}

/**
 * Brand check, deliberately not `instanceof`: a module loaded through two
 * different specifiers (Bun's test runner does this) produces two distinct
 * classes, and `instanceof` across them is false. Misclassifying an
 * event-wait as a step failure would burn an attempt and could fail the run,
 * so the property check is primary and `instanceof` is only a fallback.
 */
export function isEventWaitSignal(value: unknown): value is EventWaitSignal {
  if (typeof value !== 'object' || value === null) return false
  if ((value as Record<string, unknown>)[EVENT_WAIT_BRAND] === true) return true
  return value instanceof EventWaitSignal
}
