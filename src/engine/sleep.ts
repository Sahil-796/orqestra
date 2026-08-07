// Pure sleep primitives — no I/O, no Postgres import, unit-testable without
// a database (same posture as engine/retry.ts). This module only defines the
// control-flow signal and the duration grammar; *acting* on a sleep (writing
// the step back to `ready` with a future run_after via repositories.sleepStep
// and releasing the worker) is the worker's job.

const SLEEP_BRAND = '__orqestraSleepSignal'

/**
 * Thrown by ctx.sleep() to unwind the step function. A sleep is not an
 * error condition — it is a control-flow signal that says "suspend this
 * step, put it back on the queue with a future run_after, and release the
 * worker". The worker catches it and must NOT route it through the
 * failure/retry path: a sleep consumes no attempt.
 */
export class SleepSignal extends Error {
  /** Brand, not `instanceof` — see isSleepSignal for why. */
  readonly [SLEEP_BRAND] = true as const
  /** Wall-clock instant the step becomes due again (step.run_after). */
  readonly wakeAt: Date
  readonly durationMs: number
  /** 1-based index of this sleep within the step's execution. */
  readonly seq: number

  constructor(args: { wakeAt: Date; durationMs: number; seq: number }) {
    super(`ctx.sleep(#${args.seq}) for ${args.durationMs}ms until ${args.wakeAt.toISOString()}`)
    this.name = 'SleepSignal'
    this.wakeAt = args.wakeAt
    this.durationMs = args.durationMs
    this.seq = args.seq
  }
}

/**
 * Brand check, deliberately not `instanceof`: Bun's test runner and any setup
 * that loads this module through two different specifiers produce two distinct
 * SleepSignal classes, and `instanceof` across them is false. Misclassifying a
 * sleep as a step failure would burn an attempt and could fail the run, so the
 * property check is the primary test and instanceof is only a fallback.
 */
export function isSleepSignal(value: unknown): value is SleepSignal {
  if (typeof value !== 'object' || value === null) return false
  if ((value as Record<string, unknown>)[SLEEP_BRAND] === true) return true
  return value instanceof SleepSignal
}

const UNIT_MS: Record<string, number> = {
  ms: 1,
  s: 1_000,
  m: 60_000,
  h: 3_600_000,
  d: 86_400_000,
  w: 604_800_000,
}

// One `<number><unit>` term. Anchored scanning below rejects anything the
// grammar doesn't cover rather than silently ignoring the tail — "1h30x"
// must throw, not quietly mean 1h.
const TERM = /^(\d+(?:\.\d+)?)(ms|s|m|h|d|w)/

/**
 * "24h" | "1h30m" | "500ms" | "2d" | 1500 -> milliseconds.
 * Units: ms, s, m, h, d, w. Compound forms ("1h30m") supported.
 * A bare number (or numeric string) is milliseconds. Throws on anything
 * unparseable, negative, or non-finite — fail fast at the call site rather
 * than silently sleeping zero.
 */
export function parseDuration(input: string | number): number {
  if (typeof input === 'number') {
    if (!Number.isFinite(input) || input < 0) {
      throw new Error(`parseDuration: invalid duration ${input}`)
    }
    return input
  }

  const raw = input.trim().toLowerCase()
  if (raw.length === 0) throw new Error('parseDuration: empty duration')

  // Bare numeric string is milliseconds, matching the number overload.
  if (/^\d+(\.\d+)?$/.test(raw)) return Number(raw)

  let rest = raw
  let total = 0
  let terms = 0
  while (rest.length > 0) {
    const match = TERM.exec(rest)
    if (!match) throw new Error(`parseDuration: cannot parse duration "${input}"`)
    const value = Number(match[1])
    const unit = UNIT_MS[match[2] as string]
    if (!Number.isFinite(value) || unit === undefined) {
      throw new Error(`parseDuration: cannot parse duration "${input}"`)
    }
    total += value * unit
    terms += 1
    rest = rest.slice(match[0].length)
  }

  if (terms === 0 || !Number.isFinite(total)) {
    throw new Error(`parseDuration: cannot parse duration "${input}"`)
  }
  return total
}
