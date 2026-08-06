// Pure unit tests — no Postgres. The sleep primitives and the sleep replay
// logic in the workflow context are deliberately I/O-free so they can be
// tested like this.

import { describe, expect, test } from 'bun:test'
import { SleepSignal, isSleepSignal, parseDuration } from '../src/engine/sleep.ts'
import { createWorkflowContext } from '../src/define/context.ts'

describe('parseDuration', () => {
  test('single-unit forms', () => {
    expect(parseDuration('500ms')).toBe(500)
    expect(parseDuration('30s')).toBe(30_000)
    expect(parseDuration('5m')).toBe(300_000)
    expect(parseDuration('24h')).toBe(86_400_000)
    expect(parseDuration('2d')).toBe(172_800_000)
    expect(parseDuration('1w')).toBe(604_800_000)
  })

  test('compound forms sum their terms', () => {
    expect(parseDuration('1h30m')).toBe(5_400_000)
    expect(parseDuration('1d12h')).toBe(129_600_000)
    expect(parseDuration('1m30s500ms')).toBe(90_500)
  })

  test('fractional values and case/whitespace insensitivity', () => {
    expect(parseDuration('1.5h')).toBe(5_400_000)
    expect(parseDuration('  24H ')).toBe(86_400_000)
  })

  test('bare numbers and numeric strings are milliseconds', () => {
    expect(parseDuration(1500)).toBe(1500)
    expect(parseDuration(0)).toBe(0)
    expect(parseDuration('1500')).toBe(1500)
  })

  test('throws on unparseable, negative or non-finite input', () => {
    expect(() => parseDuration('')).toThrow()
    expect(() => parseDuration('   ')).toThrow()
    expect(() => parseDuration('soon')).toThrow()
    expect(() => parseDuration('1h30x')).toThrow()
    expect(() => parseDuration('h')).toThrow()
    expect(() => parseDuration('-5s')).toThrow()
    expect(() => parseDuration(-1)).toThrow()
    expect(() => parseDuration(Number.NaN)).toThrow()
    expect(() => parseDuration(Number.POSITIVE_INFINITY)).toThrow()
  })
})

describe('isSleepSignal', () => {
  test('accepts a SleepSignal and rejects ordinary errors/values', () => {
    const signal = new SleepSignal({ wakeAt: new Date(), durationMs: 10, seq: 1 })
    expect(isSleepSignal(signal)).toBe(true)
    expect(isSleepSignal(new Error('boom'))).toBe(false)
    expect(isSleepSignal(null)).toBe(false)
    expect(isSleepSignal(undefined)).toBe(false)
    expect(isSleepSignal('SleepSignal')).toBe(false)
    expect(isSleepSignal({})).toBe(false)
  })

  test('brand check works without instanceof (duplicate module classes)', () => {
    // Simulates the same class loaded twice: shape-identical, wrong prototype.
    const impostor = { ...new SleepSignal({ wakeAt: new Date(), durationMs: 1, seq: 1 }) }
    expect(impostor instanceof SleepSignal).toBe(false)
    expect(isSleepSignal(impostor)).toBe(true)
  })
})

describe('ctx.sleep', () => {
  test('first sleep throws a SleepSignal with seq 1 and a future wakeAt', async () => {
    const ctx = createWorkflowContext({ runId: 'run-1', input: {} })
    const before = Date.now()
    try {
      await ctx.sleep('1h')
      throw new Error('expected ctx.sleep to throw')
    } catch (err) {
      expect(isSleepSignal(err)).toBe(true)
      const signal = err as SleepSignal
      expect(signal.seq).toBe(1)
      expect(signal.durationMs).toBe(3_600_000)
      expect(signal.wakeAt.getTime()).toBeGreaterThanOrEqual(before + 3_600_000)
    }
  })

  test('with sleepSeq: 1 the first sleep resolves and the second suspends as seq 2', async () => {
    const ctx = createWorkflowContext({ runId: 'run-1', input: {}, sleepSeq: 1 })
    // Already served on the previous execution — resolves without suspending.
    await ctx.sleep('1h')

    let caught: unknown
    try {
      await ctx.sleep('30m')
    } catch (err) {
      caught = err
    }
    expect(isSleepSignal(caught)).toBe(true)
    expect((caught as SleepSignal).seq).toBe(2)
    expect((caught as SleepSignal).durationMs).toBe(1_800_000)
  })

  test('all sleeps replay through once sleepSeq covers them', async () => {
    const ctx = createWorkflowContext({ runId: 'run-1', input: {}, sleepSeq: 2 })
    await ctx.sleep('1h')
    await ctx.sleep('30m')
    // Step body past the last sleep now runs for real.
    expect(true).toBe(true)
  })

  test('a malformed duration throws a plain error, not a SleepSignal', async () => {
    const ctx = createWorkflowContext({ runId: 'run-1', input: {}, sleepSeq: 5 })
    let caught: unknown
    try {
      await ctx.sleep('whenever')
    } catch (err) {
      caught = err
    }
    expect(caught).toBeInstanceOf(Error)
    expect(isSleepSignal(caught)).toBe(false)
  })
})

describe('ctx.signal', () => {
  test('defaults to a real, non-aborted AbortSignal', () => {
    const ctx = createWorkflowContext({ runId: 'run-1', input: {} })
    expect(ctx.signal).toBeInstanceOf(AbortSignal)
    expect(ctx.signal.aborted).toBe(false)
  })

  test('reflects a passed signal, including later aborts', () => {
    const controller = new AbortController()
    const ctx = createWorkflowContext({ runId: 'run-1', input: {}, signal: controller.signal })
    expect(ctx.signal).toBe(controller.signal)
    expect(ctx.signal.aborted).toBe(false)
    controller.abort(new Error('step timeout'))
    expect(ctx.signal.aborted).toBe(true)
  })
})

describe('backwards compatibility', () => {
  test('createWorkflowContext({ runId, input }) still works with no extra fields', () => {
    const ctx = createWorkflowContext({ runId: 'run-1', input: { a: 1 } })
    expect(ctx.runId).toBe('run-1')
    expect(ctx.input).toEqual({ a: 1 })
    expect(ctx.now()).toBeInstanceOf(Date)
    expect(typeof ctx.random()).toBe('number')
  })
})
