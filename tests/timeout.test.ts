// Unit coverage for engine/timeout.ts — pure, no Postgres. Everything here
// is about the *contract the worker relies on*: only the budget produces a
// StepTimeoutError, a parent abort propagates in without being relabelled,
// control-flow signals pass through untouched, and no timer is ever leaked.

import { describe, expect, test } from 'bun:test'
import { StepTimeoutError, isStepTimeoutError, withTimeout } from '../src/engine/timeout.ts'
import { SleepSignal, isSleepSignal } from '../src/engine/sleep.ts'

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

describe('isStepTimeoutError', () => {
  test('recognizes a StepTimeoutError and nothing else', () => {
    expect(isStepTimeoutError(new StepTimeoutError(100))).toBe(true)
    expect(isStepTimeoutError(new Error('boom'))).toBe(false)
    expect(isStepTimeoutError(null)).toBe(false)
    expect(isStepTimeoutError(undefined)).toBe(false)
    expect(isStepTimeoutError('timeout')).toBe(false)
  })

  test('recognizes a structurally-branded copy (two module instances)', () => {
    // The reason this is a brand check and not `instanceof`: a duplicate class
    // identity must still be classified correctly.
    const lookalike = Object.assign(new Error('step exceeded its 5ms timeout'), {
      __orqestraStepTimeoutError: true,
      timeoutMs: 5,
    })
    expect(isStepTimeoutError(lookalike)).toBe(true)
  })

  test('carries the budget it blew', () => {
    const error = new StepTimeoutError(250)
    expect(error.timeoutMs).toBe(250)
    expect(error.name).toBe('StepTimeoutError')
    expect(error.message).toContain('250')
  })
})

describe('withTimeout', () => {
  test('resolves with the step value when it finishes inside the budget', async () => {
    const value = await withTimeout(async () => {
      await delay(5)
      return 'done'
    }, 1_000)
    expect(value).toBe('done')
  })

  test('rejects with StepTimeoutError once the budget expires', async () => {
    let caught: unknown
    try {
      await withTimeout(() => delay(5_000).then(() => 'too late'), 20)
    } catch (e) {
      caught = e
    }
    expect(isStepTimeoutError(caught)).toBe(true)
    expect((caught as StepTimeoutError).timeoutMs).toBe(20)
  })

  test('aborts the signal it handed the step, so a cooperative step can bail', async () => {
    let sawAbort = false
    const caught = await withTimeout((signal) => {
      return new Promise<string>((_resolve, reject) => {
        signal.addEventListener('abort', () => {
          sawAbort = true
          reject(new Error('step bailed on abort'))
        })
        setTimeout(() => _resolve('never'), 5_000).unref?.()
      })
    }, 20).catch((e) => e)

    expect(sawAbort).toBe(true)
    // The timeout is what the *caller* sees — the step's own bail-out loses
    // the race it was itself triggered by, which is exactly right: the reason
    // the step stopped is that it ran out of budget.
    expect(isStepTimeoutError(caught)).toBe(true)
  })

  test('does not time out when timeoutMs is null, undefined or non-positive', async () => {
    const slowish = async () => {
      await delay(30)
      return 'finished'
    }
    expect(await withTimeout(slowish, null)).toBe('finished')
    expect(await withTimeout(slowish, undefined)).toBe('finished')
    expect(await withTimeout(slowish, 0)).toBe('finished')
    expect(await withTimeout(slowish, -1)).toBe('finished')
  })

  test('still hands the step a usable signal when there is no budget', async () => {
    const seen = await withTimeout(async (signal) => signal instanceof AbortSignal, null)
    expect(seen).toBe(true)
  })

  test('a parent abort propagates in and is NOT relabelled as a timeout', async () => {
    const parent = new AbortController()
    setTimeout(() => parent.abort(new Error('cancelled by the run')), 10)

    const caught = await withTimeout(
      (signal) =>
        new Promise<string>((_resolve, reject) => {
          signal.addEventListener('abort', () => reject(new Error('step observed cancellation')))
        }),
      5_000,
      parent.signal
    ).catch((e) => e)

    // This distinction is load-bearing for worker.ts: an aborted-by-cancel
    // step must not be recorded as a timeout.
    expect(isStepTimeoutError(caught)).toBe(false)
    expect((caught as Error).message).toBe('step observed cancellation')
  })

  test('an already-aborted parent aborts the step signal immediately', async () => {
    const parent = new AbortController()
    parent.abort(new Error('cancelled before we even started'))

    const abortedAtEntry = await withTimeout(async (signal) => signal.aborted, 1_000, parent.signal)
    expect(abortedAtEntry).toBe(true)
  })

  test('passes a SleepSignal straight through — a sleep is never a timeout', async () => {
    const signal = new SleepSignal({ wakeAt: new Date(Date.now() + 60_000), durationMs: 60_000, seq: 1 })
    const caught = await withTimeout(async () => {
      throw signal
    }, 1_000).catch((e) => e)

    expect(isSleepSignal(caught)).toBe(true)
    expect(isStepTimeoutError(caught)).toBe(false)
    expect(caught).toBe(signal)
  })

  test('an ordinary throw is passed through unchanged', async () => {
    const boom = new Error('the step itself failed')
    const caught = await withTimeout(async () => {
      throw boom
    }, 1_000).catch((e) => e)
    expect(caught).toBe(boom)
  })

  test('clears its timer on the success path (no lingering handle keeps the loop alive)', async () => {
    // Bun/Node keep the event loop alive for a pending timer. If withTimeout
    // leaked its 60s timer here, this test file would hang at exit rather than
    // fail an assertion — so we assert on the observable side effect instead:
    // a fired timer would abort the signal, and it must not.
    let abortedAfterSuccess = false
    let captured: AbortSignal | undefined
    const value = await withTimeout(async (signal) => {
      captured = signal
      return 'quick'
    }, 40)
    expect(value).toBe('quick')

    await delay(80) // well past the budget
    abortedAfterSuccess = captured?.aborted ?? false
    // The timer was cleared, so nothing aborted the signal after the fact.
    expect(abortedAfterSuccess).toBe(false)
  })

  test('unsubscribes from the parent signal after it settles', async () => {
    const parent = new AbortController()
    const value = await withTimeout(async () => 'ok', 1_000, parent.signal)
    expect(value).toBe('ok')
    // Aborting the parent after the fact must not throw or reach anything.
    expect(() => parent.abort(new Error('late'))).not.toThrow()
  })

  test('the losing side of the race never surfaces as an unhandled rejection', async () => {
    // fn rejects AFTER the timeout already won. Promise.race attached a
    // handler to it, so this must stay quiet rather than crash the process.
    const caught = await withTimeout(
      () => new Promise<string>((_resolve, reject) => setTimeout(() => reject(new Error('late failure')), 40)),
      10
    ).catch((e) => e)
    expect(isStepTimeoutError(caught)).toBe(true)
    await delay(80)
  })
})
