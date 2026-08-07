import { describe, expect, test } from 'bun:test'
import {
  backoffMs,
  nextRunAfter,
  shouldRetry,
  DEFAULT_RETRY_POLICY,
  type RetryPolicy,
} from '../src/engine/retry.ts'

const NO_JITTER: RetryPolicy = { baseMs: 100, factor: 2, maxMs: 30_000, jitter: false }

describe('backoffMs', () => {
  test('grows exponentially without jitter: base * factor^(attempt-1)', () => {
    expect(backoffMs(1, NO_JITTER)).toBe(100)
    expect(backoffMs(2, NO_JITTER)).toBe(200)
    expect(backoffMs(3, NO_JITTER)).toBe(400)
    expect(backoffMs(4, NO_JITTER)).toBe(800)
  })

  test('caps at maxMs', () => {
    expect(backoffMs(20, NO_JITTER)).toBe(30_000)
  })

  test('jittered result is always <= the unjittered ceiling and always > 0', () => {
    for (let attempt = 1; attempt <= 12; attempt++) {
      const ceiling = backoffMs(attempt, NO_JITTER)
      for (let i = 0; i < 50; i++) {
        const jittered = backoffMs(attempt, DEFAULT_RETRY_POLICY)
        expect(jittered).toBeGreaterThan(0)
        expect(jittered).toBeLessThanOrEqual(ceiling)
      }
    }
  })

  test('guards against non-positive/NaN attempts by treating them as attempt 1', () => {
    expect(backoffMs(0, NO_JITTER)).toBe(backoffMs(1, NO_JITTER))
    expect(backoffMs(-5, NO_JITTER)).toBe(backoffMs(1, NO_JITTER))
    expect(backoffMs(Number.NaN, NO_JITTER)).toBe(backoffMs(1, NO_JITTER))
  })
})

describe('shouldRetry', () => {
  test('true while attempts remain, false once the ceiling is reached', () => {
    expect(shouldRetry(1, 3)).toBe(true)
    expect(shouldRetry(2, 3)).toBe(true)
    expect(shouldRetry(3, 3)).toBe(false)
    expect(shouldRetry(4, 3)).toBe(false)
  })

  test('maxAttempts of 1 never retries', () => {
    expect(shouldRetry(1, 1)).toBe(false)
  })
})

describe('nextRunAfter', () => {
  test('adds backoffMs(attempt) to now', () => {
    const now = new Date('2026-01-01T00:00:00.000Z')
    const result = nextRunAfter(2, now, NO_JITTER)
    expect(result.getTime()).toBe(now.getTime() + 200)
  })
})
