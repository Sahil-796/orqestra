// Pure unit tests for cron.ts — no DB. Proves computeNextRun for a handful
// of expressions, including field rollovers (minute -> hour -> day -> month
// -> year) and the dom/dow either-or interplay.

import { describe, expect, test } from 'bun:test'
import { computeNextRun, parseCronExpression, validateCronExpression } from '../src/triggers/cron.ts'

describe('parseCronExpression / validateCronExpression', () => {
  test('rejects a wrong field count', () => {
    expect(() => validateCronExpression('* * * *')).toThrow()
    expect(() => validateCronExpression('* * * * * *')).toThrow()
  })

  test('rejects an out-of-range value', () => {
    expect(() => validateCronExpression('60 * * * *')).toThrow()
    expect(() => validateCronExpression('* 24 * * *')).toThrow()
    expect(() => validateCronExpression('* * 32 * *')).toThrow()
    expect(() => validateCronExpression('* * * 13 *')).toThrow()
    expect(() => validateCronExpression('* * * * 8')).toThrow()
  })

  test('rejects garbage segments', () => {
    expect(() => validateCronExpression('a * * * *')).toThrow()
    expect(() => validateCronExpression('*/0 * * * *')).toThrow()
  })

  test('accepts wildcards, lists, ranges, and steps', () => {
    expect(() => validateCronExpression('* * * * *')).not.toThrow()
    expect(() => validateCronExpression('0,15,30,45 * * * *')).not.toThrow()
    expect(() => validateCronExpression('0-29 * * * *')).not.toThrow()
    expect(() => validateCronExpression('*/15 * * * *')).not.toThrow()
    expect(() => validateCronExpression('0 9-17/2 * * 1-5')).not.toThrow()
  })

  test('day-of-week 7 is accepted as an alias for Sunday (0)', () => {
    const cron = parseCronExpression('0 0 * * 7')
    expect(cron.dow.has(0)).toBe(true)
    expect(cron.dow.has(7)).toBe(false)
  })
})

describe('computeNextRun', () => {
  test('every minute: next run is exactly one minute after `from`, seconds truncated', () => {
    const from = new Date(2026, 0, 1, 10, 30, 45, 500)
    const next = computeNextRun('* * * * *', from)
    expect(next.getFullYear()).toBe(2026)
    expect(next.getMonth()).toBe(0)
    expect(next.getDate()).toBe(1)
    expect(next.getHours()).toBe(10)
    expect(next.getMinutes()).toBe(31)
    expect(next.getSeconds()).toBe(0)
    expect(next.getMilliseconds()).toBe(0)
  })

  test('is always strictly after `from`, even when `from` lands on a matching minute', () => {
    const from = new Date(2026, 0, 1, 10, 30, 0, 0) // already :30, on-the-hour-ish
    const next = computeNextRun('30 * * * *', from)
    expect(next.getTime()).toBeGreaterThan(from.getTime())
    expect(next.getHours()).toBe(11)
    expect(next.getMinutes()).toBe(30)
  })

  test('hour rollover: last minute of the hour rolls into the next hour', () => {
    const from = new Date(2026, 0, 1, 10, 59, 0, 0)
    const next = computeNextRun('0 * * * *', from)
    expect(next.getHours()).toBe(11)
    expect(next.getMinutes()).toBe(0)
  })

  test('day rollover: "0 0 * * *" (midnight) from late in the day lands the next calendar day', () => {
    const from = new Date(2026, 0, 1, 23, 59, 59, 0)
    const next = computeNextRun('0 0 * * *', from)
    expect(next.getDate()).toBe(2)
    expect(next.getHours()).toBe(0)
    expect(next.getMinutes()).toBe(0)
  })

  test('month rollover: last day of January rolls into February', () => {
    const from = new Date(2026, 0, 31, 23, 59, 0, 0)
    const next = computeNextRun('0 0 1 * *', from) // midnight on the 1st of any month
    expect(next.getMonth()).toBe(1) // February
    expect(next.getDate()).toBe(1)
  })

  test('year rollover: Dec 31 -> Jan 1', () => {
    const from = new Date(2026, 11, 31, 23, 59, 0, 0)
    const next = computeNextRun('0 0 * * *', from)
    expect(next.getFullYear()).toBe(2027)
    expect(next.getMonth()).toBe(0)
    expect(next.getDate()).toBe(1)
  })

  test('specific weekday: "0 9 * * 1" fires on the next Monday at 9am', () => {
    // 2026-08-22 is a Saturday.
    const from = new Date(2026, 7, 22, 12, 0, 0, 0)
    const next = computeNextRun('0 9 * * 1', from)
    expect(next.getDay()).toBe(1) // Monday
    expect(next.getHours()).toBe(9)
    expect(next.getMinutes()).toBe(0)
    expect(next.getTime()).toBeGreaterThan(from.getTime())
  })

  test('dom/dow either-or: "0 0 1 * 1" fires on the 1st of the month OR any Monday, whichever is sooner', () => {
    // 2026-08-22 is a Saturday; the next Monday (Aug 24) precedes Sept 1.
    const from = new Date(2026, 7, 22, 0, 0, 0, 0)
    const next = computeNextRun('0 0 1 * 1', from)
    expect(next.getDate()).toBe(24)
    expect(next.getDay()).toBe(1)
  })

  test('step values: "*/15 * * * *" fires on the quarter hours', () => {
    const from = new Date(2026, 0, 1, 10, 16, 0, 0)
    const next = computeNextRun('*/15 * * * *', from)
    expect(next.getMinutes()).toBe(30)
  })

  test('impossible expression (Feb 31st) throws rather than looping forever', () => {
    expect(() => computeNextRun('0 0 31 2 *', new Date(2026, 0, 1))).toThrow()
  })
})
