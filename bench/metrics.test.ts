import { test, expect } from 'bun:test'
import { percentile, summarize, formatReport } from './metrics.ts'

test('percentile: empty array returns 0', () => {
  expect(percentile([], 50)).toBe(0)
})

test('percentile: single value returns that value regardless of p', () => {
  expect(percentile([42], 0)).toBe(42)
  expect(percentile([42], 50)).toBe(42)
  expect(percentile([42], 99)).toBe(42)
})

test('percentile: p50 of [1..100] is 50.5', () => {
  const values = Array.from({ length: 100 }, (_, i) => i + 1)
  expect(percentile(values, 50)).toBeCloseTo(50.5, 5)
})

test('percentile: p99 of [1..100] interpolates near the top', () => {
  const values = Array.from({ length: 100 }, (_, i) => i + 1)
  // rank = 0.99 * 99 = 98.01 -> between index 98 (value 99) and 99 (value 100)
  expect(percentile(values, 99)).toBeCloseTo(99.01, 5)
})

test('percentile: p0 and p100 are min and max', () => {
  const values = [5, 3, 9, 1, 7]
  expect(percentile(values, 0)).toBe(1)
  expect(percentile(values, 100)).toBe(9)
})

test('percentile: does not require pre-sorted input', () => {
  const unsorted = [100, 1, 50, 25, 75]
  expect(percentile(unsorted, 0)).toBe(1)
  expect(percentile(unsorted, 100)).toBe(100)
})

test('percentile: does not mutate the input array', () => {
  const values = [5, 3, 9, 1, 7]
  const copy = [...values]
  percentile(values, 50)
  expect(values).toEqual(copy)
})

test('summarize: empty input returns all zeros', () => {
  expect(summarize([])).toEqual({ p50: 0, p95: 0, p99: 0, max: 0, mean: 0, min: 0 })
})

test('summarize: single value', () => {
  const s = summarize([10])
  expect(s.p50).toBe(10)
  expect(s.p95).toBe(10)
  expect(s.p99).toBe(10)
  expect(s.max).toBe(10)
  expect(s.min).toBe(10)
  expect(s.mean).toBe(10)
})

test('summarize: fields agree with percentile()', () => {
  const values = Array.from({ length: 200 }, (_, i) => i + 1)
  const s = summarize(values)
  expect(s.p50).toBeCloseTo(percentile(values, 50), 10)
  expect(s.p95).toBeCloseTo(percentile(values, 95), 10)
  expect(s.p99).toBeCloseTo(percentile(values, 99), 10)
  expect(s.max).toBe(200)
  expect(s.min).toBe(1)
  expect(s.mean).toBeCloseTo(100.5, 10)
})

test('summarize: max/min/mean on an unordered sample', () => {
  const values = [4, 8, 15, 16, 23, 42]
  const s = summarize(values)
  expect(s.max).toBe(42)
  expect(s.min).toBe(4)
  expect(s.mean).toBeCloseTo((4 + 8 + 15 + 16 + 23 + 42) / 6, 10)
})

test('formatReport: includes scenario, throughput, and latency lines', () => {
  const report = formatReport({
    scenario: 'inline-solo',
    runs: 100,
    workers: 1,
    wallMs: 1234.5,
    runsPerSec: 81.03,
    latency: { p50: 10, p95: 20, p99: 25, max: 30, mean: 12, min: 5 },
  })
  expect(report).toContain('scenario: inline-solo')
  expect(report).toContain('runs')
  expect(report).toContain('p50')
  expect(report).toContain('p99')
  expect(report).not.toContain('steps ')
})

test('formatReport: includes optional steps, stepsPerSec, and extra fields', () => {
  const report = formatReport({
    scenario: 'worker-fanout',
    runs: 50,
    steps: 500,
    workers: 4,
    wallMs: 2000,
    runsPerSec: 25,
    stepsPerSec: 250,
    latency: { p50: 1, p95: 2, p99: 3, max: 4, mean: 1.5, min: 0.5 },
    extra: { claimAttempts: 600, claimsFound: 500 },
  })
  expect(report).toContain('steps')
  expect(report).toContain('stepsPerSec')
  expect(report).toContain('claimAttempts')
  expect(report).toContain('600')
})
