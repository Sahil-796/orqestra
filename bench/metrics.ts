// Shared metrics vocabulary for the bench suite. Pure and dependency-free —
// no DB, no engine imports — so every bench harness (inline, worker,
// distributed-worker, ...) can import this without pulling in Postgres, and
// so it's unit-testable without bringing up a database.

/**
 * Percentile of a numeric sample, using linear interpolation between the two
 * nearest ranks (the same method spreadsheet software calls "inclusive").
 *
 * `p` is in [0, 100]. `values` does NOT need to be pre-sorted — this sorts a
 * defensive copy internally (ascending), so callers can hand it a raw sample
 * without worrying about mutating their own array or getting a wrong answer
 * from unsorted input. If your caller already has a sorted array and wants to
 * skip the copy for a hot path, sort once and slice `summarize`'s internals
 * directly instead of calling this in a loop.
 *
 * Empty input returns 0 (not NaN) — a zero-sample bench run reads as "no
 * data yet" rather than poisoning any arithmetic downstream (e.g. a report
 * table that sums or compares latencies).
 */
export function percentile(sorted: number[], p: number): number {
  if (sorted.length === 0) return 0
  const copy = [...sorted].sort((a, b) => a - b)
  if (copy.length === 1) return copy[0] ?? 0
  const clamped = Math.min(100, Math.max(0, p))
  const rank = (clamped / 100) * (copy.length - 1)
  const lowerIndex = Math.floor(rank)
  const upperIndex = Math.ceil(rank)
  const lower = copy[lowerIndex] ?? 0
  const upper = copy[upperIndex] ?? lower
  const frac = rank - lowerIndex
  return lower + (upper - lower) * frac
}

export interface Summary {
  p50: number
  p95: number
  p99: number
  max: number
  mean: number
  min: number
}

/** Empty input returns all-zero fields. */
export function summarize(values: number[]): Summary {
  if (values.length === 0) {
    return { p50: 0, p95: 0, p99: 0, max: 0, mean: 0, min: 0 }
  }
  const sorted = [...values].sort((a, b) => a - b)
  const sum = sorted.reduce((acc, v) => acc + v, 0)
  return {
    p50: percentile(sorted, 50),
    p95: percentile(sorted, 95),
    p99: percentile(sorted, 99),
    max: sorted[sorted.length - 1] ?? 0,
    mean: sum / sorted.length,
    min: sorted[0] ?? 0,
  }
}

export interface ReportInput {
  scenario: string
  runs: number
  steps?: number
  workers: number
  wallMs: number
  runsPerSec: number
  stepsPerSec?: number
  latency: Summary
  extra?: Record<string, string | number>
}

function fmtNum(n: number): string {
  return Number.isInteger(n) ? String(n) : n.toFixed(2)
}

function line(label: string, value: string): string {
  return `  ${label.padEnd(16)} ${value}`
}

/**
 * Readable, greppable multi-line text report. Each field lives on its own
 * `label: value` line so `grep p99` (or `grep scenario`) against a saved
 * report file finds it directly.
 */
export function formatReport(input: ReportInput): string {
  const lines: string[] = []
  lines.push(`scenario: ${input.scenario}`)
  lines.push(line('runs', String(input.runs)))
  if (input.steps !== undefined) lines.push(line('steps', String(input.steps)))
  lines.push(line('workers', String(input.workers)))
  lines.push(line('wallMs', fmtNum(input.wallMs)))
  lines.push(line('runsPerSec', fmtNum(input.runsPerSec)))
  if (input.stepsPerSec !== undefined) lines.push(line('stepsPerSec', fmtNum(input.stepsPerSec)))
  lines.push('  latency (ms):')
  lines.push(line('p50', fmtNum(input.latency.p50)))
  lines.push(line('p95', fmtNum(input.latency.p95)))
  lines.push(line('p99', fmtNum(input.latency.p99)))
  lines.push(line('max', fmtNum(input.latency.max)))
  lines.push(line('mean', fmtNum(input.latency.mean)))
  lines.push(line('min', fmtNum(input.latency.min)))
  if (input.extra) {
    lines.push('  extra:')
    for (const [key, value] of Object.entries(input.extra)) {
      lines.push(line(key, String(value)))
    }
  }
  return lines.join('\n')
}
