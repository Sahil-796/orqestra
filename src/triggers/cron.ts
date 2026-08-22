// Standard 5-field cron (minute hour day-of-month month day-of-week) — no
// heavy dependency, hand-rolled and unit-tested (see tests/cron.test.ts).
// This file owns the only cron maths in the system: the repository layer
// (`rescheduleCron`) deliberately takes a pre-computed `nextRunAt` and does
// none of its own (see repositories.ts's comment on that function) — the
// trigger daemon (runner.ts/scheduled.ts) is the caller that closes the loop.
//
// Field syntax supported per position: `*`, a bare number, a range `a-b`, a
// comma-separated list of any of those, and a `/step` suffix on `*` or a
// range (`*/5`, `1-20/2`). Day-of-week accepts 0-7 (0 and 7 both mean
// Sunday, matching cron convention). Times are computed in the process's
// local timezone — this is a deliberate simplification for Phase 5; there is
// no per-schedule timezone field in the schema.
//
// dom/dow interplay follows standard cron semantics: if BOTH fields are
// restricted (not `*`), a match is either-or (dom matches OR dow matches);
// if only one is restricted, that one alone decides; if neither is
// restricted, every day matches.

import type { WorkflowHandle } from '../define/workflow.ts'
import { createSchedule, type ScheduleRow } from '../store/repositories.ts'
import type { Db } from '../store/client.ts'

interface FieldRange {
  min: number
  max: number
}

const MINUTE_RANGE: FieldRange = { min: 0, max: 59 }
const HOUR_RANGE: FieldRange = { min: 0, max: 23 }
const DOM_RANGE: FieldRange = { min: 1, max: 31 }
const MONTH_RANGE: FieldRange = { min: 1, max: 12 }
// Parsed with an extended max (7) so "0-7" / bare "7" are accepted here;
// normalized down to 0-6 (7 -> 0) once parsed, see parseCronExpression.
const DOW_PARSE_RANGE: FieldRange = { min: 0, max: 7 }

const SEGMENT_RE = /^(\*|\d+(?:-\d+)?)(?:\/(\d+))?$/

function parseField(raw: string, range: FieldRange, fieldName: string): Set<number> {
  const values = new Set<number>()
  for (const part of raw.split(',')) {
    const match = SEGMENT_RE.exec(part)
    if (!match) {
      throw new Error(`invalid cron ${fieldName} segment "${part}" in expression field "${raw}"`)
    }
    const [, body, stepRaw] = match
    const step = stepRaw !== undefined ? Number(stepRaw) : 1
    if (!Number.isInteger(step) || step <= 0) {
      throw new Error(`invalid cron ${fieldName} step "${stepRaw}" in segment "${part}"`)
    }

    let start: number
    let end: number
    if (body === '*') {
      start = range.min
      end = range.max
    } else if (body!.includes('-')) {
      const [a, b] = body!.split('-')
      start = Number(a)
      end = Number(b)
    } else {
      start = end = Number(body)
    }

    if (
      !Number.isInteger(start) ||
      !Number.isInteger(end) ||
      start > end ||
      start < range.min ||
      end > range.max
    ) {
      throw new Error(
        `invalid cron ${fieldName} range "${part}" — must be within ${range.min}-${range.max}`
      )
    }

    for (let v = start; v <= end; v += step) values.add(v)
  }
  if (values.size === 0) {
    throw new Error(`cron ${fieldName} field "${raw}" produced no valid values`)
  }
  return values
}

export interface ParsedCron {
  minute: Set<number>
  hour: Set<number>
  dom: Set<number>
  month: Set<number>
  /** Already normalized: 7 folded into 0 (Sunday). */
  dow: Set<number>
  domRestricted: boolean
  dowRestricted: boolean
}

/** Parse (and validate) a 5-field cron expression. Throws a descriptive error on malformed input. */
export function parseCronExpression(expr: string): ParsedCron {
  const fields = expr.trim().split(/\s+/)
  if (fields.length !== 5) {
    throw new Error(
      `cron expression must have exactly 5 fields (minute hour dom month dow), got ${fields.length}: "${expr}"`
    )
  }
  const [minuteRaw, hourRaw, domRaw, monthRaw, dowRaw] = fields as [
    string,
    string,
    string,
    string,
    string,
  ]

  const minute = parseField(minuteRaw, MINUTE_RANGE, 'minute')
  const hour = parseField(hourRaw, HOUR_RANGE, 'hour')
  const dom = parseField(domRaw, DOM_RANGE, 'day-of-month')
  const month = parseField(monthRaw, MONTH_RANGE, 'month')
  const dowParsed = parseField(dowRaw, DOW_PARSE_RANGE, 'day-of-week')

  const dow = new Set<number>()
  for (const v of dowParsed) dow.add(v === 7 ? 0 : v)

  return {
    minute,
    hour,
    dom,
    month,
    dow,
    domRestricted: domRaw !== '*',
    dowRestricted: dowRaw !== '*',
  }
}

/** Throws if `expr` is not a valid 5-field cron expression; returns nothing on success. */
export function validateCronExpression(expr: string): void {
  parseCronExpression(expr)
}

function matches(cron: ParsedCron, date: Date): boolean {
  if (!cron.minute.has(date.getMinutes())) return false
  if (!cron.hour.has(date.getHours())) return false
  if (!cron.month.has(date.getMonth() + 1)) return false

  const domHit = cron.dom.has(date.getDate())
  const dowHit = cron.dow.has(date.getDay())

  if (cron.domRestricted && cron.dowRestricted) return domHit || dowHit
  if (cron.domRestricted) return domHit
  if (cron.dowRestricted) return dowHit
  return true
}

// Enough minutes to cover 4 full years at minute resolution — generous
// headroom for the rarest rollover (e.g. a Feb-29-only expression) without
// risking a runaway loop on a genuinely impossible expression (dom=31 and
// month=2, which never matches — this cap turns that into a clear error
// instead of an infinite loop).
const MAX_SEARCH_MINUTES = 4 * 366 * 24 * 60

/**
 * The next time at or after `from` (exclusive — always strictly later, even
 * if `from` itself lands on a minute boundary that matches) that `expr`
 * fires. This is the sole place cron maths happens; callers (scheduled.ts)
 * pass the result straight to `rescheduleCron`.
 */
export function computeNextRun(expr: string, from: Date): Date {
  const cron = parseCronExpression(expr)
  const candidate = new Date(from.getTime())
  candidate.setSeconds(0, 0)
  candidate.setMinutes(candidate.getMinutes() + 1)

  for (let i = 0; i < MAX_SEARCH_MINUTES; i++) {
    if (matches(cron, candidate)) return candidate
    candidate.setMinutes(candidate.getMinutes() + 1)
  }
  throw new Error(`computeNextRun: no matching time found for "${expr}" within the search window`)
}

// ---- syncing cron triggers to schedule rows -------------------------------
//
// At daemon startup, every workflow declared with a `{type:'cron'}` trigger
// (see WorkflowHandle.triggers, define/workflow.ts) should have exactly one
// enabled `cron` schedule row driving it. `createSchedule` has no unique
// constraint on (workflow_name, cron_expression) and repositories.ts exposes
// no "does a schedule already exist for this workflow" lookup, so this
// module tracks what it has already created *within this process* to avoid
// re-registering on every call within a single daemon lifetime.
//
// This does NOT make registration safe across a process restart: a fresh
// process has an empty guard set and, lacking a lookup-by-workflow-name
// repository function, cannot tell whether a schedule row from a previous
// run already exists — it will insert a duplicate. Closing that gap needs a
// small addition to repositories.ts (e.g.
// `getSchedulesByWorkflowName(sql, name, kind)`), which is outside this
// agent's allowlist; flagged in the handoff report rather than made here.
const syncedCronKeys = new Set<string>()

export interface SyncCronSchedulesResult {
  created: ScheduleRow[]
  skipped: number
}

/**
 * Ensure each `{type:'cron'}` trigger on the given workflow handles has a
 * corresponding enabled `cron` schedule row, creating one via
 * `createSchedule` where missing. Call once at daemon startup.
 */
export async function syncCronSchedules(
  sql: Db,
  workflows: readonly WorkflowHandle[],
  opts: { now?: Date } = {}
): Promise<SyncCronSchedulesResult> {
  const now = opts.now ?? new Date()
  const created: ScheduleRow[] = []
  let skipped = 0

  for (const workflow of workflows) {
    for (const trigger of workflow.triggers) {
      if (trigger.type !== 'cron') continue

      const key = `${workflow.name}::${trigger.cron}`
      if (syncedCronKeys.has(key)) {
        skipped++
        continue
      }

      validateCronExpression(trigger.cron)
      const nextRunAt = computeNextRun(trigger.cron, now)
      const row = await createSchedule(sql, {
        workflowName: workflow.name,
        kind: 'cron',
        cronExpression: trigger.cron,
        nextRunAt,
      })
      syncedCronKeys.add(key)
      created.push(row)
    }
  }

  return { created, skipped }
}

/** Test-only escape hatch: clear the in-process "already synced" guard. */
export function __resetCronSyncGuardForTests(): void {
  syncedCronKeys.clear()
}
