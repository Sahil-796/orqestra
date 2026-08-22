// Schedule firing (#23 recurring cron, #24 delayed/one-shot starts). One
// poll tick: claim whatever schedules are due, start a run for each, then
// close the loop — `markScheduleFired` disables a 'once' row,
// `rescheduleCron` advances a 'cron' row to its next occurrence (computed
// here, in cron.ts's computeNextRun — repositories.ts does no cron maths).
//
// `claimDueSchedules` already gives at-most-once-per-tick semantics (FOR
// UPDATE SKIP LOCKED + a forward guard bump on next_run_at), so two
// concurrent poll ticks — same process or two daemon instances — never both
// claim the same due row. This module just has to not lose a claimed row:
// if starting the run throws, we leave the schedule as claimed-but-not-yet-
// rescheduled; the guard bump `claimDueSchedules` already applied means it
// naturally becomes due again (and gets retried) once that bump elapses,
// rather than firing twice or getting stuck.

import type { Db } from '../store/client.ts'
import {
  claimDueSchedules,
  markScheduleFired,
  rescheduleCron,
  startRunForWorkflowName,
  type ScheduleRow,
} from '../store/repositories.ts'
import { computeNextRun } from './cron.ts'

export interface PollSchedulesOptions {
  now?: Date
  limit?: number
  guardMs?: number
}

export interface ScheduleFireError {
  schedule: ScheduleRow
  error: unknown
}

export interface PollSchedulesResult {
  /** Schedules claimed as due this tick. */
  claimed: number
  /** Of those, how many successfully started a run. */
  started: number
  errors: ScheduleFireError[]
}

/** One poll tick: claim due schedules and fire each. Safe to call repeatedly on an interval. */
export async function pollDueSchedules(
  sql: Db,
  opts: PollSchedulesOptions = {}
): Promise<PollSchedulesResult> {
  const now = opts.now ?? new Date()
  const due = await claimDueSchedules(sql, now, opts.limit, opts.guardMs)
  const result: PollSchedulesResult = { claimed: due.length, started: 0, errors: [] }

  for (const schedule of due) {
    try {
      await startRunForWorkflowName(sql, {
        workflowName: schedule.workflow_name,
        input: schedule.input,
        namespace: schedule.namespace,
        priority: schedule.priority,
      })
      result.started++

      const firedAt = new Date()
      if (schedule.kind === 'once') {
        await markScheduleFired(sql, schedule.id, firedAt)
        continue
      }

      // kind === 'cron' beyond this point (schema CHECK enforces the two
      // kinds are exhaustive); cron_expression is required for 'cron' rows.
      const expr = schedule.cron_expression
      if (!expr) {
        // Shouldn't happen given the CHECK constraint pairing kind='cron'
        // with a non-null cron_expression — but if it ever does, disabling
        // via markScheduleFired is safer than leaving a cron row that can
        // never be rescheduled (rescheduleCron would just no-op forever).
        await markScheduleFired(sql, schedule.id, firedAt)
        continue
      }
      const nextRunAt = computeNextRun(expr, firedAt)
      await rescheduleCron(sql, schedule.id, nextRunAt, firedAt)
    } catch (error) {
      result.errors.push({ schedule, error })
    }
  }

  return result
}
