// DB-backed proof for scheduled.ts's pollDueSchedules (#23 cron, #24
// delayed/one-shot). Each test uses a unique namespace and a uniquely-named
// workflow, per Agent 1's test-isolation note: leftover incomplete runs on
// the shared 'default' namespace can starve unrelated claim loops.
//
// The dev DB is shared with Agent 2's tests, which register their own
// schedules against workflow names that may not (yet, or ever) be
// registered in this process — a global `pollDueSchedules` call can
// therefore claim rows that are none of this suite's business and fail to
// start them. Assertions here deliberately key off this test's own
// schedule/workflow id rather than the poll result's aggregate counts, so
// that cross-suite noise in the shared schedules table can't make an
// otherwise-correct run look broken (or vice versa).

import { describe, expect, test, beforeAll, afterAll } from 'bun:test'
import { createDb } from '../src/store/client.ts'
import { migrate } from '../src/store/migrate.ts'
import {
  createSchedule,
  getIncompleteRuns,
  getSchedule,
  getWorkflowByName,
  insertWorkflow,
} from '../src/store/repositories.ts'
import type { WorkflowDefinition } from '../src/types.ts'
import { pollDueSchedules } from '../src/triggers/scheduled.ts'
import { computeNextRun } from '../src/triggers/cron.ts'

const sql = createDb()

beforeAll(async () => {
  await migrate(sql)
})

afterAll(async () => {
  await sql.end()
})

function uniqueName(prefix: string): string {
  return `${prefix}-${crypto.randomUUID()}`
}

async function registerWorkflow(namePrefix: string): Promise<{ name: string; id: string }> {
  const name = uniqueName(namePrefix)
  const dag: WorkflowDefinition = {
    name,
    version: 1,
    steps: [{ name: 'only', dependsOn: [], maxAttempts: 1, priority: 0 }],
  }
  const row = await insertWorkflow(sql, { name, dag })
  return { name, id: row.id }
}

async function runsForWorkflow(workflowId: string) {
  const incomplete = await getIncompleteRuns(sql)
  return incomplete.filter((r) => r.workflow_id === workflowId)
}

describe('pollDueSchedules', () => {
  test('a due `once` schedule is claimed, promoted to exactly one run, then disabled', async () => {
    const workflow = await registerWorkflow('sched-once')
    const namespace = uniqueName('ns-once')
    const past = new Date(Date.now() - 60_000)

    const schedule = await createSchedule(sql, {
      workflowName: workflow.name,
      kind: 'once',
      nextRunAt: past,
      namespace,
    })

    const result = await pollDueSchedules(sql, { limit: 200 })
    expect(result.errors.find((e) => e.schedule.id === schedule.id)).toBeUndefined()

    const after = await getSchedule(sql, schedule.id)
    expect(after?.enabled).toBe(false)
    expect(after?.last_fired_at).not.toBeNull()

    const runs = await runsForWorkflow(workflow.id)
    expect(runs.length).toBe(1)
    expect(runs[0]?.namespace).toBe(namespace)
  })

  test('two concurrent poll ticks never double-fire the same `once` schedule', async () => {
    const workflow = await registerWorkflow('sched-once-race')
    const namespace = uniqueName('ns-once-race')
    const past = new Date(Date.now() - 60_000)

    const schedule = await createSchedule(sql, {
      workflowName: workflow.name,
      kind: 'once',
      nextRunAt: past,
      namespace,
    })

    await Promise.all([pollDueSchedules(sql, { limit: 200 }), pollDueSchedules(sql, { limit: 200 })])

    // FOR UPDATE SKIP LOCKED means at most one of the two concurrent ticks
    // could have claimed this row — so exactly one run should exist for it,
    // no matter how the race landed.
    const runs = await runsForWorkflow(workflow.id)
    expect(runs.length).toBe(1)

    const after = await getSchedule(sql, schedule.id)
    expect(after?.enabled).toBe(false)
  })

  test('a due `cron` schedule fires and is rescheduled forward, not disabled', async () => {
    const workflow = await registerWorkflow('sched-cron')
    const namespace = uniqueName('ns-cron')
    const past = new Date(Date.now() - 60_000)
    const cronExpression = '* * * * *' // every minute

    const schedule = await createSchedule(sql, {
      workflowName: workflow.name,
      kind: 'cron',
      cronExpression,
      nextRunAt: past,
      namespace,
    })

    const beforePoll = new Date()
    const result = await pollDueSchedules(sql, { limit: 200 })
    expect(result.errors.find((e) => e.schedule.id === schedule.id)).toBeUndefined()

    const after = await getSchedule(sql, schedule.id)
    expect(after?.enabled).toBe(true)
    expect(after?.last_fired_at).not.toBeNull()
    // next_run_at should have advanced to computeNextRun's answer, strictly
    // after the moment we fired — not just the guard-bump value.
    expect(after!.next_run_at.getTime()).toBeGreaterThan(beforePoll.getTime())
    const expectedFloor = computeNextRun(cronExpression, beforePoll).getTime()
    expect(after!.next_run_at.getTime()).toBeLessThanOrEqual(expectedFloor + 60_000)

    const runs = await runsForWorkflow(workflow.id)
    expect(runs.length).toBe(1)
  })

  test('a schedule not yet due is left untouched', async () => {
    const workflow = await registerWorkflow('sched-future')
    const namespace = uniqueName('ns-future')
    const future = new Date(Date.now() + 3_600_000)

    const schedule = await createSchedule(sql, {
      workflowName: workflow.name,
      kind: 'once',
      nextRunAt: future,
      namespace,
    })

    await pollDueSchedules(sql, { limit: 200 })

    const after = await getSchedule(sql, schedule.id)
    expect(after?.enabled).toBe(true)
    expect(after?.last_fired_at).toBeNull()

    const runs = await runsForWorkflow(workflow.id)
    expect(runs.length).toBe(0)
  })

  test('a fired run carries the schedule input and namespace through', async () => {
    const workflow = await registerWorkflow('sched-run-shape')
    const namespace = uniqueName('ns-run-shape')
    const past = new Date(Date.now() - 60_000)

    await createSchedule(sql, {
      workflowName: workflow.name,
      kind: 'once',
      nextRunAt: past,
      namespace,
      input: { hello: 'schedule' },
    })

    await pollDueSchedules(sql, { limit: 200 })

    const runs = await runsForWorkflow(workflow.id)
    expect(runs.length).toBe(1)
    expect(runs[0]?.namespace).toBe(namespace)
    expect(runs[0]?.input).toEqual({ hello: 'schedule' })
  })
})

// Sanity check that getWorkflowByName + a fired schedule agree on the same
// workflow id (guards against a typo'd column name silently matching
// nothing and the `runsForWorkflow` filter above passing vacuously).
test('registerWorkflow helper returns the id getWorkflowByName would resolve to', async () => {
  const workflow = await registerWorkflow('sched-helper-sanity')
  const found = await getWorkflowByName(sql, workflow.name)
  expect(found?.id).toBe(workflow.id)
})
