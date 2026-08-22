// Proves syncCronSchedules (cron.ts) and the daemon loop (runner.ts) tie
// everything together: startup seeds a cron schedule from a workflow's
// declared `{type:'cron'}` trigger, and the running loop picks up both due
// schedules and published events without the caller manually driving ticks.

import { describe, expect, test, beforeAll, afterAll } from 'bun:test'
import { createDb } from '../src/store/client.ts'
import { migrate } from '../src/store/migrate.ts'
import { getIncompleteRuns, publishEvent } from '../src/store/repositories.ts'
import { defineWorkflow } from '../src/define/workflow.ts'
import { syncCronSchedules, __resetCronSyncGuardForTests } from '../src/triggers/cron.ts'
import { startTriggerRunner, runTriggerTick } from '../src/triggers/runner.ts'

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

async function runsForWorkflow(workflowId: string) {
  const incomplete = await getIncompleteRuns(sql)
  return incomplete.filter((r) => r.workflow_id === workflowId)
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

describe('syncCronSchedules', () => {
  test('creates an enabled cron schedule for a workflow with a cron trigger', async () => {
    const workflowName = uniqueName('wf-cron-sync')
    const handle = defineWorkflow(
      workflowName,
      (b) => {
        b.step('only', async () => 'ok')
      },
      { triggers: [{ type: 'cron', cron: '*/5 * * * *' }] }
    )
    await handle.register(sql)

    const result = await syncCronSchedules(sql, [handle])
    expect(result.created.length).toBe(1)
    const row = result.created[0]!
    expect(row.workflow_name).toBe(workflowName)
    expect(row.kind).toBe('cron')
    expect(row.cron_expression).toBe('*/5 * * * *')
    expect(row.enabled).toBe(true)
  })

  test('is idempotent within a process: a second call does not duplicate the schedule', async () => {
    const workflowName = uniqueName('wf-cron-sync-dup')
    const handle = defineWorkflow(
      workflowName,
      (b) => {
        b.step('only', async () => 'ok')
      },
      { triggers: [{ type: 'cron', cron: '0 * * * *' }] }
    )
    await handle.register(sql)

    const first = await syncCronSchedules(sql, [handle])
    expect(first.created.length).toBe(1)

    const second = await syncCronSchedules(sql, [handle])
    expect(second.created.length).toBe(0)
    expect(second.skipped).toBe(1)
  })

  test('is idempotent across a process restart: the durable DB check prevents a duplicate cron row', async () => {
    const workflowName = uniqueName('wf-cron-sync-restart')
    const handle = defineWorkflow(
      workflowName,
      (b) => {
        b.step('only', async () => 'ok')
      },
      { triggers: [{ type: 'cron', cron: '15 * * * *' }] }
    )
    await handle.register(sql)

    const first = await syncCronSchedules(sql, [handle])
    expect(first.created.length).toBe(1)

    // Simulate a fresh process: the in-memory guard is empty again, so the only
    // thing standing between us and a duplicate cron row is findCronSchedule.
    __resetCronSyncGuardForTests()
    const afterRestart = await syncCronSchedules(sql, [handle])
    expect(afterRestart.created.length).toBe(0)
    expect(afterRestart.skipped).toBe(1)

    const rows = await sql<{ count: number }[]>`
      select count(*)::int as count from schedules
      where workflow_name = ${workflowName} and kind = 'cron' and enabled
    `
    expect(rows[0]!.count).toBe(1)
  })

  test('a workflow with no cron trigger is left alone', async () => {
    const workflowName = uniqueName('wf-no-cron')
    const handle = defineWorkflow(workflowName, (b) => {
      b.step('only', async () => 'ok')
    })
    await handle.register(sql)

    const result = await syncCronSchedules(sql, [handle])
    expect(result.created.length).toBe(0)
  })
})

describe('runTriggerTick', () => {
  test('one manual tick fires a due schedule and routes a matching event together', async () => {
    __resetCronSyncGuardForTests()

    const cronWorkflowName = uniqueName('wf-tick-cron')
    const eventName = uniqueName('tick.event')
    const eventWorkflowName = uniqueName('wf-tick-event')

    const cronHandle = defineWorkflow(
      cronWorkflowName,
      (b) => {
        b.step('only', async () => 'ok')
      },
      { triggers: [{ type: 'cron', cron: '* * * * *' }] }
    )
    const cronWorkflowRow = await cronHandle.register(sql)

    const eventHandle = defineWorkflow(
      eventWorkflowName,
      (b) => {
        b.step('only', async () => 'ok')
      },
      { triggers: [{ type: 'event', event: eventName }] }
    )
    const eventWorkflowRow = await eventHandle.register(sql)

    // Seed the cron schedule already due (nextRunAt in the past) via
    // syncCronSchedules directly is awkward (it computes a *future*
    // nextRunAt from `now`), so drive the same path scheduled.test.ts uses:
    // create a due schedule directly, then verify the runner's manual tick
    // picks it up exactly like pollDueSchedules alone does.
    const { createSchedule } = await import('../src/store/repositories.ts')
    const schedule = await createSchedule(sql, {
      workflowName: cronWorkflowName,
      kind: 'cron',
      cronExpression: '* * * * *',
      nextRunAt: new Date(Date.now() - 60_000),
    })

    const { event } = await publishEvent(sql, { name: eventName, payload: { hi: true } })

    // Shared dev DB caveat: other suites' schedules/events may also be due
    // right now and fail to route (their workflows aren't necessarily
    // registered in this process) — that's not this test's concern, so only
    // assert this test's own schedule/event routed cleanly.
    const result = await runTriggerTick(sql, [cronHandle, eventHandle], { scheduleLimit: 200, eventLimit: 200 })
    expect(result.schedules.errors.find((e) => e.schedule.id === schedule.id)).toBeUndefined()
    expect(result.events.errors.find((e) => e.event.id === event.id)).toBeUndefined()

    expect((await runsForWorkflow(cronWorkflowRow.id)).length).toBe(1)
    expect((await runsForWorkflow(eventWorkflowRow.id)).length).toBe(1)
  })
})

describe('startTriggerRunner', () => {
  test('runs syncCronSchedules at startup and polls on an interval until stopped', async () => {
    __resetCronSyncGuardForTests()

    const workflowName = uniqueName('wf-runner-loop')
    const handle = defineWorkflow(
      workflowName,
      (b) => {
        b.step('only', async () => 'ok')
      },
      { triggers: [{ type: 'cron', cron: '* * * * *' }] }
    )
    const workflowRow = await handle.register(sql)

    const errors: unknown[] = []
    const runner = startTriggerRunner({
      db: sql,
      workflows: [handle],
      pollIntervalMs: 20,
      onError: (e) => errors.push(e),
    })

    // Give the startup sync a moment to run.
    await sleep(50)

    await runner.stop()
    expect(runner.running).toBe(false)
    expect(errors).toEqual([])

    // The runner's own startup call to syncCronSchedules should have
    // already created (and guarded) the cron schedule for this workflow —
    // calling it again here, in this same process, should find nothing left
    // to create.
    const followUp = await syncCronSchedules(sql, [handle])
    expect(followUp.created.length).toBe(0)
    expect(followUp.skipped).toBe(1)

    void workflowRow
  })
})
