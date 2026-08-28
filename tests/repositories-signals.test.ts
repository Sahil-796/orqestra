// Phase 5 (#18) storage-layer proof: publishEvent's wake matching (name +
// correlation, broadcast, exactly-once, idempotency), the schedules
// primitives (create / claim / fire / reschedule + the kind/expression
// CHECK), undispatched-event routing, and the start-a-run-by-name wrapper the
// trigger agents build on.

import { describe, expect, test, beforeAll, afterAll } from 'bun:test'
import { createDb } from '../src/store/client.ts'
import { migrate } from '../src/store/migrate.ts'
import {
  claimDueSchedules,
  claimUndispatchedEvents,
  createRun,
  createSchedule,
  findMatchingEventSince,
  getSchedule,
  getStepsByRun,
  getStepsWaitingForEvent,
  getUndispatchedEvents,
  insertSteps,
  insertWorkflow,
  markScheduleFired,
  markStepRunning,
  publishEvent,
  registerStepEventWait,
  rescheduleCron,
  startRunForWorkflowName,
  type NewStep,
  type StepRow,
} from '../src/store/repositories.ts'
import type { WorkflowDefinition } from '../src/types.ts'

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

// Create a run with a single step and park that step in `blocked` waiting for
// `eventName` (+ optional correlation), returning the blocked step row.
async function blockedStepWaitingFor(
  eventName: string,
  correlationKey?: string
): Promise<StepRow> {
  const name = uniqueName('sig-wf')
  const dag: WorkflowDefinition = {
    name,
    version: 1,
    steps: [{ name: 'wait', dependsOn: [], maxAttempts: 1, priority: 0 }],
  }
  const workflow = await insertWorkflow(sql, { name, dag })
  const { run } = await createRun(sql, { workflowId: workflow.id })
  const steps: NewStep[] = [{ name: 'wait', dependsOn: [], maxAttempts: 1, priority: 0, status: 'ready' }]
  const [inserted] = await insertSteps(sql, run.id, steps)
  if (!inserted) throw new Error('setup: no step inserted')
  await markStepRunning(sql, inserted.id)
  const blocked = await registerStepEventWait(sql, { stepId: inserted.id, eventName, correlationKey })
  if (!blocked) throw new Error('setup: registerStepEventWait did not block the step')
  expect(blocked.status).toBe('blocked')
  expect(blocked.waiting_event_name).toBe(eventName)
  return blocked
}

describe('publishEvent / event-wait matching', () => {
  test('wakes a matching blocked step and delivers the payload', async () => {
    const name = uniqueName('order.paid')
    const step = await blockedStepWaitingFor(name)

    const { event, created, woken } = await publishEvent(sql, {
      name,
      payload: { amount: 42 },
    })

    expect(created).toBe(true)
    expect(event.name).toBe(name)
    expect(woken.map((s) => s.id)).toEqual([step.id])

    const woke = woken[0]!
    expect(woke.status).toBe('ready')
    expect(woke.waiting_event_name).toBeNull()
    expect(woke.event_seq).toBe(1)
    expect(woke.event_payloads).toEqual([{ amount: 42 }])
  })

  test('wake is exactly-once — a second matching publish does not re-wake', async () => {
    const name = uniqueName('order.shipped')
    await blockedStepWaitingFor(name)

    const first = await publishEvent(sql, { name, payload: 1 })
    expect(first.woken.length).toBe(1)

    const second = await publishEvent(sql, { name, payload: 2 })
    // The step left `blocked` on the first publish, so the second matches
    // nothing — no double-wake, no second payload appended.
    expect(second.woken.length).toBe(0)

    const woke = first.woken[0]!
    expect(woke.event_seq).toBe(1)
    expect(woke.event_payloads).toEqual([1])
  })

  test('idempotencyKey de-dupes a redelivered publish (no second wake)', async () => {
    const name = uniqueName('order.refunded')
    await blockedStepWaitingFor(name)
    const key = `evt-${crypto.randomUUID()}`

    const first = await publishEvent(sql, { name, payload: 'x', idempotencyKey: key })
    expect(first.created).toBe(true)
    expect(first.woken.length).toBe(1)

    // A fresh waiter for the same name, then a REDELIVERY (same key): it must
    // neither record a new event nor wake the new waiter.
    const second = await blockedStepWaitingFor(name)
    const redelivered = await publishEvent(sql, { name, payload: 'x', idempotencyKey: key })
    expect(redelivered.created).toBe(false)
    expect(redelivered.woken.length).toBe(0)
    expect(redelivered.event.id).toBe(first.event.id)

    const stillWaiting = await getStepsWaitingForEvent(sql, second.run_id)
    expect(stillWaiting.map((s) => s.id)).toContain(second.id)
  })

  test('correlation: a correlated event wakes the matching + the broad waiter, not a different correlation', async () => {
    const name = uniqueName('payment.confirmed')
    const forA = await blockedStepWaitingFor(name, 'order-A')
    const forB = await blockedStepWaitingFor(name, 'order-B')
    const broad = await blockedStepWaitingFor(name) // no correlation — any 'name'

    const { woken } = await publishEvent(sql, { name, correlationKey: 'order-A', payload: { ok: true } })
    const wokenIds = woken.map((s) => s.id).sort()
    expect(wokenIds).toEqual([forA.id, broad.id].sort())
    expect(wokenIds).not.toContain(forB.id)
  })

  test('a correlated waiter is not woken by an uncorrelated event', async () => {
    const name = uniqueName('invoice.settled')
    const forA = await blockedStepWaitingFor(name, 'acct-1')

    const { woken } = await publishEvent(sql, { name, payload: null })
    expect(woken.map((s) => s.id)).not.toContain(forA.id)
  })

  test('findMatchingEventSince backstops only events at/after the given instant', async () => {
    const name = uniqueName('webhook.received')
    await publishEvent(sql, { name, payload: 'old' })
    // A floor comfortably in the future finds nothing…
    const none = await findMatchingEventSince(sql, { name, since: new Date(Date.now() + 60_000) })
    expect(none).toBeUndefined()
    // …but a floor comfortably in the past finds the event (past/future used
    // to sidestep JS-vs-DB clock skew; the worker path uses a DB timestamp).
    const found = await findMatchingEventSince(sql, { name, since: new Date(Date.now() - 60_000) })
    expect(found?.name).toBe(name)
  })
})

describe('schedules', () => {
  test('createSchedule + claimDueSchedules claims a due row and pushes it out of the window', async () => {
    const wf = uniqueName('sched-once')
    const past = new Date(Date.now() - 60_000)
    const created = await createSchedule(sql, {
      workflowName: wf,
      kind: 'once',
      nextRunAt: past,
      input: { hello: 'world' },
    })
    expect(created.enabled).toBe(true)

    // High limit on purpose: the whole suite shares one Postgres, so other
    // schedule tests' due rows accumulate here. A small limit could fill the
    // claim batch with their rows and crowd this one out — a false negative,
    // not a real claim failure. Claim wide so this row is always in the batch.
    const claimed = await claimDueSchedules(sql, new Date(), 100_000, 60_000)
    const mine = claimed.find((s) => s.id === created.id)
    expect(mine).toBeDefined()
    // The claim bumped next_run_at forward so a second poller won't re-claim.
    expect(mine!.next_run_at.getTime()).toBeGreaterThan(Date.now())

    // A second immediate claim no longer sees it.
    const again = await claimDueSchedules(sql, new Date(), 100_000, 60_000)
    expect(again.find((s) => s.id === created.id)).toBeUndefined()
  })

  test("markScheduleFired disables a 'once' schedule", async () => {
    const wf = uniqueName('sched-once-fire')
    const created = await createSchedule(sql, {
      workflowName: wf,
      kind: 'once',
      nextRunAt: new Date(Date.now() - 1000),
    })
    const fired = await markScheduleFired(sql, created.id, new Date())
    expect(fired?.enabled).toBe(false)
    expect(fired?.last_fired_at).not.toBeNull()
  })

  test('rescheduleCron advances a cron schedule and keeps it enabled', async () => {
    const wf = uniqueName('sched-cron')
    const created = await createSchedule(sql, {
      workflowName: wf,
      kind: 'cron',
      cronExpression: '*/5 * * * *',
      nextRunAt: new Date(Date.now() - 1000),
    })
    const next = new Date(Date.now() + 5 * 60_000)
    const rescheduled = await rescheduleCron(sql, created.id, next, new Date())
    expect(rescheduled?.enabled).toBe(true)
    expect(rescheduled?.next_run_at.getTime()).toBe(next.getTime())

    const reread = await getSchedule(sql, created.id)
    expect(reread?.last_fired_at).not.toBeNull()
  })

  test('the kind/cron_expression CHECK rejects incoherent schedules', async () => {
    await expect(
      createSchedule(sql, { workflowName: uniqueName('bad'), kind: 'cron', nextRunAt: new Date() })
    ).rejects.toThrow()
    await expect(
      createSchedule(sql, {
        workflowName: uniqueName('bad'),
        kind: 'once',
        cronExpression: '* * * * *',
        nextRunAt: new Date(),
      })
    ).rejects.toThrow()
  })
})

describe('undispatched events (trigger routing)', () => {
  test('claimUndispatchedEvents stamps dispatched_at exactly once', async () => {
    const name = uniqueName('trigger.me')
    const { event } = await publishEvent(sql, { name, source: 'webhook', payload: { n: 1 } })

    const before = await getUndispatchedEvents(sql, 500)
    expect(before.map((e) => e.id)).toContain(event.id)

    const claimed = await claimUndispatchedEvents(sql, 500)
    expect(claimed.map((e) => e.id)).toContain(event.id)

    // Once claimed it is no longer undispatched, so a second claim can't
    // re-route it.
    const after = await claimUndispatchedEvents(sql, 500)
    expect(after.map((e) => e.id)).not.toContain(event.id)
  })
})

describe('startRunForWorkflowName', () => {
  test('materializes a run + steps from the stored dag, idempotently', async () => {
    const name = uniqueName('by-name-wf')
    const dag: WorkflowDefinition = {
      name,
      version: 1,
      steps: [
        { name: 'first', dependsOn: [], maxAttempts: 2, priority: 0 },
        { name: 'second', dependsOn: ['first'], maxAttempts: 1, priority: 0 },
      ],
    }
    await insertWorkflow(sql, { name, dag })

    const key = `by-name-${crypto.randomUUID()}`
    const started = await startRunForWorkflowName(sql, {
      workflowName: name,
      input: { go: true },
      idempotencyKey: key,
    })
    expect(started.created).toBe(true)

    const steps = await getStepsByRun(sql, started.runId)
    expect(steps.map((s) => s.name).sort()).toEqual(['first', 'second'])
    // no-deps step ready, dependent step pending
    expect(steps.find((s) => s.name === 'first')?.status).toBe('ready')
    expect(steps.find((s) => s.name === 'second')?.status).toBe('pending')

    // Same idempotency key → same run, no re-materialization.
    const again = await startRunForWorkflowName(sql, {
      workflowName: name,
      input: { go: true },
      idempotencyKey: key,
    })
    expect(again.created).toBe(false)
    expect(again.runId).toBe(started.runId)
  })

  test('throws for an unregistered workflow name', async () => {
    await expect(
      startRunForWorkflowName(sql, { workflowName: uniqueName('does-not-exist') })
    ).rejects.toThrow()
  })
})
