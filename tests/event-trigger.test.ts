// DB-backed proof for events.ts's pollUndispatchedEvents (#22 event
// triggers): publishing an event a workflow declares a `{type:'event'}`
// trigger for starts a run of it, correlation narrows which events count,
// and re-dispatching an already-claimed event never double-starts.
//
// Uses defineWorkflow (the real registration path, define/workflow.ts) so
// the WorkflowHandle passed to pollUndispatchedEvents is exactly the shape
// the trigger daemon operates on in production, not a hand-rolled stand-in.

import { describe, expect, test, beforeAll, afterAll } from 'bun:test'
import { createDb } from '../src/store/client.ts'
import { migrate } from '../src/store/migrate.ts'
import { getIncompleteRuns, getWorkflowByName, publishEvent } from '../src/store/repositories.ts'
import { defineWorkflow } from '../src/define/workflow.ts'
import { pollUndispatchedEvents, eventTriggerIdempotencyKey } from '../src/triggers/events.ts'

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

describe('pollUndispatchedEvents', () => {
  test('publishing an event a workflow subscribes to starts exactly one run', async () => {
    const eventName = uniqueName('order.placed')
    const workflowName = uniqueName('wf-event-basic')

    const handle = defineWorkflow(
      workflowName,
      (b) => {
        b.step('only', async () => 'ok')
      },
      { triggers: [{ type: 'event', event: eventName }] }
    )
    const workflowRow = await handle.register(sql)

    const { event } = await publishEvent(sql, { name: eventName, payload: { orderId: 1 } })

    const result = await pollUndispatchedEvents(sql, [handle], { limit: 200 })
    expect(result.errors.find((e) => e.event.id === event.id)).toBeUndefined()

    const runs = await runsForWorkflow(workflowRow.id)
    expect(runs.length).toBe(1)
    expect(runs[0]?.input).toEqual({ orderId: 1 })
  })

  test('re-dispatching an already-claimed event never double-starts the run (idempotency)', async () => {
    const eventName = uniqueName('order.placed.redispatch')
    const workflowName = uniqueName('wf-event-redispatch')

    const handle = defineWorkflow(
      workflowName,
      (b) => {
        b.step('only', async () => 'ok')
      },
      { triggers: [{ type: 'event', event: eventName }] }
    )
    const workflowRow = await handle.register(sql)

    const { event } = await publishEvent(sql, { name: eventName, payload: { n: 1 } })

    // First tick claims and routes it.
    const first = await pollUndispatchedEvents(sql, [handle], { limit: 200 })
    expect(first.started).toBeGreaterThanOrEqual(1)

    // Simulate a re-dispatch of the same already-claimed event by routing it
    // again directly through the same idempotency key startRunForWorkflowName
    // would derive — claimUndispatchedEvents itself won't hand it back (it's
    // already dispatched), so exercise the idempotency guard the way a crash-
    // and-retry of the routing step would: call startRunForWorkflowName again
    // with the identical key events.ts computes.
    const { startRunForWorkflowName } = await import('../src/store/repositories.ts')
    const key = eventTriggerIdempotencyKey(event.id, workflowName)
    const redelivered = await startRunForWorkflowName(sql, {
      workflowName,
      input: event.payload,
      idempotencyKey: key,
    })
    expect(redelivered.created).toBe(false)

    const runs = await runsForWorkflow(workflowRow.id)
    expect(runs.length).toBe(1)
  })

  test('a claimed event is never re-routed by a later poll tick', async () => {
    const eventName = uniqueName('order.placed.once-claimed')
    const workflowName = uniqueName('wf-event-once-claimed')

    const handle = defineWorkflow(
      workflowName,
      (b) => {
        b.step('only', async () => 'ok')
      },
      { triggers: [{ type: 'event', event: eventName }] }
    )
    const workflowRow = await handle.register(sql)

    await publishEvent(sql, { name: eventName, payload: { n: 1 } })

    await pollUndispatchedEvents(sql, [handle], { limit: 200 })
    await pollUndispatchedEvents(sql, [handle], { limit: 200 }) // second tick: nothing left to claim

    const runs = await runsForWorkflow(workflowRow.id)
    expect(runs.length).toBe(1)
  })

  test('correlationKey narrows which events start a run', async () => {
    const eventName = uniqueName('payment.received')
    const workflowName = uniqueName('wf-event-correlated')

    const handle = defineWorkflow(
      workflowName,
      (b) => {
        b.step('only', async () => 'ok')
      },
      { triggers: [{ type: 'event', event: eventName, correlationKey: 'acct-123' }] }
    )
    const workflowRow = await handle.register(sql)

    // Non-matching correlation: should not start a run.
    await publishEvent(sql, { name: eventName, correlationKey: 'acct-999', payload: {} })
    await pollUndispatchedEvents(sql, [handle], { limit: 200 })
    expect((await runsForWorkflow(workflowRow.id)).length).toBe(0)

    // Matching correlation: should start a run.
    await publishEvent(sql, { name: eventName, correlationKey: 'acct-123', payload: {} })
    await pollUndispatchedEvents(sql, [handle], { limit: 200 })
    expect((await runsForWorkflow(workflowRow.id)).length).toBe(1)
  })

  test('a workflow with no matching trigger is left alone', async () => {
    const eventName = uniqueName('unrelated.event')
    const workflowName = uniqueName('wf-event-unrelated')

    const handle = defineWorkflow(
      workflowName,
      (b) => {
        b.step('only', async () => 'ok')
      },
      { triggers: [{ type: 'event', event: uniqueName('some.other.event') }] }
    )
    const workflowRow = await handle.register(sql)

    await publishEvent(sql, { name: eventName, payload: {} })
    await pollUndispatchedEvents(sql, [handle], { limit: 200 })

    expect((await runsForWorkflow(workflowRow.id)).length).toBe(0)
  })
})

test('getWorkflowByName resolves the same row register() returned', async () => {
  const workflowName = uniqueName('wf-event-sanity')
  const handle = defineWorkflow(workflowName, (b) => {
    b.step('only', async () => 'ok')
  })
  const registered = await handle.register(sql)
  const found = await getWorkflowByName(sql, workflowName)
  expect(found?.id).toBe(registered.id)
})
