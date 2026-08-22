// Agent 2 (Phase 5) — end-to-end proof that the HTTP ingress actually resumes
// a workflow: POST /webhooks/:name and POST /signals both durably publish an
// event that wakes a step blocked on `ctx.waitForEvent(...)`, driven through a
// REAL worker. Mirrors the pattern in tests/signals.test.ts (Agent 1), which
// this reuses rather than re-deriving.

import { describe, expect, test, beforeAll, afterAll } from 'bun:test'
import { createDb } from '../src/store/client.ts'
import { migrate } from '../src/store/migrate.ts'
import { defineWorkflow, type WorkflowHandle } from '../src/define/workflow.ts'
import { enqueueRun } from '../src/engine/executor.ts'
import { createWorker, type Worker } from '../src/worker/worker.ts'
import { createTriggerHandler } from '../src/triggers/http.ts'
import { getRun, getStepsWaitingForEvent, type RunRow } from '../src/store/repositories.ts'

const sql = createDb()
const handler = createTriggerHandler({ db: sql })

beforeAll(async () => {
  await migrate(sql)
})

afterAll(async () => {
  await sql.end()
})

function unique(prefix: string): string {
  return `${prefix}-${crypto.randomUUID()}`
}

function req(path: string, init: RequestInit = {}): Request {
  return new Request(`http://localhost${path}`, init)
}

function wait(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

async function until(label: string, predicate: () => Promise<boolean>, timeoutMs = 10_000): Promise<void> {
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    if (await predicate()) return
    await wait(20)
  }
  throw new Error(`timed out waiting for: ${label}`)
}

async function drain(runId: string, workers: Worker[], timeoutMs = 15_000): Promise<RunRow> {
  const deadline = Date.now() + timeoutMs
  let run = await getRun(sql, runId)
  while (
    run &&
    run.status !== 'completed' &&
    run.status !== 'failed' &&
    run.status !== 'cancelled' &&
    Date.now() < deadline
  ) {
    await wait(20)
    run = await getRun(sql, runId)
  }
  await Promise.all(workers.map((w) => w.stop()))
  if (!run) throw new Error(`drain: run "${runId}" vanished`)
  return run
}

function waiterWorkflow(eventName: string, correlationKey?: string): WorkflowHandle {
  return defineWorkflow(unique('http-waiter'), (builder) => {
    builder.step(
      'await-event',
      async (ctx) => {
        const payload = await ctx.waitForEvent<Record<string, unknown>>(eventName, { correlationKey })
        return { received: payload }
      },
      { maxAttempts: 1 }
    )
  })
}

describe('POST /webhooks/:name — webhook ingestion (#25)', () => {
  test('publishes an event that wakes a step blocked on ctx.waitForEvent', async () => {
    const routeName = unique('inbound-hook')
    const handle = waiterWorkflow(routeName)
    const ns = unique('ns')
    const { runId } = await enqueueRun(sql, handle, { input: {}, namespace: ns })

    const worker = createWorker({ db: sql, handles: [handle], namespace: ns })
    worker.start()

    await until('step blocked waiting for the webhook event', async () => {
      const waiting = await getStepsWaitingForEvent(sql, runId)
      return waiting.length === 1
    })

    const res = await handler(
      req(`/webhooks/${routeName}`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ amount: 42 }),
      })
    )
    expect(res.status).toBe(201)
    const body = (await res.json()) as { eventId: string; created: boolean; woken: number }
    expect(body.created).toBe(true)
    expect(body.woken).toBe(1)

    const run = await drain(runId, [worker])
    expect(run.status).toBe('completed')
    expect((run.output as Record<string, unknown>)['await-event']).toEqual({
      received: { amount: 42 },
    })
  }, 20_000)

  test('a redelivered webhook (same Idempotency-Key) does not double-publish', async () => {
    const routeName = unique('inbound-hook-idem')
    const key = unique('delivery')

    const makeReq = () =>
      req(`/webhooks/${routeName}`, {
        method: 'POST',
        headers: { 'content-type': 'application/json', 'idempotency-key': key },
        body: JSON.stringify({ x: 1 }),
      })

    const first = await handler(makeReq())
    expect(first.status).toBe(201)
    const firstBody = (await first.json()) as { eventId: string; created: boolean }
    expect(firstBody.created).toBe(true)

    const second = await handler(makeReq())
    expect(second.status).toBe(200)
    const secondBody = (await second.json()) as { eventId: string; created: boolean }
    expect(secondBody.created).toBe(false)
    expect(secondBody.eventId).toBe(firstBody.eventId)
  })

  test('a body-level `type` field qualifies the event name, matching a correlated waiter', async () => {
    const routeName = unique('provider')
    const eventName = `${routeName}.charge.succeeded`
    const handle = waiterWorkflow(eventName, 'order-99')
    const ns = unique('ns')
    const { runId } = await enqueueRun(sql, handle, { input: {}, namespace: ns })

    const worker = createWorker({ db: sql, handles: [handle], namespace: ns })
    worker.start()

    await until('step blocked', async () => (await getStepsWaitingForEvent(sql, runId)).length === 1)

    const res = await handler(
      req(`/webhooks/${routeName}`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ type: 'charge.succeeded', correlationId: 'order-99', amount: 7 }),
      })
    )
    expect(res.status).toBe(201)
    const body = (await res.json()) as { name: string; woken: number }
    expect(body.name).toBe(eventName)
    expect(body.woken).toBe(1)

    const run = await drain(runId, [worker])
    expect(run.status).toBe('completed')
  }, 20_000)

  test('400 on malformed JSON body', async () => {
    const res = await handler(
      req(`/webhooks/${unique('bad-json')}`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: '{not json',
      })
    )
    expect(res.status).toBe(400)
  })

  test('405 on GET', async () => {
    const res = await handler(req(`/webhooks/${unique('method')}`, { method: 'GET' }))
    expect(res.status).toBe(405)
  })
})

describe('POST /signals — direct signal publish', () => {
  test('wakes a step blocked on ctx.waitForEvent', async () => {
    const eventName = unique('direct.signal')
    const handle = waiterWorkflow(eventName)
    const ns = unique('ns')
    const { runId } = await enqueueRun(sql, handle, { input: {}, namespace: ns })

    const worker = createWorker({ db: sql, handles: [handle], namespace: ns })
    worker.start()

    await until('step blocked', async () => (await getStepsWaitingForEvent(sql, runId)).length === 1)

    const res = await handler(
      req('/signals', {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ name: eventName, payload: { ok: true } }),
      })
    )
    expect(res.status).toBe(201)
    const body = (await res.json()) as { woken: number }
    expect(body.woken).toBe(1)

    const run = await drain(runId, [worker])
    expect(run.status).toBe('completed')
    expect((run.output as Record<string, unknown>)['await-event']).toEqual({ received: { ok: true } })
  }, 20_000)

  test('400 when "name" is missing', async () => {
    const res = await handler(
      req('/signals', {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ payload: {} }),
      })
    )
    expect(res.status).toBe(400)
  })
})
