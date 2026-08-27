// Agent 2 (Phase 5) — API trigger (#21) + delayed start (#24) coverage.
// Exercises the HTTP handler directly (no socket bound — `createTriggerHandler`
// returns a plain (Request) => Promise<Response> function) against a real,
// migrated Postgres, the same pattern tests/signals.test.ts uses for the
// storage layer it depends on.

import { describe, expect, test, beforeAll, afterAll } from 'bun:test'
import { createDb } from '../src/store/client.ts'
import { migrate } from '../src/store/migrate.ts'
import { defineWorkflow } from '../src/define/workflow.ts'
import { createTriggerHandler } from '../src/triggers/http.ts'
import { getRun, getSchedule, getStepsByRun } from '../src/store/repositories.ts'

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

async function registerSimpleWorkflow(name: string) {
  const handle = defineWorkflow(name, (builder) => {
    builder.step('only-step', async (ctx) => ({ input: ctx.input }))
  })
  await handle.register(sql)
  return handle
}

describe('GET /healthz', () => {
  test('reports ok', async () => {
    const res = await handler(req('/healthz'))
    expect(res.status).toBe(200)
    expect(await res.json()).toEqual({ ok: true })
  })

  test('405 on wrong method', async () => {
    const res = await handler(req('/healthz', { method: 'POST' }))
    expect(res.status).toBe(405)
  })
})

describe('unknown routes', () => {
  test('404', async () => {
    const res = await handler(req('/nope'))
    expect(res.status).toBe(404)
  })
})

describe('POST /workflows/:name/runs — API trigger (#21)', () => {
  test('starts a run for a registered workflow', async () => {
    const name = unique('http-trigger-wf')
    await registerSimpleWorkflow(name)
    const ns = unique('ns')

    const res = await handler(
      req(`/workflows/${name}/runs`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ input: { hello: 'world' }, namespace: ns }),
      })
    )
    expect(res.status).toBe(201)
    const body = (await res.json()) as { runId: string; created: boolean }
    expect(body.created).toBe(true)
    expect(typeof body.runId).toBe('string')

    const run = await getRun(sql, body.runId)
    expect(run).toBeDefined()
    expect(run?.namespace).toBe(ns)
    expect(run?.input).toEqual({ hello: 'world' })

    const steps = await getStepsByRun(sql, body.runId)
    expect(steps.length).toBe(1)
  })

  test('is idempotent on repeated Idempotency-Key header', async () => {
    const name = unique('http-trigger-idem')
    await registerSimpleWorkflow(name)
    const ns = unique('ns')
    const key = unique('idem')

    const makeReq = () =>
      req(`/workflows/${name}/runs`, {
        method: 'POST',
        headers: { 'content-type': 'application/json', 'idempotency-key': key },
        body: JSON.stringify({ input: {}, namespace: ns }),
      })

    const first = await handler(makeReq())
    expect(first.status).toBe(201)
    const firstBody = (await first.json()) as { runId: string; created: boolean }
    expect(firstBody.created).toBe(true)

    const second = await handler(makeReq())
    expect(second.status).toBe(200)
    const secondBody = (await second.json()) as { runId: string; created: boolean }
    expect(secondBody.created).toBe(false)
    expect(secondBody.runId).toBe(firstBody.runId)
  })

  test('POST /runs alias reads the workflow name from the body', async () => {
    const name = unique('http-trigger-alias')
    await registerSimpleWorkflow(name)
    const ns = unique('ns')

    const res = await handler(
      req('/runs', {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ workflowName: name, input: {}, namespace: ns }),
      })
    )
    expect(res.status).toBe(201)
    const body = (await res.json()) as { runId: string }
    const run = await getRun(sql, body.runId)
    expect(run).toBeDefined()
  })

  test('404s on unregistered workflow name -> 400', async () => {
    const res = await handler(
      req(`/workflows/${unique('does-not-exist')}/runs`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({}),
      })
    )
    expect(res.status).toBe(400)
  })

  test('400 on malformed JSON body', async () => {
    const name = unique('http-trigger-badjson')
    await registerSimpleWorkflow(name)
    const res = await handler(
      req(`/workflows/${name}/runs`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: '{not json',
      })
    )
    expect(res.status).toBe(400)
  })

  test('405 on GET', async () => {
    const name = unique('http-trigger-method')
    const res = await handler(req(`/workflows/${name}/runs`, { method: 'GET' }))
    expect(res.status).toBe(405)
  })
})

describe('delayed start (#24) via runAt / delayMs', () => {
  test('a future runAt creates a `once` schedule instead of starting a run', async () => {
    const name = unique('http-delayed-runat')
    await registerSimpleWorkflow(name)
    const ns = unique('ns')
    const runAt = new Date(Date.now() + 60_000).toISOString()

    const res = await handler(
      req(`/workflows/${name}/runs`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ input: { a: 1 }, namespace: ns, runAt }),
      })
    )
    expect(res.status).toBe(202)
    const body = (await res.json()) as { scheduleId: string; kind: string }
    expect(body.kind).toBe('once')

    const schedule = await getSchedule(sql, body.scheduleId)
    expect(schedule).toBeDefined()
    expect(schedule?.workflow_name).toBe(name)
    expect(schedule?.enabled).toBe(true)
    expect(schedule?.namespace).toBe(ns)
  })

  test('delayMs also creates a schedule, roughly delayMs from now', async () => {
    const name = unique('http-delayed-delayms')
    await registerSimpleWorkflow(name)
    const before = Date.now()

    const res = await handler(
      req(`/workflows/${name}/runs`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ delayMs: 30_000 }),
      })
    )
    expect(res.status).toBe(202)
    const body = (await res.json()) as { scheduleId: string }
    const schedule = await getSchedule(sql, body.scheduleId)
    expect(schedule).toBeDefined()
    const delta = new Date(schedule!.next_run_at).getTime() - before
    expect(delta).toBeGreaterThan(20_000)
    expect(delta).toBeLessThan(40_000)
  })

  test('a malformed runAt is a 400', async () => {
    const name = unique('http-delayed-bad')
    await registerSimpleWorkflow(name)
    const res = await handler(
      req(`/workflows/${name}/runs`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ runAt: 'not-a-date' }),
      })
    )
    expect(res.status).toBe(400)
  })
})
