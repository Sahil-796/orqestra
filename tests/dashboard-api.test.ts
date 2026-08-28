// Phase 8 dashboard read API (#30-#35) + operator retry (#27) / cancel (#11)
// controls, exercised through the composed HTTP handler
// (src/server.ts's createServerHandler) against a real, migrated Postgres —
// same style as tests/manual-retry.test.ts.

import { describe, expect, test, beforeAll, afterAll } from 'bun:test'
import { createDb } from '../src/store/client.ts'
import { migrate } from '../src/store/migrate.ts'
import { defineWorkflow } from '../src/define/workflow.ts'
import { enqueueRun } from '../src/engine/executor.ts'
import { createServerHandler } from '../src/server.ts'
import {
  cancelPendingSteps,
  deadLetterRun,
  failStep,
  getRun,
  getStepsByRun,
  insertHistory,
  updateRunStatus,
  type RunRow,
} from '../src/store/repositories.ts'
import { serializeError } from '../src/types.ts'
import type { WorkflowHandle } from '../src/define/workflow.ts'

const sql = createDb()
const handler = createServerHandler({ db: sql })

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

async function registerWorkflow(): Promise<WorkflowHandle> {
  const name = unique('dash-wf')
  const handle = defineWorkflow(name, (builder) => {
    builder.step('only-step', async (ctx) => ({ echoed: ctx.input }))
  })
  await handle.register(sql)
  return handle
}

async function seedQueuedRun(handle: WorkflowHandle): Promise<{ run: RunRow; namespace: string }> {
  const namespace = unique('ns')
  const { runId } = await enqueueRun(sql, handle, { namespace })
  const run = await getRun(sql, runId)
  if (!run) throw new Error('seedQueuedRun: run vanished')
  return { run, namespace }
}

// Same recipe as manual-retry.test.ts's seedDeadLetteredRun: fail the only
// step, cancel downstream pending steps, mark the run failed, then park it.
async function seedDeadLetteredRun(
  handle: WorkflowHandle,
  reason = 'retries exhausted'
): Promise<{ run: RunRow; namespace: string }> {
  const namespace = unique('ns')
  const { runId } = await enqueueRun(sql, handle, { namespace })
  const steps = await getStepsByRun(sql, runId)
  const step = steps[0]
  if (!step) throw new Error('seedDeadLetteredRun: no step materialized')

  await failStep(sql, step.id, serializeError(new Error('boom')))
  await cancelPendingSteps(sql, runId)
  await updateRunStatus(sql, runId, 'failed')
  const parked = await deadLetterRun(sql, runId, reason)
  expect(parked?.status).toBe('dead_letter')
  const run = await getRun(sql, runId)
  if (!run) throw new Error('seedDeadLetteredRun: run vanished')
  return { run, namespace }
}

describe('GET /dashboard/api/runs', () => {
  test('lists runs and filters by status', async () => {
    const handle = await registerWorkflow()
    const { run: queuedRun } = await seedQueuedRun(handle)
    const { run: deadRun } = await seedDeadLetteredRun(handle)

    const all = await handler(req('/dashboard/api/runs?limit=500'))
    expect(all.status).toBe(200)
    const allBody = (await all.json()) as { runs: { id: string; status: string }[]; count: number }
    expect(allBody.runs.some((r) => r.id === queuedRun.id)).toBe(true)
    expect(allBody.runs.some((r) => r.id === deadRun.id)).toBe(true)

    const filtered = await handler(req('/dashboard/api/runs?status=dead_letter&limit=500'))
    expect(filtered.status).toBe(200)
    const filteredBody = (await filtered.json()) as { runs: { id: string; status: string }[] }
    expect(filteredBody.runs.every((r) => r.status === 'dead_letter')).toBe(true)
    expect(filteredBody.runs.some((r) => r.id === deadRun.id)).toBe(true)
    expect(filteredBody.runs.some((r) => r.id === queuedRun.id)).toBe(false)
  })

  test('rejects an invalid status with 400', async () => {
    const res = await handler(req('/dashboard/api/runs?status=not-a-status'))
    expect(res.status).toBe(400)
  })

  test('rejects a non-positive limit with 400', async () => {
    const res = await handler(req('/dashboard/api/runs?limit=0'))
    expect(res.status).toBe(400)
  })

  test('405 on wrong method', async () => {
    const res = await handler(req('/dashboard/api/runs', { method: 'POST' }))
    expect(res.status).toBe(405)
  })
})

describe('GET /dashboard/api/runs/:id', () => {
  test('200 with run, steps, timeline, and errors', async () => {
    const handle = await registerWorkflow()
    const { run } = await seedDeadLetteredRun(handle, 'boom happened')

    const res = await handler(req(`/dashboard/api/runs/${run.id}`))
    expect(res.status).toBe(200)
    const body = (await res.json()) as {
      run: { id: string }
      steps: { id: string }[]
      timeline: unknown[]
      errors: { stepName: string; error: { name: string; message: string } | null }[]
    }
    expect(body.run.id).toBe(run.id)
    expect(body.steps.length).toBeGreaterThan(0)
    expect(body.timeline.length).toBeGreaterThan(0)
    expect(body.errors.length).toBeGreaterThan(0)
    expect(body.errors[0]?.error?.message).toBe('boom')
  })

  test('404 for an unknown run id', async () => {
    const res = await handler(req(`/dashboard/api/runs/${crypto.randomUUID()}`))
    expect(res.status).toBe(404)
  })

  test('400 for a malformed id segment', async () => {
    const res = await handler(req('/dashboard/api/runs/%'))
    expect(res.status).toBe(400)
  })
})

describe('GET /dashboard/api/runs/:id/logs', () => {
  test('returns log-typed history rows for the run', async () => {
    const handle = await registerWorkflow()
    const { run } = await seedQueuedRun(handle)
    await insertHistory(sql, { runId: run.id, type: 'log', data: { message: 'hello' } })
    await insertHistory(sql, { runId: run.id, type: 'not-a-log', data: { message: 'ignore me' } })

    const res = await handler(req(`/dashboard/api/runs/${run.id}/logs`))
    expect(res.status).toBe(200)
    const body = (await res.json()) as { runId: string; logs: { type: string; data: unknown }[] }
    expect(body.runId).toBe(run.id)
    expect(body.logs.length).toBe(1)
    expect(body.logs[0]?.type).toBe('log')
  })

  test('404 for an unknown run id', async () => {
    const res = await handler(req(`/dashboard/api/runs/${crypto.randomUUID()}/logs`))
    expect(res.status).toBe(404)
  })
})

describe('GET /dashboard/api/metrics', () => {
  test('returns an aggregate metrics row', async () => {
    const handle = await registerWorkflow()
    await seedDeadLetteredRun(handle)

    const res = await handler(req('/dashboard/api/metrics'))
    expect(res.status).toBe(200)
    const body = (await res.json()) as { metrics: { workflowName: string | null; runCount: number }[] }
    expect(body.metrics.length).toBeGreaterThan(0)
  })

  test('405 on wrong method', async () => {
    const res = await handler(req('/dashboard/api/metrics', { method: 'DELETE' }))
    expect(res.status).toBe(405)
  })
})

describe('GET /dashboard/api/queue', () => {
  test('returns depth and throughput', async () => {
    const res = await handler(req('/dashboard/api/queue'))
    expect(res.status).toBe(200)
    const body = (await res.json()) as { depth: Record<string, number>; throughput: unknown[] }
    expect(typeof body.depth).toBe('object')
    expect(Array.isArray(body.throughput)).toBe(true)
  })
})

describe('GET /dashboard/api/workers', () => {
  test('returns a workers array (possibly empty)', async () => {
    const res = await handler(req('/dashboard/api/workers'))
    expect(res.status).toBe(200)
    const body = (await res.json()) as { workers: unknown[] }
    expect(Array.isArray(body.workers)).toBe(true)
  })
})

describe('POST /dashboard/api/runs/:id/retry', () => {
  test('retries a dead-lettered run', async () => {
    const handle = await registerWorkflow()
    const { run } = await seedDeadLetteredRun(handle)

    const res = await handler(req(`/dashboard/api/runs/${run.id}/retry`, { method: 'POST' }))
    expect(res.status).toBe(200)
    const body = (await res.json()) as { runId: string; status: string; retried: boolean }
    expect(body.retried).toBe(true)
    expect(body.runId).toBe(run.id)
    expect(body.status).toBe('queued')
    expect((await getRun(sql, run.id))?.status).toBe('queued')
  })

  test('404 when the run is not dead-lettered', async () => {
    const handle = await registerWorkflow()
    const { run } = await seedQueuedRun(handle)

    const res = await handler(req(`/dashboard/api/runs/${run.id}/retry`, { method: 'POST' }))
    expect(res.status).toBe(404)
  })

  test('405 on wrong method', async () => {
    const res = await handler(req(`/dashboard/api/runs/${crypto.randomUUID()}/retry`, { method: 'GET' }))
    expect(res.status).toBe(405)
  })
})

describe('POST /dashboard/api/runs/:id/cancel', () => {
  test('cancels a run with no step currently running', async () => {
    const handle = await registerWorkflow()
    const { run } = await seedQueuedRun(handle)

    const res = await handler(req(`/dashboard/api/runs/${run.id}/cancel`, { method: 'POST' }))
    expect(res.status).toBe(200)
    const body = (await res.json()) as { runId: string; cancelled: boolean; status: string }
    expect(body.runId).toBe(run.id)
    expect(body.cancelled).toBe(true)
    expect(body.status).toBe('cancelled')
    expect((await getRun(sql, run.id))?.status).toBe('cancelled')
  })

  test('404 for an unknown run id', async () => {
    const res = await handler(req(`/dashboard/api/runs/${crypto.randomUUID()}/cancel`, { method: 'POST' }))
    expect(res.status).toBe(404)
  })

  test('405 on wrong method', async () => {
    const res = await handler(req(`/dashboard/api/runs/${crypto.randomUUID()}/cancel`, { method: 'GET' }))
    expect(res.status).toBe(405)
  })
})

describe('existing routes still reachable through the composed handler', () => {
  test('GET /healthz falls through to the trigger handler', async () => {
    const res = await handler(req('/healthz'))
    expect(res.status).toBe(200)
    expect(await res.json()).toEqual({ ok: true })
  })

  test('GET /dead-letter still works', async () => {
    const res = await handler(req('/dead-letter'))
    expect(res.status).toBe(200)
  })
})
