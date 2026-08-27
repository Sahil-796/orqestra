// Feature #27 — operator manual retry. Covers both the control surface
// (src/control/retry.ts) and the operator HTTP routes wired into
// src/server.ts, against a real, migrated Postgres.
//
// The load-bearing proof is re-claimability: a genuinely dead-lettered run,
// once retried, must be reset into exactly the shape a worker's normal claim
// loop picks up — so we don't just assert the row flipped to `queued`, we
// stand up a real worker on the run's namespace and drain it to `completed`,
// asserting the revived step actually re-ran.
//
// Same isolation discipline as the other store-backed tests: every run gets
// its own random namespace + workflow name so rows never collide with
// leftovers in the shared dev database.

import { describe, expect, test, beforeAll, afterAll } from 'bun:test'
import { createDb } from '../src/store/client.ts'
import { migrate } from '../src/store/migrate.ts'
import { defineWorkflow } from '../src/define/workflow.ts'
import { enqueueRun } from '../src/engine/executor.ts'
import { createWorker, type Worker } from '../src/worker/worker.ts'
import { createServerHandler } from '../src/server.ts'
import {
  listDeadLetteredRuns,
  retryDeadLetterRun,
} from '../src/control/retry.ts'
import {
  cancelPendingSteps,
  deadLetterRun,
  failStep,
  getRun,
  getStepsByRun,
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

// A one-step workflow whose handler bumps a shared counter, so a test can prove
// the step actually re-ran (not just that the row flipped status). Returns the
// registered handle and the counter.
async function registerCountingWorkflow(): Promise<{ handle: WorkflowHandle; ran: () => number }> {
  const name = unique('manual-retry-wf')
  let runs = 0
  const handle = defineWorkflow(name, (builder) => {
    builder.step('only-step', async (ctx) => {
      runs++
      return { echoed: ctx.input }
    })
  })
  await handle.register(sql)
  return { handle, ran: () => runs }
}

// Enqueue a run for `handle`, then force it into `dead_letter` the way the
// executor's exhausted-retry path does: fail its only step, cancel any pending
// downstream, mark the run failed, then park it. Returns the run + namespace.
async function seedDeadLetteredRun(
  handle: WorkflowHandle,
  reason = 'retries exhausted'
): Promise<{ run: RunRow; namespace: string }> {
  const namespace = unique('ns')
  const { runId } = await enqueueRun(sql, handle, { namespace })
  const steps = await getStepsByRun(sql, runId)
  const step = steps[0]
  expect(step).toBeDefined()
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

// Spin up a single worker on `namespace` and wait for the run to reach a
// terminal status, then stop it.
async function drainWithWorker(
  runId: string,
  namespace: string,
  handle: WorkflowHandle,
  timeoutMs = 15_000
): Promise<RunRow> {
  const worker: Worker = createWorker({
    db: sql,
    handles: [handle],
    namespace,
    workerId: unique('manual-retry-worker'),
    concurrency: 1,
    leaseTtlMs: 5_000,
    pollIntervalMs: 15,
    reclaimIntervalMs: 60_000,
  })
  worker.start()
  try {
    const deadline = Date.now() + timeoutMs
    let run = await getRun(sql, runId)
    while (
      run &&
      run.status !== 'completed' &&
      run.status !== 'failed' &&
      run.status !== 'cancelled' &&
      run.status !== 'dead_letter' &&
      Date.now() < deadline
    ) {
      await new Promise((resolve) => setTimeout(resolve, 20))
      run = await getRun(sql, runId)
    }
    if (!run) throw new Error(`drain: run "${runId}" vanished`)
    return run
  } finally {
    await worker.stop()
  }
}

describe('retryDeadLetterRun (control surface)', () => {
  test('a genuinely dead-lettered run is reset to queued and re-runs to completion', async () => {
    const { handle, ran } = await registerCountingWorkflow()
    const { run, namespace } = await seedDeadLetteredRun(handle)

    const result = await retryDeadLetterRun(sql, run.id)
    expect(result.retried).toBe(true)
    expect(result.reason).toBeUndefined()
    expect(result.run?.status).toBe('queued')
    expect(result.run?.dead_lettered_at).toBeNull()
    expect(result.run?.dead_letter_reason).toBeNull()
    expect(result.run?.finished_at).toBeNull()

    // The revived step is claimable by a normal worker, which re-runs it to
    // completion — the load-bearing proof that the reset lands the run in the
    // exact shape the claim loop expects.
    const finished = await drainWithWorker(run.id, namespace, handle)
    expect(finished.status).toBe('completed')
    expect(ran()).toBe(1)

    const steps = await getStepsByRun(sql, run.id)
    expect(steps[0]?.status).toBe('completed')
  })

  test('retrying a run that is NOT dead-lettered returns not_dead_letter and mutates nothing', async () => {
    const { handle } = await registerCountingWorkflow()
    const namespace = unique('ns')
    const { runId } = await enqueueRun(sql, handle, { namespace })

    const before = await getRun(sql, runId)
    expect(before?.status).toBe('queued')

    const result = await retryDeadLetterRun(sql, runId)
    expect(result.retried).toBe(false)
    expect(result.reason).toBe('not_dead_letter')
    expect(result.run).toBeUndefined()

    // Untouched.
    const after = await getRun(sql, runId)
    expect(after?.status).toBe('queued')
  })

  test('retrying an unknown run id returns not_dead_letter', async () => {
    const result = await retryDeadLetterRun(sql, crypto.randomUUID())
    expect(result.retried).toBe(false)
    expect(result.reason).toBe('not_dead_letter')
    expect(result.run).toBeUndefined()
  })

  test('a second retry of the same run is a no-op (already out of dead_letter)', async () => {
    const { handle } = await registerCountingWorkflow()
    const { run } = await seedDeadLetteredRun(handle)

    const first = await retryDeadLetterRun(sql, run.id)
    expect(first.retried).toBe(true)

    const second = await retryDeadLetterRun(sql, run.id)
    expect(second.retried).toBe(false)
    expect(second.reason).toBe('not_dead_letter')
  })
})

describe('listDeadLetteredRuns (control surface)', () => {
  test('surfaces parked runs and drops them once retried', async () => {
    const { handle } = await registerCountingWorkflow()
    const { run } = await seedDeadLetteredRun(handle)

    const listed = await listDeadLetteredRuns(sql, 500)
    expect(listed.some((r) => r.id === run.id)).toBe(true)

    await retryDeadLetterRun(sql, run.id)
    const afterRetry = await listDeadLetteredRuns(sql, 500)
    expect(afterRetry.some((r) => r.id === run.id)).toBe(false)
  })
})

describe('GET /dead-letter (operator HTTP)', () => {
  test('lists dead-lettered runs', async () => {
    const { handle } = await registerCountingWorkflow()
    const { run } = await seedDeadLetteredRun(handle, 'for http listing')

    const res = await handler(req('/dead-letter'))
    expect(res.status).toBe(200)
    const body = (await res.json()) as {
      runs: { runId: string; deadLetterReason: string | null; status: string }[]
      count: number
    }
    const found = body.runs.find((r) => r.runId === run.id)
    expect(found).toBeDefined()
    expect(found?.status).toBe('dead_letter')
    expect(found?.deadLetterReason).toBe('for http listing')
  })

  test('rejects a non-positive limit with 400', async () => {
    const res = await handler(req('/dead-letter?limit=0'))
    expect(res.status).toBe(400)
  })

  test('405 on wrong method', async () => {
    const res = await handler(req('/dead-letter', { method: 'POST' }))
    expect(res.status).toBe(405)
  })
})

describe('POST /dead-letter/:id/retry (operator HTTP)', () => {
  test('retries a dead-lettered run and reports queued', async () => {
    const { handle } = await registerCountingWorkflow()
    const { run } = await seedDeadLetteredRun(handle)

    const res = await handler(req(`/dead-letter/${run.id}/retry`, { method: 'POST' }))
    expect(res.status).toBe(200)
    const body = (await res.json()) as { runId: string; status: string; retried: boolean }
    expect(body.retried).toBe(true)
    expect(body.runId).toBe(run.id)
    expect(body.status).toBe('queued')

    expect((await getRun(sql, run.id))?.status).toBe('queued')
  })

  test('404 when the run is not dead-lettered', async () => {
    const { handle } = await registerCountingWorkflow()
    const namespace = unique('ns')
    const { runId } = await enqueueRun(sql, handle, { namespace })

    const res = await handler(req(`/dead-letter/${runId}/retry`, { method: 'POST' }))
    expect(res.status).toBe(404)
  })

  test('404 on an unknown run id', async () => {
    const res = await handler(req(`/dead-letter/${crypto.randomUUID()}/retry`, { method: 'POST' }))
    expect(res.status).toBe(404)
  })

  test('405 on wrong method', async () => {
    const res = await handler(req(`/dead-letter/${crypto.randomUUID()}/retry`, { method: 'GET' }))
    expect(res.status).toBe(405)
  })
})

describe('trigger routes still reachable through the composed handler', () => {
  test('GET /healthz falls through to the trigger handler', async () => {
    const res = await handler(req('/healthz'))
    expect(res.status).toBe(200)
    expect(await res.json()).toEqual({ ok: true })
  })
})
