// Unit-level smoke test for this part's own surface: enqueueRun actually
// returns before anything runs, and a worker started against the queue
// drains a run to completion using the real lease/claim/retry machinery.
// The full "3 workers, kill one mid-step, lease reclaimed" proof is owned
// by the Phase 2 integration suite, not this file.

import { describe, expect, test, beforeAll, afterAll } from 'bun:test'
import { createDb } from '../src/store/client.ts'
import { migrate } from '../src/store/migrate.ts'
import { defineWorkflow } from '../src/define/workflow.ts'
import { enqueueRun } from '../src/engine/executor.ts'
import { createWorker } from '../src/worker/worker.ts'
import { getRun, getStepsByRun } from '../src/store/repositories.ts'

const sql = createDb()

beforeAll(async () => {
  await migrate(sql)
})

afterAll(async () => {
  await sql.end()
})

describe('enqueueRun', () => {
  test('returns immediately without executing any step', async () => {
    const namespace = `worker-test-${crypto.randomUUID()}`
    const wf = defineWorkflow(`worker-test-enqueue-${crypto.randomUUID()}`, (builder) => {
      builder.step('only', async () => 'never runs here')
    })

    const { runId, created } = await enqueueRun(sql, wf, { namespace })
    expect(created).toBe(true)

    const run = await getRun(sql, runId)
    expect(run!.status).toBe('queued') // NOT running/completed — enqueueRun did not execute it

    const steps = await getStepsByRun(sql, runId)
    expect(steps).toHaveLength(1)
    expect(steps[0]!.status).toBe('ready') // materialized, but never claimed
  })
})

describe('createWorker', () => {
  test('drains an enqueued multi-step run to completion, including a retry', async () => {
    const namespace = `worker-test-${crypto.randomUUID()}`
    let attempts = 0
    const wf = defineWorkflow(`worker-test-drain-${crypto.randomUUID()}`, (builder) => {
      builder.step('a', async () => 'a-value')
      builder.step(
        'b',
        async () => {
          attempts++
          if (attempts === 1) throw new Error('flaky on first attempt')
          return 'b-value'
        },
        { dependsOn: ['a'], maxAttempts: 3 }
      )
    })

    const { runId } = await enqueueRun(sql, wf, { namespace })

    const worker = createWorker({
      db: sql,
      handles: [wf],
      namespace,
      concurrency: 2,
      leaseTtlMs: 5_000,
      pollIntervalMs: 20,
      reclaimIntervalMs: 60_000, // not exercising reclaim here
    })
    worker.start()

    const deadline = Date.now() + 10_000
    let run = await getRun(sql, runId)
    while (run && run.status !== 'completed' && run.status !== 'failed' && Date.now() < deadline) {
      await new Promise((resolve) => setTimeout(resolve, 25))
      run = await getRun(sql, runId)
    }

    await worker.stop()

    expect(run!.status).toBe('completed')
    expect(attempts).toBe(2) // failed once, retried, then succeeded
    expect(run!.output).toEqual({ a: 'a-value', b: 'b-value' })
    expect(worker.inFlight).toBe(0)
  })
})
