// Phase 7 on the DURABLE worker path (worker.ts's commitOutcome), as opposed to
// the inline startRun/executeRun path proved in dead-letter/failure-policy
// tests. These drive a real createWorker claim loop end to end:
//   #26 — a run whose step exhausts its retries lands in `dead_letter`, not a
//         bare `failed`, so the operator DLQ + manual retry have something to
//         act on under real worker operation.
//   #28 — a continue_on_error run keeps its independent steps progressing and
//         finishes as `completed_with_errors` rather than dying on the first
//         terminal failure.
// Compensation (#29) is NOT exercised here: the worker drives one step in
// isolation without replaying the definition, so a completed step's
// ctx.compensate closure isn't in-process at a later step's failure — durable
// worker-path saga is a separate, deliberate piece of work.

import { describe, expect, test, beforeAll, afterAll } from 'bun:test'
import { createDb } from '../src/store/client.ts'
import { migrate } from '../src/store/migrate.ts'
import { defineWorkflow } from '../src/define/workflow.ts'
import { enqueueRun } from '../src/engine/executor.ts'
import { createWorker } from '../src/worker/worker.ts'
import { getRun, getStepsByRun, type RunRow } from '../src/store/repositories.ts'

const sql = createDb()

beforeAll(async () => {
  await migrate(sql)
})

afterAll(async () => {
  await sql.end()
})

const TERMINAL = new Set(['completed', 'completed_with_errors', 'failed', 'cancelled', 'dead_letter'])

async function drainToTerminal(runId: string): Promise<RunRow> {
  const deadline = Date.now() + 10_000
  let run = await getRun(sql, runId)
  while (run && !TERMINAL.has(run.status) && Date.now() < deadline) {
    await new Promise((resolve) => setTimeout(resolve, 25))
    run = await getRun(sql, runId)
  }
  return run!
}

describe('worker path: failure handling', () => {
  test('#26 fail_fast: a run that exhausts its retries lands in the dead-letter queue', async () => {
    const namespace = `worker-dlq-${crypto.randomUUID()}`
    let downstreamRan = false
    const wf = defineWorkflow(`worker-dlq-${crypto.randomUUID()}`, (builder) => {
      builder.step('boom', async () => {
        throw new Error('kaboom')
      })
      builder.step(
        'never',
        async () => {
          downstreamRan = true
          return 'unreachable'
        },
        { dependsOn: ['boom'] }
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
      reclaimIntervalMs: 60_000,
    })
    worker.start()
    const run = await drainToTerminal(runId)
    await worker.stop()

    // The run is parked in the DLQ (not a bare `failed`) so the operator surface
    // can find and re-drive it; the failing step row itself stays `failed`.
    expect(run.status).toBe('dead_letter')
    expect(run.dead_lettered_at).not.toBeNull()
    expect(downstreamRan).toBe(false)

    const steps = await getStepsByRun(sql, runId)
    expect(steps.find((s) => s.name === 'boom')?.status).toBe('failed')
    // fail_fast cancels the rest of a dead run rather than leaving it pending.
    expect(steps.find((s) => s.name === 'never')?.status).toBe('cancelled')
  })

  test('#28 continue_on_error: independent steps still run; the run finishes completed_with_errors', async () => {
    const namespace = `worker-coe-${crypto.randomUUID()}`
    let okRan = false
    let dependentRan = false
    const wf = defineWorkflow(
      `worker-coe-${crypto.randomUUID()}`,
      (builder) => {
        builder.step('boom', async () => {
          throw new Error('kaboom')
        })
        builder.step('ok', async () => {
          okRan = true
          return 'ok-value'
        })
        builder.step(
          'dependent',
          async () => {
            dependentRan = true
            return 'unreachable'
          },
          { dependsOn: ['boom'] }
        )
      },
      { failurePolicy: 'continue_on_error' }
    )

    const { runId } = await enqueueRun(sql, wf, { namespace })
    const worker = createWorker({
      db: sql,
      handles: [wf],
      namespace,
      concurrency: 2,
      leaseTtlMs: 5_000,
      pollIntervalMs: 20,
      reclaimIntervalMs: 60_000,
    })
    worker.start()
    const run = await drainToTerminal(runId)
    await worker.stop()

    // The failed step did not sink the whole run; the independent step ran and
    // the run finalized with errors (not dead-lettered, no rollback).
    expect(run.status).toBe('completed_with_errors')
    expect(okRan).toBe(true)
    expect(dependentRan).toBe(false)
    expect((run.output as Record<string, unknown>).ok).toBe('ok-value')

    const steps = await getStepsByRun(sql, runId)
    expect(steps.find((s) => s.name === 'boom')?.status).toBe('failed')
    expect(steps.find((s) => s.name === 'ok')?.status).toBe('completed')
    // A step behind a failed dependency is never satisfiable, so it stays pending.
    expect(steps.find((s) => s.name === 'dependent')?.status).toBe('pending')
  })
})
