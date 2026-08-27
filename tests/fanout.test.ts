// Phase 4 shipping bar: "a run that fans out to 10 parallel steps and
// continues only when all 10 commit." Driven through real concurrent
// worker processes (in-process, but independent `createWorker` instances
// each with their own poll loop and lease), against real Postgres — the
// same 10-way concurrent-commit race repositories-dag.test.ts proves at the
// storage layer, exercised here end-to-end through defineWorkflow +
// enqueueRun + the worker loop.

import { describe, expect, test, beforeAll, afterAll } from 'bun:test'
import { createDb } from '../src/store/client.ts'
import { migrate } from '../src/store/migrate.ts'
import { defineWorkflow } from '../src/define/workflow.ts'
import { enqueueRun } from '../src/engine/executor.ts'
import { createWorker, type Worker } from '../src/worker/worker.ts'
import { getRun, getStepsByRun, countStepsByStatus, type RunRow } from '../src/store/repositories.ts'

const sql = createDb()

beforeAll(async () => {
  await migrate(sql)
})

afterAll(async () => {
  await sql.end()
})

async function drain(runId: string, workers: Worker[], timeoutMs = 15_000): Promise<RunRow> {
  const deadline = Date.now() + timeoutMs
  let run = await getRun(sql, runId)
  while (run && run.status !== 'completed' && run.status !== 'failed' && Date.now() < deadline) {
    await new Promise((resolve) => setTimeout(resolve, 20))
    run = await getRun(sql, runId)
  }
  await Promise.all(workers.map((w) => w.stop()))
  if (!run) throw new Error(`drain: run "${runId}" vanished`)
  return run
}

describe('phase 4 shipping bar: 10-way fan-out / fan-in across concurrent workers', () => {
  test('fans out to 10 parallel steps across 10 concurrent workers, joins exactly once', async () => {
    const namespace = `fanout-shipping-bar-${crypto.randomUUID()}`
    const N = 10

    // Tracks, per fan-out step, which worker actually ran it and when —
    // proof this genuinely parallelizes rather than one worker draining
    // the queue serially.
    const ranBy: Record<string, string> = {}
    let joinRanAt: number | undefined
    let maxConcurrent = 0
    let inFlightNow = 0

    const wf = defineWorkflow(`fanout-shipping-bar-wf-${crypto.randomUUID()}`, (builder) => {
      builder.step('split', async () => 'go')
      const fanNames = builder.fanOut(
        'shard',
        N,
        async (i, ctx) => {
          inFlightNow++
          maxConcurrent = Math.max(maxConcurrent, inFlightNow)
          // Hold the step open briefly so overlapping in-flight execution
          // is actually observable, not just theoretically possible.
          await new Promise((resolve) => setTimeout(resolve, 50))
          inFlightNow--
          return { index: i, base: (ctx.input as { base: number }).base }
        },
        { dependsOn: ['split'] }
      )
      builder.step(
        'join',
        async (ctx) => {
          joinRanAt = Date.now()
          return `joined-${(ctx.input as { base: number }).base}`
        },
        { dependsOn: fanNames }
      )
    })

    const { runId } = await enqueueRun(sql, wf, { namespace, input: { base: 42 } })

    // 10 independent worker instances, each concurrency 1 — the fleet the
    // shipping bar describes, not one worker with a concurrency knob.
    const workers: Worker[] = Array.from({ length: N }, (_, i) =>
      createWorker({
        db: sql,
        handles: [wf],
        namespace,
        workerId: `fanout-worker-${i}`,
        concurrency: 1,
        leaseTtlMs: 5_000,
        pollIntervalMs: 15,
        reclaimIntervalMs: 60_000,
      })
    )
    for (const w of workers) w.start()

    const run = await drain(runId, workers)

    expect(run.status).toBe('completed')
    expect(joinRanAt).toBeDefined()
    // the actual parallelism proof: more than one shard was in flight at once
    expect(maxConcurrent).toBeGreaterThan(1)

    const steps = await getStepsByRun(sql, runId)
    const shardSteps = steps.filter((s) => s.name.startsWith('shard-'))
    expect(shardSteps).toHaveLength(N)
    expect(shardSteps.every((s) => s.status === 'completed')).toBe(true)

    const counts = await countStepsByStatus(sql, runId)
    expect(counts.completed).toBe(N + 2) // split + N shards + join

    const output = run.output as Record<string, unknown>
    expect(output.join).toBe('joined-42')
    for (let i = 0; i < N; i++) {
      expect(output[`shard-${i}`]).toEqual({ index: i, base: 42 })
    }
  }, 20_000)

  test('the join step is claimed and runs exactly once even though all 10 parents commit concurrently', async () => {
    const namespace = `fanout-once-${crypto.randomUUID()}`
    const N = 10
    let joinRunCount = 0

    const wf = defineWorkflow(`fanout-once-wf-${crypto.randomUUID()}`, (builder) => {
      const fanNames = builder.fanOut('leaf', N, async (i) => i * 2)
      builder.step(
        'join',
        async () => {
          joinRunCount++
          return 'joined'
        },
        { dependsOn: fanNames }
      )
    })

    const { runId } = await enqueueRun(sql, wf, { namespace })

    const workers: Worker[] = Array.from({ length: 5 }, (_, i) =>
      createWorker({
        db: sql,
        handles: [wf],
        namespace,
        workerId: `once-worker-${i}`,
        concurrency: 4,
        leaseTtlMs: 5_000,
        pollIntervalMs: 15,
        reclaimIntervalMs: 60_000,
      })
    )
    for (const w of workers) w.start()

    const run = await drain(runId, workers)

    expect(run.status).toBe('completed')
    expect(joinRunCount).toBe(1) // never released twice, never left blocked
  }, 20_000)
})
