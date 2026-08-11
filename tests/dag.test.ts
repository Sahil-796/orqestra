// Phase 4 engine-layer proof: dependency gating (#19) and conditional
// branching (#17), driven through the real worker claim/run/commit loop —
// not just the storage-layer primitives repositories-dag.test.ts already
// covers, but the policy this unit built on top of them: a skipped
// dependency satisfies a downstream edge, a step whose entire lineage was
// skipped cascades to skipped too, and a join with at least one real
// completion still runs. Fan-out/fan-in at the phase's shipping-bar scale
// (10 parallel steps) lives in tests/fanout.test.ts.

import { describe, expect, test, beforeAll, afterAll } from 'bun:test'
import { createDb } from '../src/store/client.ts'
import { migrate } from '../src/store/migrate.ts'
import { defineWorkflow } from '../src/define/workflow.ts'
import { enqueueRun, startRun } from '../src/engine/executor.ts'
import { createWorker, type Worker } from '../src/worker/worker.ts'
import { getRun, getStepsByRun, type RunRow } from '../src/store/repositories.ts'

const sql = createDb()

beforeAll(async () => {
  await migrate(sql)
})

afterAll(async () => {
  await sql.end()
})

async function drain(runId: string, worker: Worker, timeoutMs = 10_000): Promise<RunRow> {
  const deadline = Date.now() + timeoutMs
  let run = await getRun(sql, runId)
  while (run && run.status !== 'completed' && run.status !== 'failed' && Date.now() < deadline) {
    await new Promise((resolve) => setTimeout(resolve, 20))
    run = await getRun(sql, runId)
  }
  await worker.stop()
  if (!run) throw new Error(`drain: run "${runId}" vanished`)
  return run
}

function makeWorker(namespace: string, handles: Parameters<typeof createWorker>[0]['handles']): Worker {
  const worker = createWorker({
    db: sql,
    handles,
    namespace,
    concurrency: 4,
    leaseTtlMs: 5_000,
    pollIntervalMs: 20,
    reclaimIntervalMs: 60_000,
  })
  worker.start()
  return worker
}

// ---- #19 dependency gating, through the worker loop ------------------------

describe('dependency gating (#19)', () => {
  test('a step with unmet deps is not claimable until its last dependency commits', async () => {
    const namespace = `dag-gating-${crypto.randomUUID()}`
    const order: string[] = []
    const wf = defineWorkflow(`dag-gating-wf-${crypto.randomUUID()}`, (builder) => {
      builder.step('a', async () => {
        order.push('a')
        return 'a-value'
      })
      builder.step(
        'b',
        async () => {
          order.push('b')
          return 'b-value'
        },
        { dependsOn: ['a'] }
      )
    })

    const { runId } = await enqueueRun(sql, wf, { namespace })
    const worker = makeWorker(namespace, [wf])
    const run = await drain(runId, worker)

    expect(run.status).toBe('completed')
    expect(order).toEqual(['a', 'b']) // b never ran before a committed
    expect(run.output).toEqual({ a: 'a-value', b: 'b-value' })
  })

  test('a step is never claimed twice and never left stuck (diamond DAG)', async () => {
    const namespace = `dag-diamond-${crypto.randomUUID()}`
    const runCounts: Record<string, number> = {}
    const bump = (name: string) => (runCounts[name] = (runCounts[name] ?? 0) + 1)
    const wf = defineWorkflow(`dag-diamond-wf-${crypto.randomUUID()}`, (builder) => {
      builder.step('a', async () => {
        bump('a')
        return 1
      })
      builder.step(
        'b',
        async () => {
          bump('b')
          return 2
        },
        { dependsOn: ['a'] }
      )
      builder.step(
        'c',
        async () => {
          bump('c')
          return 3
        },
        { dependsOn: ['a'] }
      )
      builder.step(
        'd',
        async () => {
          bump('d')
          return 4
        },
        { dependsOn: ['b', 'c'] }
      )
    })

    const { runId } = await enqueueRun(sql, wf, { namespace })
    const worker = makeWorker(namespace, [wf])
    const run = await drain(runId, worker)

    expect(run.status).toBe('completed')
    expect(runCounts).toEqual({ a: 1, b: 1, c: 1, d: 1 })
    expect(run.output).toEqual({ a: 1, b: 2, c: 3, d: 4 })
  })
})

// ---- #17 conditional branching + skip policy --------------------------------

describe('conditional branching (#17)', () => {
  test('the untaken branch is skipped and the join runs on the completed branch alone', async () => {
    const namespace = `dag-branch-${crypto.randomUUID()}`
    let branchBRan = false
    const wf = defineWorkflow(`dag-branch-wf-${crypto.randomUUID()}`, (builder) => {
      builder.step('decide', async (ctx) => {
        ctx.skip('branch-b')
        return 'took-a'
      })
      builder.step('branch-a', async () => 'a-ran', { dependsOn: ['decide'] })
      builder.step(
        'branch-b',
        async () => {
          branchBRan = true
          return 'b-ran'
        },
        { dependsOn: ['decide'] }
      )
      builder.step('join', async () => 'joined', { dependsOn: ['branch-a', 'branch-b'] })
    })

    const { runId } = await enqueueRun(sql, wf, { namespace })
    const worker = makeWorker(namespace, [wf])
    const run = await drain(runId, worker)

    expect(run.status).toBe('completed')
    expect(branchBRan).toBe(false)

    const steps = await getStepsByRun(sql, runId)
    const branchB = steps.find((s) => s.name === 'branch-b')
    expect(branchB?.status).toBe('skipped')
    expect(branchB?.skip_reason).toBe('branch not taken')

    const join = steps.find((s) => s.name === 'join')
    expect(join?.status).toBe('completed')
    expect(run.output).toEqual({ decide: 'took-a', 'branch-a': 'a-ran', join: 'joined' })
  })

  test('a step downstream of only a skipped dependency cascades to skipped, not run', async () => {
    const namespace = `dag-cascade-${crypto.randomUUID()}`
    let branchB2Ran = false
    const wf = defineWorkflow(`dag-cascade-wf-${crypto.randomUUID()}`, (builder) => {
      builder.step('decide', async (ctx) => {
        ctx.skip('branch-b')
        return 'took-a'
      })
      builder.step('branch-a', async () => 'a-ran', { dependsOn: ['decide'] })
      builder.step('branch-b', async () => 'b-ran', { dependsOn: ['decide'] })
      // chained further into the untaken branch — must cascade, not execute
      builder.step(
        'branch-b2',
        async () => {
          branchB2Ran = true
          return 'b2-ran'
        },
        { dependsOn: ['branch-b'] }
      )
      // fans in on the real branch AND the cascade-skipped chain — must
      // still run, since branch-a actually completed.
      builder.step('join', async () => 'joined', { dependsOn: ['branch-a', 'branch-b2'] })
    })

    const { runId } = await enqueueRun(sql, wf, { namespace })
    const worker = makeWorker(namespace, [wf])
    const run = await drain(runId, worker)

    expect(run.status).toBe('completed')
    expect(branchB2Ran).toBe(false)

    const steps = await getStepsByRun(sql, runId)
    expect(steps.find((s) => s.name === 'branch-b')?.status).toBe('skipped')
    expect(steps.find((s) => s.name === 'branch-b2')?.status).toBe('skipped')
    expect(steps.find((s) => s.name === 'join')?.status).toBe('completed')
    expect(run.output).toEqual({
      decide: 'took-a',
      'branch-a': 'a-ran',
      'branch-b2': undefined,
      join: 'joined',
    })
  })

  test('a join whose every branch was skipped is itself skipped, not deadlocked', async () => {
    const namespace = `dag-all-skipped-${crypto.randomUUID()}`
    const wf = defineWorkflow(`dag-all-skipped-wf-${crypto.randomUUID()}`, (builder) => {
      builder.step('decide', async (ctx) => {
        ctx.skip('branch-a', 'branch-b')
        return 'took-neither'
      })
      builder.step('branch-a', async () => 'a-ran', { dependsOn: ['decide'] })
      builder.step('branch-b', async () => 'b-ran', { dependsOn: ['decide'] })
      builder.step('join', async () => 'joined', { dependsOn: ['branch-a', 'branch-b'] })
    })

    const { runId } = await enqueueRun(sql, wf, { namespace })
    const worker = makeWorker(namespace, [wf])
    const run = await drain(runId, worker)

    expect(run.status).toBe('completed') // not stuck — resolves via cascade
    const steps = await getStepsByRun(sql, runId)
    expect(steps.find((s) => s.name === 'branch-a')?.status).toBe('skipped')
    expect(steps.find((s) => s.name === 'branch-b')?.status).toBe('skipped')
    expect(steps.find((s) => s.name === 'join')?.status).toBe('skipped')
  })

  test('the inline executor (startRun) also honors ctx.skip() for a direct branch', async () => {
    const wf = defineWorkflow(`dag-inline-branch-wf-${crypto.randomUUID()}`, (builder) => {
      builder.step('decide', async (ctx) => {
        ctx.skip('branch-b')
        return 'took-a'
      })
      builder.step('branch-a', async () => 'a-ran', { dependsOn: ['decide'] })
      builder.step('branch-b', async () => 'b-ran', { dependsOn: ['decide'] })
      builder.step('join', async () => 'joined', { dependsOn: ['branch-a', 'branch-b'] })
    })

    const result = await startRun(sql, wf)
    expect(result.status).toBe('completed')

    const steps = await getStepsByRun(sql, result.runId)
    expect(steps.find((s) => s.name === 'branch-b')?.status).toBe('skipped')
    expect(steps.find((s) => s.name === 'join')?.status).toBe('completed')
  })
})
