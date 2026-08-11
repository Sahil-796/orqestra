// Phase 4 #20: child workflows. Exercises the public surface
// (runChildWorkflow / runChildWorkflowResult, src/control/child.ts) against
// real Postgres and real workers — the same "durable, crash-proof" bar
// Phase 2/3's worker tests hold.
//
// Design note (see src/control/child.ts's module doc for the long version):
// the parent step suspends into the dedicated `'blocked'` status with
// `awaited_child_run_id` set, and is woken by the child run's terminal
// transition — no polling, no timer. The properties proved here are
// unchanged from the original polling implementation: the parent's lease is
// genuinely released while the child runs, resumption survives the spawning
// worker never coming back, and a failed/cancelled child propagates per the
// stated policy. tests/child-blocking.test.ts covers the block mechanism
// itself (suspends exactly once, wakes on the child's terminal write).
//
// `pollIntervalMs` still appears below purely to keep proving that the old
// option is accepted and ignored rather than being a compile error.

import { describe, expect, test, beforeAll, afterAll } from 'bun:test'
import { createDb } from '../src/store/client.ts'
import { migrate } from '../src/store/migrate.ts'
import { defineWorkflow } from '../src/define/workflow.ts'
import { enqueueRun } from '../src/engine/executor.ts'
import { createWorker, type Worker } from '../src/worker/worker.ts'
import { getRun, getStepsByRun, getChildRuns, type RunRow } from '../src/store/repositories.ts'
import {
  runChildWorkflow,
  runChildWorkflowResult,
  ChildWorkflowError,
  isChildWorkflowError,
  classifyChildRun,
} from '../src/index.ts'

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

describe('classifyChildRun (pure)', () => {
  test('completed carries the run output through untouched', () => {
    const outcome = classifyChildRun('completed', { double: 42 }, undefined)
    expect(outcome).toEqual({ ok: true, value: { double: 42 } })
  })

  test('failed/cancelled carry the supplied step error', () => {
    const error = { name: 'Error', message: 'boom' }
    expect(classifyChildRun('failed', undefined, error)).toEqual({ ok: false, status: 'failed', error })
    expect(classifyChildRun('cancelled', undefined, undefined)).toEqual({
      ok: false,
      status: 'cancelled',
      error: undefined,
    })
  })

  test('throws on a non-terminal status', () => {
    expect(() => classifyChildRun('queued', undefined, undefined)).toThrow()
    expect(() => classifyChildRun('running', undefined, undefined)).toThrow()
  })
})

describe('isChildWorkflowError', () => {
  test('accepts a ChildWorkflowError and rejects ordinary errors/values', () => {
    const error = new ChildWorkflowError('run-1', 'failed')
    expect(isChildWorkflowError(error)).toBe(true)
    expect(isChildWorkflowError(new Error('boom'))).toBe(false)
    expect(isChildWorkflowError(null)).toBe(false)
    expect(isChildWorkflowError({})).toBe(false)
  })

  test('brand check works without instanceof (duplicate module classes)', () => {
    const impostor = { ...new ChildWorkflowError('run-1', 'cancelled') }
    expect(impostor instanceof ChildWorkflowError).toBe(false)
    expect(isChildWorkflowError(impostor)).toBe(true)
  })
})

describe('runChildWorkflow: end-to-end', () => {
  test('a parent step spawns a child and resumes with its result', async () => {
    const namespace = `child-wf-${crypto.randomUUID()}`

    const childWf = defineWorkflow(`child-wf-child-${crypto.randomUUID()}`, (builder) => {
      builder.step('double', async (ctx) => {
        const { n } = ctx.input as { n: number }
        return n * 2
      })
    })

    const parentWf = defineWorkflow(`child-wf-parent-${crypto.randomUUID()}`, (builder) => {
      builder.step('spawn', async (ctx) => {
        const doubled = await runChildWorkflow<number>(sql, ctx, childWf, {
          input: ctx.input,
          pollIntervalMs: 40,
        })
        return { doubled }
      })
    })

    const { runId } = await enqueueRun(sql, parentWf, { namespace, input: { n: 21 } })

    const worker = createWorker({
      db: sql,
      handles: [parentWf, childWf],
      namespace,
      pollIntervalMs: 20,
      leaseTtlMs: 5_000,
    })
    worker.start()

    const run = await drain(runId, [worker])

    expect(run.status).toBe('completed')
    expect(run.output).toEqual({ spawn: { doubled: { double: 42 } } })

    // The child run really is linked to the parent (run.parent_run_id).
    const children = await getChildRuns(sql, runId)
    expect(children).toHaveLength(1)
    expect(children[0]?.status).toBe('completed')
    expect(children[0]?.output).toEqual({ double: 42 })
  }, 20_000)

  test("the parent's lease is genuinely released while the child runs", async () => {
    const namespace = `child-wf-lease-${crypto.randomUUID()}`

    // The child takes a while (its own step sleeps), so there's a real
    // window to observe the parent step sitting idle mid-wait.
    const childWf = defineWorkflow(`child-wf-lease-child-${crypto.randomUUID()}`, (builder) => {
      builder.step('slow', async (ctx) => {
        await ctx.sleep('400ms')
        return 'child-done'
      })
    })

    const parentWf = defineWorkflow(`child-wf-lease-parent-${crypto.randomUUID()}`, (builder) => {
      builder.step('spawn', async (ctx) => {
        return runChildWorkflow<string>(sql, ctx, childWf, { input: {}, pollIntervalMs: 50 })
      })
    })

    const { runId } = await enqueueRun(sql, parentWf, { namespace })

    const worker = createWorker({
      db: sql,
      handles: [parentWf, childWf],
      namespace,
      pollIntervalMs: 20,
      leaseTtlMs: 5_000,
    })
    worker.start()

    // Wait for the parent to actually suspend rather than sleeping a fixed
    // guess at how long that takes: how quickly a worker gets around to
    // claiming the step is a property of the machine, not of the feature,
    // and a fixed window turns a slow laptop into a red test. The assertions
    // below still only hold if the suspension is real — the child must still
    // be in flight when we look, which is checked explicitly.
    const deadline = Date.now() + 10_000
    let spawnStep = (await getStepsByRun(sql, runId)).find((s) => s.name === 'spawn')
    while (spawnStep?.status !== 'blocked' && Date.now() < deadline) {
      await new Promise((resolve) => setTimeout(resolve, 10))
      spawnStep = (await getStepsByRun(sql, runId)).find((s) => s.name === 'spawn')
    }

    expect(spawnStep).toBeDefined()
    // Not `running`: no worker holds this step's lease mid-wait.
    expect(spawnStep?.status).toBe('blocked')
    expect(spawnStep?.lease_owner).toBeNull()
    expect(spawnStep?.lease_expires_at).toBeNull()
    // And it says what it's waiting on, rather than a wake time.
    const children = await getChildRuns(sql, runId)
    expect(spawnStep?.awaited_child_run_id).toBe(children[0]!.id)
    // The child is genuinely still in flight at this point, so what we just
    // observed is a parent released mid-wait — not one that had already been
    // woken by a child that finished while we waited to look.
    expect(['queued', 'running']).toContain(children[0]!.status)

    const run = await drain(runId, [worker])
    expect(run.status).toBe('completed')
    expect(run.output).toEqual({ spawn: { slow: 'child-done' } })
  }, 20_000)
})

describe('failed-child propagation policy', () => {
  test('default: a failed child fails the parent step (and its run)', async () => {
    const namespace = `child-wf-fail-${crypto.randomUUID()}`

    const childWf = defineWorkflow(`child-wf-fail-child-${crypto.randomUUID()}`, (builder) => {
      builder.step(
        'boom',
        async () => {
          throw new Error('child exploded')
        },
        { maxAttempts: 1 }
      )
    })

    const parentWf = defineWorkflow(`child-wf-fail-parent-${crypto.randomUUID()}`, (builder) => {
      builder.step(
        'spawn',
        async (ctx) => runChildWorkflow(sql, ctx, childWf, { input: {}, pollIntervalMs: 40 }),
        { maxAttempts: 1 }
      )
    })

    const { runId } = await enqueueRun(sql, parentWf, { namespace })

    const worker = createWorker({
      db: sql,
      handles: [parentWf, childWf],
      namespace,
      pollIntervalMs: 20,
      leaseTtlMs: 5_000,
    })
    worker.start()

    const run = await drain(runId, [worker])

    expect(run.status).toBe('failed')

    const steps = await getStepsByRun(sql, runId)
    const spawnStep = steps.find((s) => s.name === 'spawn')
    const error = spawnStep?.error as { name?: string; message?: string } | null
    expect(error?.name).toBe('ChildWorkflowError')
    expect(error?.message).toContain('failed')

    const children = await getChildRuns(sql, runId)
    expect(children[0]?.status).toBe('failed')
  }, 20_000)

  test('runChildWorkflowResult never throws — the parent step can inspect the failure itself', async () => {
    const namespace = `child-wf-fail-result-${crypto.randomUUID()}`

    const childWf = defineWorkflow(`child-wf-fail-result-child-${crypto.randomUUID()}`, (builder) => {
      builder.step(
        'boom',
        async () => {
          throw new Error('child exploded again')
        },
        { maxAttempts: 1 }
      )
    })

    const parentWf = defineWorkflow(`child-wf-fail-result-parent-${crypto.randomUUID()}`, (builder) => {
      builder.step('spawn', async (ctx) => {
        const result = await runChildWorkflowResult(sql, ctx, childWf, { input: {}, pollIntervalMs: 40 })
        return result.ok ? { outcome: 'unexpected-success' } : { outcome: 'compensated', status: result.status }
      })
    })

    const { runId } = await enqueueRun(sql, parentWf, { namespace })

    const worker = createWorker({
      db: sql,
      handles: [parentWf, childWf],
      namespace,
      pollIntervalMs: 20,
      leaseTtlMs: 5_000,
    })
    worker.start()

    const run = await drain(runId, [worker])

    // The parent step itself never threw — its own run completes normally,
    // carrying the compensation it chose to return.
    expect(run.status).toBe('completed')
    expect(run.output).toEqual({ spawn: { outcome: 'compensated', status: 'failed' } })
  }, 20_000)
})

describe('crash-proof resume', () => {
  test('the parent resumes even when the worker that spawned the child never comes back', async () => {
    const namespace = `child-wf-crash-${crypto.randomUUID()}`

    const childWf = defineWorkflow(`child-wf-crash-child-${crypto.randomUUID()}`, (builder) => {
      builder.step('slow', async (ctx) => {
        await ctx.sleep('300ms')
        return 'child-survived'
      })
    })

    const parentWf = defineWorkflow(`child-wf-crash-parent-${crypto.randomUUID()}`, (builder) => {
      builder.step('spawn', async (ctx) => {
        return runChildWorkflow<string>(sql, ctx, childWf, { input: {}, pollIntervalMs: 40 })
      })
    })

    const { runId } = await enqueueRun(sql, parentWf, { namespace })

    // Worker A: spawns the child and takes the parent step through its
    // first poll-sleep, then is stopped for good — nothing about
    // resolution may depend on this process instance ever running again.
    const workerA = createWorker({
      db: sql,
      handles: [parentWf, childWf],
      namespace,
      pollIntervalMs: 20,
      leaseTtlMs: 5_000,
      workerId: `worker-a-${crypto.randomUUID()}`,
    })
    workerA.start()
    await new Promise((resolve) => setTimeout(resolve, 120))
    await workerA.stop()

    // Confirm the run genuinely has not finished yet — there's real work
    // left for a successor to pick up.
    const midRun = await getRun(sql, runId)
    expect(midRun?.status === 'completed').toBe(false)

    // Worker B: an entirely separate instance (own workerId, own closures,
    // shares nothing with A but the database) picks up wherever A left off.
    const workerB = createWorker({
      db: sql,
      handles: [parentWf, childWf],
      namespace,
      pollIntervalMs: 20,
      leaseTtlMs: 5_000,
      workerId: `worker-b-${crypto.randomUUID()}`,
    })
    workerB.start()

    const run = await drain(runId, [workerB])

    expect(run.status).toBe('completed')
    expect(run.output).toEqual({ spawn: { slow: 'child-survived' } })
  }, 20_000)
})
