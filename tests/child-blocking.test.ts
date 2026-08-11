// Phase 4 #20, the event-driven half: a parent step awaiting a child
// suspends into the `'blocked'` status EXACTLY ONCE and is woken by the
// child run's terminal transition — not by a timer, and not by the process
// that spawned it.
//
// tests/child-workflows.test.ts covers the feature's behaviour (results,
// propagation policy, crash-style resume). This file covers the mechanism:
// what the step row looks like while it waits, how many times it is
// re-executed, what cannot wake it, and what does.

import { describe, expect, test, beforeAll, afterAll } from 'bun:test'
import { createDb } from '../src/store/client.ts'
import { migrate } from '../src/store/migrate.ts'
import { defineWorkflow } from '../src/define/workflow.ts'
import { enqueueRun } from '../src/engine/executor.ts'
import { maybeFinalizeRun, sweepBlockedChildAwaits, wakeParentAwaiting } from '../src/engine/dag.ts'
import { reclaimExpiredLeases } from '../src/queue/lease.ts'
import { cancelRun } from '../src/control/cancel.ts'
import { runChildWorkflow, runChildWorkflowResult } from '../src/control/child.ts'
import { createWorker, type Worker } from '../src/worker/worker.ts'
import {
  getChildRuns,
  getRun,
  getStepsByRun,
  updateRunStatus,
  type HistoryRow,
  type RunRow,
  type StepRow,
} from '../src/store/repositories.ts'

const sql = createDb()

beforeAll(async () => {
  await migrate(sql)
})

afterAll(async () => {
  await sql.end()
})

function wait(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

async function step(runId: string, name: string): Promise<StepRow> {
  const steps = await getStepsByRun(sql, runId)
  const found = steps.find((s) => s.name === name)
  if (!found) throw new Error(`step "${name}" not found in run ${runId}`)
  return found
}

async function history(runId: string): Promise<HistoryRow[]> {
  return sql<HistoryRow[]>`select * from history where run_id = ${runId} order by at, id`
}

/** Wait until `predicate` holds for the run's steps, or throw on timeout. */
async function until(
  label: string,
  predicate: () => Promise<boolean>,
  timeoutMs = 10_000
): Promise<void> {
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
  while (run && run.status !== 'completed' && run.status !== 'failed' && run.status !== 'cancelled' && Date.now() < deadline) {
    await wait(20)
    run = await getRun(sql, runId)
  }
  await Promise.all(workers.map((w) => w.stop()))
  if (!run) throw new Error(`drain: run "${runId}" vanished`)
  return run
}

describe('the block itself', () => {
  test('a parent awaiting a child suspends exactly once and is woken by the child finishing', async () => {
    const namespace = `blocking-once-${crypto.randomUUID()}`

    // The child takes ~700ms of wall clock, so there is a long window in
    // which a polling implementation would have woken the parent several
    // times over.
    const childWf = defineWorkflow(`blocking-once-child-${crypto.randomUUID()}`, (builder) => {
      builder.step('slow', async (ctx) => {
        await ctx.sleep('700ms')
        return 'child-done'
      })
    })

    const parentWf = defineWorkflow(`blocking-once-parent-${crypto.randomUUID()}`, (builder) => {
      builder.step('spawn', async (ctx) => runChildWorkflow<string>(sql, ctx, childWf, { input: {} }))
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

    await until('parent step blocked', async () => (await step(runId, 'spawn')).status === 'blocked')

    const blocked = await step(runId, 'spawn')
    const children = await getChildRuns(sql, runId)
    expect(children).toHaveLength(1)
    // It records what it is waiting on — the previously-dead column.
    expect(blocked.awaited_child_run_id).toBe(children[0]!.id)
    // Its worker is genuinely released: no lease, and nothing to reclaim.
    expect(blocked.lease_owner).toBeNull()
    expect(blocked.lease_expires_at).toBeNull()
    // And it is NOT parked on a clock the way a sleeping step is.
    expect(blocked.sleeping_until).toBeNull()

    // Watch it for half a second: a timer-driven implementation would
    // re-claim it (attempt++ / status flipping through running) in here.
    const attemptWhileBlocked = blocked.attempt
    for (let i = 0; i < 10; i++) {
      await wait(50)
      const now = await step(runId, 'spawn')
      if (now.status !== 'blocked') break // the child finished — stop sampling
      expect(now.attempt).toBe(attemptWhileBlocked)
      expect(now.awaited_child_run_id).toBe(children[0]!.id)
    }

    const run = await drain(runId, [worker])
    expect(run.status).toBe('completed')
    expect(run.output).toEqual({ spawn: { slow: 'child-done' } })

    // The step suspended once and ran twice: the execution that spawned and
    // blocked, and the one that replayed to the child's result.
    const events = await history(runId)
    expect(events.filter((e) => e.type === 'step.blocked')).toHaveLength(1)
    expect(events.filter((e) => e.type === 'step.sleeping')).toHaveLength(0)
    // ...but only one attempt is spent: blocking gives back the attempt the
    // claim consumed, exactly as sleeping does, so awaiting a child never
    // eats into the step's retry budget.
    expect((await step(runId, 'spawn')).attempt).toBe(1)
    // The link is cleared on resolution, not left dangling.
    expect((await step(runId, 'spawn')).awaited_child_run_id).toBeNull()
  }, 30_000)

  test('nothing else can resurrect or finalize past a blocked step', async () => {
    const namespace = `blocking-inert-${crypto.randomUUID()}`

    const childWf = defineWorkflow(`blocking-inert-child-${crypto.randomUUID()}`, (builder) => {
      builder.step('slow', async (ctx) => {
        await ctx.sleep('600ms')
        return 'ok'
      })
    })

    const parentWf = defineWorkflow(`blocking-inert-parent-${crypto.randomUUID()}`, (builder) => {
      builder.step('spawn', async (ctx) => runChildWorkflow(sql, ctx, childWf, { input: {} }))
    })

    const { runId } = await enqueueRun(sql, parentWf, { namespace })

    const worker = createWorker({
      db: sql,
      handles: [parentWf, childWf],
      namespace,
      pollIntervalMs: 20,
      // Short TTL so an (incorrect) reaper pass would have plenty of expired
      // leases to be tempted by.
      leaseTtlMs: 400,
    })
    worker.start()

    await until('parent step blocked', async () => (await step(runId, 'spawn')).status === 'blocked')

    // 1. The lease reaper only scans `running` steps, so a blocked step is
    //    invisible to it — it must not come back as a "crashed" step.
    await reclaimExpiredLeases(sql)
    expect((await step(runId, 'spawn')).status).toBe('blocked')

    // 2. `blocked` is not a done state: the run must not finalize past it.
    expect(await maybeFinalizeRun(sql, runId)).toBeUndefined()
    expect((await getRun(sql, runId))?.status).toBe('running')

    // 3. Waking is gated on the child actually being terminal — calling the
    //    resolver early is a no-op, not a premature release.
    const childRunId = (await getChildRuns(sql, runId))[0]!.id
    expect(await wakeParentAwaiting(sql, childRunId)).toBeUndefined()
    expect((await step(runId, 'spawn')).status).toBe('blocked')

    const run = await drain(runId, [worker])
    expect(run.status).toBe('completed')
  }, 30_000)
})

describe('who does the waking', () => {
  test('the worker that spawned the child can be gone before the child finishes', async () => {
    const namespace = `blocking-handoff-${crypto.randomUUID()}`

    const childWf = defineWorkflow(`blocking-handoff-child-${crypto.randomUUID()}`, (builder) => {
      builder.step('slow', async (ctx) => {
        await ctx.sleep('400ms')
        return 'woken-by-b'
      })
    })

    const parentWf = defineWorkflow(`blocking-handoff-parent-${crypto.randomUUID()}`, (builder) => {
      builder.step('spawn', async (ctx) => runChildWorkflow<string>(sql, ctx, childWf, { input: {} }))
    })

    const { runId } = await enqueueRun(sql, parentWf, { namespace })

    const workerA = createWorker({
      db: sql,
      handles: [parentWf, childWf],
      namespace,
      pollIntervalMs: 20,
      leaseTtlMs: 5_000,
      workerId: `blocking-a-${crypto.randomUUID()}`,
    })
    workerA.start()
    await until('parent step blocked', async () => (await step(runId, 'spawn')).status === 'blocked')
    await workerA.stop()

    // A is gone while the parent is still blocked and the child unfinished.
    expect((await step(runId, 'spawn')).status).toBe('blocked')
    const childRunId = (await getChildRuns(sql, runId))[0]!.id
    expect((await getRun(sql, childRunId))?.status).not.toBe('completed')

    const workerB = createWorker({
      db: sql,
      handles: [parentWf, childWf],
      namespace,
      pollIntervalMs: 20,
      leaseTtlMs: 5_000,
      workerId: `blocking-b-${crypto.randomUUID()}`,
    })
    workerB.start()

    const run = await drain(runId, [workerB])
    expect(run.status).toBe('completed')
    expect(run.output).toEqual({ spawn: { slow: 'woken-by-b' } })
  }, 30_000)

  test('the reconciliation sweep wakes a step whose child finished without an inline wake', async () => {
    const namespace = `blocking-sweep-${crypto.randomUUID()}`

    const childWf = defineWorkflow(`blocking-sweep-child-${crypto.randomUUID()}`, (builder) => {
      builder.step('slow', async (ctx) => {
        await ctx.sleep('30s') // never finishes on its own within this test
        return 'unreachable'
      })
    })

    const parentWf = defineWorkflow(`blocking-sweep-parent-${crypto.randomUUID()}`, (builder) => {
      builder.step('spawn', async (ctx) => runChildWorkflow(sql, ctx, childWf, { input: {} }))
    })

    const { runId } = await enqueueRun(sql, parentWf, { namespace })

    const workerA = createWorker({
      db: sql,
      handles: [parentWf, childWf],
      namespace,
      pollIntervalMs: 20,
      leaseTtlMs: 5_000,
    })
    workerA.start()
    await until('parent step blocked', async () => (await step(runId, 'spawn')).status === 'blocked')
    await workerA.stop()

    // Drive the child terminal behind the engine's back — this is what a
    // finalization path that forgot to call wakeParentAwaiting looks like
    // from the parent's side.
    const childRunId = (await getChildRuns(sql, runId))[0]!.id
    await updateRunStatus(sql, childRunId, 'completed', {
      output: { slow: 'swept' },
      finishedAt: new Date(),
    })
    expect((await step(runId, 'spawn')).status).toBe('blocked')

    const woken = await sweepBlockedChildAwaits(sql)
    expect(woken).toContain((await step(runId, 'spawn')).id)
    expect((await step(runId, 'spawn')).status).toBe('ready')

    const workerB = createWorker({
      db: sql,
      handles: [parentWf, childWf],
      namespace,
      pollIntervalMs: 20,
      leaseTtlMs: 5_000,
    })
    workerB.start()

    const run = await drain(runId, [workerB])
    expect(run.status).toBe('completed')
    expect(run.output).toEqual({ spawn: { slow: 'swept' } })
  }, 30_000)

  test('a cancelled child wakes its parent too (cancellation is a terminal transition)', async () => {
    const namespace = `blocking-cancel-${crypto.randomUUID()}`

    const childWf = defineWorkflow(`blocking-cancel-child-${crypto.randomUUID()}`, (builder) => {
      // Cooperative: it watches ctx.signal, so the cancel actually stops it
      // (JS has no preemption — see engine/timeout.ts).
      builder.step('long', async (ctx) => {
        await new Promise<void>((resolve, reject) => {
          const timer = setTimeout(resolve, 5_000)
          ctx.signal.addEventListener(
            'abort',
            () => {
              clearTimeout(timer)
              reject(new Error('child step aborted'))
            },
            { once: true }
          )
        })
        return 'should-not-be-reached'
      })
    })

    const parentWf = defineWorkflow(`blocking-cancel-parent-${crypto.randomUUID()}`, (builder) => {
      builder.step('spawn', async (ctx) => {
        const result = await runChildWorkflowResult(sql, ctx, childWf, { input: {} })
        return result.ok ? { outcome: 'unexpected' } : { outcome: 'child-was', status: result.status }
      })
    })

    const { runId } = await enqueueRun(sql, parentWf, { namespace })

    const worker = createWorker({
      db: sql,
      handles: [parentWf, childWf],
      namespace,
      concurrency: 2,
      pollIntervalMs: 20,
      // Heartbeat (leaseTtlMs / 3) is the cancellation checkpoint, so keep
      // the TTL short enough that the in-flight child step notices quickly.
      leaseTtlMs: 900,
    })
    worker.start()

    await until('parent step blocked', async () => (await step(runId, 'spawn')).status === 'blocked')
    const childRunId = (await getChildRuns(sql, runId))[0]!.id
    await until('child step running', async () => (await step(childRunId, 'long')).status === 'running')

    await cancelRun(sql, childRunId)

    const run = await drain(runId, [worker])
    expect(run.status).toBe('completed')
    expect(run.output).toEqual({ spawn: { outcome: 'child-was', status: 'cancelled' } })
    expect((await getRun(sql, childRunId))?.status).toBe('cancelled')
  }, 30_000)
})
