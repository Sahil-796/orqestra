// Phase 5 (#18) end-to-end proof — the phase's shipping bar: a workflow that
// blocks on `ctx.waitForEvent("payment.confirmed")` and resumes when the
// event is published. This exercises the whole path through a REAL worker:
//   * the step suspends into `blocked` and RELEASES its worker (no busy poll)
//   * publishing the event wakes it exactly once and delivers the payload
//   * the step body runs exactly twice (once to the wait, once after) — the
//     replay converges, it does not re-suspend or double-run downstream
//   * the wait is durable across worker death (block on worker A, kill A,
//     publish, resume on a fresh worker B)

import { describe, expect, test, beforeAll, afterAll } from 'bun:test'
import { createDb } from '../src/store/client.ts'
import { migrate } from '../src/store/migrate.ts'
import { defineWorkflow, type WorkflowHandle } from '../src/define/workflow.ts'
import { enqueueRun } from '../src/engine/executor.ts'
import { publishSignal } from '../src/control/signal.ts'
import { createWorker, type Worker } from '../src/worker/worker.ts'
import {
  getRun,
  getStepsByRun,
  getStepsWaitingForEvent,
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

function unique(prefix: string): string {
  return `${prefix}-${crypto.randomUUID()}`
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

async function history(runId: string): Promise<HistoryRow[]> {
  return sql<HistoryRow[]>`select * from history where run_id = ${runId} order by at, id`
}

async function stepByName(runId: string, name: string): Promise<StepRow> {
  const steps = await getStepsByRun(sql, runId)
  const found = steps.find((s) => s.name === name)
  if (!found) throw new Error(`step "${name}" not found in run ${runId}`)
  return found
}

// A one-step workflow that waits for `eventName` and returns the payload it
// was woken with, wrapped in a marker so the test can assert delivery. The
// counters prove how many times the body ran and how far past the wait it got.
function waiterWorkflow(eventName: string): {
  handle: WorkflowHandle
  counts: { bodyRuns: number; afterWaitRuns: number }
} {
  const counts = { bodyRuns: 0, afterWaitRuns: 0 }
  const handle = defineWorkflow(unique('phase5-waiter'), (builder) => {
    builder.step(
      'await-payment',
      async (ctx) => {
        counts.bodyRuns++
        const payload = await ctx.waitForEvent<{ amount: number }>(eventName)
        counts.afterWaitRuns++
        return { received: payload }
      },
      // maxAttempts: 1 — a wait must not consume the step's only attempt, so
      // the step can only finish if registerStepEventWait gave the attempt back.
      { maxAttempts: 1 }
    )
  })
  return { handle, counts }
}

describe('waitForEvent end-to-end', () => {
  test('blocks, releases the worker, then resumes with the published payload', async () => {
    const eventName = unique('payment.confirmed')
    const ns = unique('ns')
    const { handle, counts } = waiterWorkflow(eventName)
    const { runId } = await enqueueRun(sql, handle, { input: {}, namespace: ns })

    const worker = createWorker({ db: sql, handles: [handle], namespace: ns })
    worker.start()

    // The step suspends into `blocked` waiting for the event…
    await until('step is blocked waiting for the event', async () => {
      const waiting = await getStepsWaitingForEvent(sql, runId)
      return waiting.length === 1 && waiting[0]!.name === 'await-payment'
    })

    // …and the worker is not pinned by it: nothing in flight, body ran once.
    await until('worker released the slot', async () => worker.inFlight === 0)
    expect(counts.bodyRuns).toBe(1)
    expect(counts.afterWaitRuns).toBe(0)

    // It stays blocked and does NOT busy-poll: the body count doesn't climb.
    await wait(300)
    expect(counts.bodyRuns).toBe(1)
    const stillBlocked = await stepByName(runId, 'await-payment')
    expect(stillBlocked.status).toBe('blocked')

    // Publish the event → wake, replay, complete.
    const published = await publishSignal(sql, { name: eventName, payload: { amount: 99 } })
    expect(published.woken.length).toBe(1)

    const run = await drain(runId, [worker])
    expect(run.status).toBe('completed')
    expect((run.output as Record<string, unknown>)['await-payment']).toEqual({ received: { amount: 99 } })

    // Exactly-once wake: the body ran exactly twice total (once to the wait,
    // once after), and the after-wait section exactly once.
    expect(counts.bodyRuns).toBe(2)
    expect(counts.afterWaitRuns).toBe(1)

    const events = await history(runId)
    expect(events.filter((h) => h.type === 'step.waiting_for_event').length).toBe(1)
    expect(events.filter((h) => h.type === 'step.event_delivered').length).toBe(1)
  }, 20_000)

  test('only the correlated run is resumed by a correlated event', async () => {
    const eventName = unique('order.paid')
    const ns = unique('ns')
    const counts = { a: 0, b: 0 }
    const handle = defineWorkflow(unique('phase5-corr'), (builder) => {
      builder.step(
        'wait',
        async (ctx) => {
          const which = (ctx.input as { which: string }).which
          const payload = await ctx.waitForEvent<number>(eventName, { correlationKey: which })
          if (which === 'A') counts.a++
          else counts.b++
          return payload
        },
        { maxAttempts: 1 }
      )
    })

    const runA = await enqueueRun(sql, handle, { input: { which: 'A' }, namespace: ns })
    const runB = await enqueueRun(sql, handle, { input: { which: 'B' }, namespace: ns })

    const worker = createWorker({ db: sql, handles: [handle], namespace: ns })
    worker.start()

    await until('both runs blocked', async () => {
      const a = await stepByName(runA.runId, 'wait')
      const b = await stepByName(runB.runId, 'wait')
      return a.status === 'blocked' && b.status === 'blocked'
    })

    // Wake only A.
    await publishSignal(sql, { name: eventName, correlationKey: 'A', payload: 1 })

    await until('run A completed', async () => (await getRun(sql, runA.runId))?.status === 'completed')
    // B is still blocked — a different correlation didn't touch it.
    const b = await stepByName(runB.runId, 'wait')
    expect(b.status).toBe('blocked')
    expect(counts.a).toBe(1)
    expect(counts.b).toBe(0)

    // Now wake B and let everything drain.
    await publishSignal(sql, { name: eventName, correlationKey: 'B', payload: 2 })
    const finalB = await drain(runB.runId, [worker])
    expect(finalB.status).toBe('completed')
    expect(counts.b).toBe(1)
  }, 20_000)

  test('the wait survives worker death — block on one worker, publish, resume on another', async () => {
    const eventName = unique('shipment.ready')
    const ns = unique('ns')
    const { handle, counts } = waiterWorkflow(eventName)
    const { runId } = await enqueueRun(sql, handle, { input: {}, namespace: ns })

    // Worker A blocks the step, then goes away (graceful stop — the block is a
    // durable row, not held by the worker, so this models a crash for the
    // wait's purposes: after stop() nothing is executing this run).
    const workerA = createWorker({ db: sql, handles: [handle], namespace: ns, workerId: unique('A') })
    workerA.start()
    await until('step blocked on worker A', async () => {
      const waiting = await getStepsWaitingForEvent(sql, runId)
      return waiting.length === 1
    })
    await workerA.stop()
    expect(counts.bodyRuns).toBe(1)

    // Publish while NO worker is running — the event just wakes the durable
    // blocked row to `ready`.
    await publishSignal(sql, { name: eventName, payload: { amount: 7 } })
    await until('step ready after publish', async () => {
      const s = await stepByName(runId, 'await-payment')
      return s.status === 'ready'
    })

    // A fresh worker B picks up the now-ready step and finishes the run.
    const workerB = createWorker({ db: sql, handles: [handle], namespace: ns, workerId: unique('B') })
    workerB.start()
    const run = await drain(runId, [workerB])
    expect(run.status).toBe('completed')
    expect((run.output as Record<string, unknown>)['await-payment']).toEqual({ received: { amount: 7 } })
    // Body ran twice total across the two workers; after-wait exactly once.
    expect(counts.bodyRuns).toBe(2)
    expect(counts.afterWaitRuns).toBe(1)
  }, 20_000)
})
