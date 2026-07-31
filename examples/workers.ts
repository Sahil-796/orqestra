// Phase 2, runnable: enqueue a handful of runs DURABLY (enqueueRun returns
// immediately — nothing executes on this call), start 3 concurrent workers
// pointed at the same Postgres queue, and watch them drain it. One step
// deliberately fails its first attempt so the run shows retry + backoff
// actually happening, not just the happy path.
//
// Run with: bun run examples/workers.ts

import { defineWorkflow, orquestra, enqueueRun, createWorker, repositories } from '../src/index.ts'
import type { RunStatus } from '../src/index.ts'

// Per-orderId attempt counter for the flaky step below. Module-level and
// shared across all 3 workers in this process on purpose — it's what makes
// the demo deterministic ("fails exactly once, then succeeds") regardless
// of which worker ends up claiming the retry.
const transformAttempts = new Map<number, number>()

export const drainDemoWorkflow = defineWorkflow('drain-demo', (wf) => {
  wf.step('fetch', async (ctx) => {
    const { orderId } = ctx.input as { orderId: number }
    await new Promise((resolve) => setTimeout(resolve, 30 + Math.random() * 100))
    return { orderId, fetchedAt: ctx.now().toISOString() }
  })

  wf.step(
    'transform',
    async (ctx) => {
      const { orderId } = ctx.input as { orderId: number }
      const attempts = (transformAttempts.get(orderId) ?? 0) + 1
      transformAttempts.set(orderId, attempts)
      if (attempts === 1) {
        throw new Error(`transient failure transforming order ${orderId} (attempt ${attempts})`)
      }
      await new Promise((resolve) => setTimeout(resolve, 20))
      return { orderId, transformed: true, attempts }
    },
    { dependsOn: ['fetch'], maxAttempts: 3 }
  )

  wf.step(
    'notify',
    async (ctx) => {
      const { orderId } = ctx.input as { orderId: number }
      return { orderId, notifiedAt: ctx.now().toISOString() }
    },
    { dependsOn: ['transform'] }
  )
})

function isTerminal(status: RunStatus): boolean {
  return status === 'completed' || status === 'failed' || status === 'cancelled'
}

if (import.meta.main) {
  const orq = orquestra()
  const runToken = Date.now()
  // The step queue is global by design (any worker can claim any due step)
  // — namespaced so this demo's workers only compete over rows it created
  // itself, not leftover step rows other test/example runs left behind in
  // this same shared dev Postgres (see tests/queue.test.ts for the same
  // pattern and rationale).
  const namespace = `examples-workers-${runToken}`

  const RUN_COUNT = 6
  const runIds: string[] = []
  for (let orderId = 0; orderId < RUN_COUNT; orderId++) {
    const { runId, created } = await enqueueRun(orq.db, drainDemoWorkflow, {
      input: { orderId },
      namespace,
      idempotencyKey: `workers-demo-${runToken}-${orderId}`,
    })
    runIds.push(runId)
    console.log(`enqueued run ${runId} for order ${orderId} (created=${created})`)
  }

  const workers = Array.from({ length: 3 }, (_, i) =>
    createWorker({
      db: orq.db,
      handles: [drainDemoWorkflow],
      workerId: `demo-worker-${i}`,
      concurrency: 2,
      leaseTtlMs: 5_000,
      pollIntervalMs: 100,
      reclaimIntervalMs: 1_000,
      namespace,
    })
  )

  for (const worker of workers) worker.start()
  console.log(`${workers.length} workers draining ${RUN_COUNT} runs...`)

  // Poll run status from the outside — exactly what a real caller of the
  // durable API does, since enqueueRun already returned before any of this
  // ran. No engine internals here, just repositories.getRun.
  const deadline = Date.now() + 20_000
  let allDone = false
  while (Date.now() < deadline) {
    const runs = await Promise.all(runIds.map((id) => repositories.getRun(orq.db, id)))
    allDone = runs.every((run) => run !== undefined && isTerminal(run.status))
    if (allDone) break
    await new Promise((resolve) => setTimeout(resolve, 150))
  }

  if (!allDone) console.error('timed out waiting for runs to drain')

  const finalRuns = await Promise.all(runIds.map((id) => repositories.getRun(orq.db, id)))
  for (const run of finalRuns) {
    if (!run) continue
    console.log(`run ${run.id} -> ${run.status}`, run.status === 'completed' ? JSON.stringify(run.output) : '')
  }

  await Promise.all(workers.map((worker) => worker.stop()))
  console.log('all workers stopped')

  await orq.close()
}
