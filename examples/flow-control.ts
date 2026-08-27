// Phase 6, runnable: a workflow that declares all three flow-control step
// options —
//   #12 concurrency:  cap how many `chargeCard` steps run at once, globally
//   #13 rateLimit:    cap how many `callPartnerApi` steps START per window
//   #14 priority:     let `notifyCustomer` jump ahead of routine work, with
//                      aging (ORQ_PRIORITY_AGE_RATE_PER_SEC / _MAX_BOOST) so a
//                      flood of high-priority steps can't starve normal ones
//
// None of these need extra infrastructure — they're columns the claim query
// (src/store/repositories.ts `claimNextStep`) already enforces atomically
// under concurrent claimers, the same way the base queue does.
//
// Run with: bun run examples/flow-control.ts

import { defineWorkflow, orquestra, enqueueRun, createWorker, repositories } from '../src/index.ts'
import type { RunStatus } from '../src/index.ts'

export const flowControlDemoWorkflow = defineWorkflow('flow-control-demo', (wf) => {
  // #12 concurrency limits: at most 3 `chargeCard` steps are ever `running`
  // at once — across every run in the whole database that shares this key,
  // not per-worker and not per-run. Good for a downstream dependency (a
  // payment processor, a DB connection pool) that can't take unbounded
  // parallel load. A step blocked by a full key just stays `ready` and is
  // retried on the next poll — never failed.
  wf.step(
    'chargeCard',
    async (ctx) => {
      const { orderId } = ctx.input as { orderId: number }
      return { orderId, charged: true }
    },
    { concurrency: { key: 'payment-processor', limit: 3 } }
  )

  // #13 rate limiting: at most 100 `callPartnerApi` steps may START per
  // 60-second window — again global by key. This is the "≤100 calls/min"
  // shape from the build plan. A step that would exceed the window's budget
  // is deferred (its `run_after` is pushed to the next window boundary), not
  // failed or dropped — it becomes claimable again the moment fresh budget
  // exists.
  wf.step(
    'callPartnerApi',
    async (ctx) => {
      const { orderId } = ctx.input as { orderId: number }
      return { orderId, called: true }
    },
    { dependsOn: ['chargeCard'], rateLimit: { key: 'partner-api', limit: 100, windowMs: 60_000 } }
  )

  // #14 priority + fairness: a customer-facing notification should jump the
  // queue ahead of routine work sitting at the default priority (0). But
  // priority alone would let a flood of these starve everything else
  // forever — aging fixes that: the longer a lower-priority step waits, the
  // more its *effective* priority climbs (see src/control/priority.ts),
  // until it eventually outranks even fresh high-priority work. Aging is
  // config, not per-step: ORQ_PRIORITY_AGE_RATE_PER_SEC (points/sec, default
  // 1) and ORQ_PRIORITY_AGE_MAX_BOOST (cap, default 100).
  wf.step(
    'notifyCustomer',
    async (ctx) => {
      const { orderId } = ctx.input as { orderId: number }
      return { orderId, notified: true }
    },
    { dependsOn: ['callPartnerApi'], priority: 10 }
  )

  // Routine background work at the default priority — this is what aging
  // protects from being starved if `notifyCustomer` steps keep flooding in.
  wf.step(
    'reconcileLedger',
    async (ctx) => {
      const { orderId } = ctx.input as { orderId: number }
      return { orderId, reconciled: true }
    },
    { dependsOn: ['chargeCard'] }
  )
})

function isTerminal(status: RunStatus): boolean {
  return status === 'completed' || status === 'failed' || status === 'cancelled'
}

if (import.meta.main) {
  const orq = orquestra()
  const runToken = Date.now()
  // Namespaced so this demo's workers only compete over rows it created
  // itself, not leftover rows other example/test runs left behind in this
  // same shared dev Postgres (see examples/workers.ts for the same pattern).
  const namespace = `examples-flow-control-${runToken}`

  const RUN_COUNT = 8
  const runIds: string[] = []
  for (let orderId = 0; orderId < RUN_COUNT; orderId++) {
    const { runId, created } = await enqueueRun(orq.db, flowControlDemoWorkflow, {
      input: { orderId },
      namespace,
      idempotencyKey: `flow-control-demo-${runToken}-${orderId}`,
    })
    runIds.push(runId)
    console.log(`enqueued run ${runId} for order ${orderId} (created=${created})`)
  }

  // More workers than the concurrency limit, on purpose — this is what
  // proves the cap is enforced across workers, not just within one.
  const workers = Array.from({ length: 5 }, (_, i) =>
    createWorker({
      db: orq.db,
      handles: [flowControlDemoWorkflow],
      workerId: `flow-control-worker-${i}`,
      concurrency: 2,
      leaseTtlMs: 5_000,
      pollIntervalMs: 100,
      reclaimIntervalMs: 1_000,
      namespace,
    })
  )

  for (const worker of workers) worker.start()
  console.log(`${workers.length} workers draining ${RUN_COUNT} runs (concurrency + rate + priority all in play)...`)

  const deadline = Date.now() + 30_000
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
