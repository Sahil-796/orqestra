// Phase 3, runnable: a workflow whose middle step sleeps.
//
// This is the Phase 3 ships criterion in miniature. `await ctx.sleep('...')`
// does NOT block — it throws a control-flow signal that the worker catches,
// writes the step back to the queue with a future `run_after`, and then goes
// looking for other work. Nothing is held open for the duration of the sleep:
// no worker slot, no database connection, not even this process (kill it and
// the step still wakes on schedule, because the wake time is a column).
//
// Two things a first-time reader should notice:
//
//   1. The sleeping step re-runs FROM THE TOP when it wakes. There is no
//      saved stack — durability comes from the row, not from the closure. The
//      "reminder" log below therefore prints twice. On the second execution
//      the engine knows this step has already served one sleep (step.sleep_seq
//      is 1), so `ctx.sleep` returns immediately instead of suspending again,
//      and the step runs on to its return value. Write step bodies so that
//      re-running the part before a sleep is harmless.
//   2. The sleep costs no retry attempt. Suspending is not failing.
//
// The demo sleeps ~2 seconds so it finishes while you watch; in production
// the same code says '24h' and behaves identically.
//
// Run with: bun run examples/sleeping-workflow.ts

import { defineWorkflow, orquestra, enqueueRun, createWorker, repositories } from '../src/index.ts'
import type { RunStatus } from '../src/index.ts'

const SLEEP_FOR = '2s'

export const reminderWorkflow = defineWorkflow('sleep-demo', (wf) => {
  wf.step('createOrder', async (ctx) => {
    const { orderId } = ctx.input as { orderId: number }
    console.log(`[createOrder] order ${orderId} placed`)
    return { orderId }
  })

  // The sleeping step. Everything above the `await ctx.sleep(...)` line runs
  // twice — once before suspending, once after waking — so keep it cheap and
  // idempotent.
  wf.step(
    'sendReminder',
    async (ctx) => {
      const { orderId } = ctx.input as { orderId: number }
      console.log(`[sendReminder] order ${orderId}: waiting ${SLEEP_FOR} before the reminder`)

      await ctx.sleep(SLEEP_FOR)

      // Only reached on the execution *after* the sleep has been served.
      console.log(`[sendReminder] order ${orderId}: woke up, sending reminder`)
      return { orderId, remindedAt: new Date().toISOString() }
    },
    { dependsOn: ['createOrder'] }
  )

  wf.step(
    'archive',
    async (ctx) => {
      const { orderId } = ctx.input as { orderId: number }
      console.log(`[archive] order ${orderId} archived`)
      return { orderId, archived: true }
    },
    { dependsOn: ['sendReminder'] }
  )
})

function isTerminal(status: RunStatus): boolean {
  return status === 'completed' || status === 'failed' || status === 'cancelled'
}

if (import.meta.main) {
  const orq = orquestra()
  const runToken = Date.now()
  // The step queue is global (any worker can claim any due step), so this
  // demo namespaces its rows to avoid competing with other examples/tests
  // sharing the same dev Postgres.
  const namespace = `examples-sleep-${runToken}`

  const { runId } = await enqueueRun(orq.db, reminderWorkflow, {
    input: { orderId: 1 },
    namespace,
    idempotencyKey: `sleep-demo-${runToken}`,
  })
  console.log(`enqueued run ${runId}`)

  // A single worker with concurrency 1 — the point is that even ONE worker
  // slot is enough, because the sleeping step is not occupying it.
  const worker = createWorker({
    db: orq.db,
    handles: [reminderWorkflow],
    workerId: 'sleep-demo-worker',
    concurrency: 1,
    leaseTtlMs: 5_000,
    pollIntervalMs: 100,
    reclaimIntervalMs: 1_000,
    namespace,
  })
  worker.start()

  // While the step sleeps, it sits in the queue as `ready` with a future
  // run_after — visible here, invisible to claimNextStep until it is due.
  const deadline = Date.now() + 30_000
  let sawSleeping = false
  let run = await repositories.getRun(orq.db, runId)
  while (Date.now() < deadline) {
    if (!sawSleeping) {
      const sleeping = await repositories.getSleepingSteps(orq.db, runId)
      const step = sleeping[0]
      if (step) {
        sawSleeping = true
        console.log(
          `[observer] step "${step.name}" is asleep until ${step.sleeping_until?.toISOString()} — the worker is free`
        )
      }
    }
    run = await repositories.getRun(orq.db, runId)
    if (run && isTerminal(run.status)) break
    await new Promise((resolve) => setTimeout(resolve, 100))
  }

  console.log(`run ${runId} -> ${run?.status}`, run?.status === 'completed' ? JSON.stringify(run.output) : '')

  await worker.stop()
  await orq.close()
}
