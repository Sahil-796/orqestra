// The Phase 3 "ships" proof, against real Postgres: sleep, timeouts and
// cancellation observed through durable state, not mocks. Every assertion is
// about a row (or a history entry) that survived the process, because that is
// the whole claim being made — a sleep that only exists in a worker's memory
// is a setTimeout, not a durable sleep.
//
// Timing discipline: no fixed `await sleep(n)` used as a synchronisation
// primitive. Everything waits on a *condition* with a generous deadline, so a
// loaded machine makes this test slower, never flaky. The one genuine time
// dependency — a sleeping step outlasting a trivial run on the same worker —
// is asserted from durable timestamps, with a sleep long enough that the
// ordering can't realistically invert.

import { describe, expect, test, beforeAll, afterAll } from 'bun:test'
import { createDb } from '../src/store/client.ts'
import { migrate } from '../src/store/migrate.ts'
import { defineWorkflow } from '../src/define/workflow.ts'
import { enqueueRun } from '../src/engine/executor.ts'
import { createWorker, type Worker } from '../src/worker/worker.ts'
import {
  getRun,
  getSleepingSteps,
  getStepsByRun,
  requestRunCancellation,
  type HistoryRow,
  type RunRow,
  type StepRow,
} from '../src/store/repositories.ts'
import {
  cancellableWorkflow,
  quickWorkflow,
  sleeperWorkflow,
  timeoutWorkflow,
} from './fixtures/execution-control-workflows.ts'

const sql = createDb()

beforeAll(async () => {
  await migrate(sql)
})

afterAll(async () => {
  await sql.end()
})

const DEADLINE_MS = 20_000

/** Poll `read` until `done` accepts its result, or fail loudly with what was last seen. */
async function until<T>(
  what: string,
  read: () => Promise<T>,
  done: (value: T) => boolean,
  deadlineMs = DEADLINE_MS
): Promise<T> {
  const expiresAt = Date.now() + deadlineMs
  let last: T = await read()
  while (!done(last)) {
    if (Date.now() > expiresAt) {
      throw new Error(`timed out after ${deadlineMs}ms waiting for: ${what}\nlast seen: ${JSON.stringify(last)}`)
    }
    await new Promise((resolve) => setTimeout(resolve, 20))
    last = await read()
  }
  return last
}

function stepNamed(steps: StepRow[], name: string): StepRow {
  const step = steps.find((s) => s.name === name)
  if (!step) throw new Error(`no step named "${name}" in [${steps.map((s) => s.name).join(', ')}]`)
  return step
}

async function history(runId: string): Promise<HistoryRow[]> {
  return sql<HistoryRow[]>`select * from history where run_id = ${runId} order by at, id`
}

function terminal(run: RunRow | undefined): boolean {
  return (
    run?.status === 'completed' ||
    run?.status === 'completed_with_errors' ||
    run?.status === 'failed' ||
    run?.status === 'cancelled' ||
    run?.status === 'dead_letter'
  )
}

async function stopQuietly(worker: Worker): Promise<void> {
  await worker.stop()
}

describe('#9 sleep — a sleeping step releases its worker and wakes on schedule', () => {
  test('suspends without burning an attempt, frees the worker, then completes on wake', async () => {
    const namespace = `phase3-sleep-${crypto.randomUUID()}`
    const sleepMs = 2_000
    const sleeper = sleeperWorkflow(sleepMs)
    const quick = quickWorkflow()

    const { runId: sleeperRunId } = await enqueueRun(sql, sleeper.handle, { namespace })

    // concurrency: 1 is what makes "the worker was released" a real claim —
    // with a second slot the quick run below could have been picked up by a
    // slot the sleeper never occupied.
    const worker = createWorker({
      db: sql,
      handles: [sleeper.handle, quick.handle],
      namespace,
      concurrency: 1,
      leaseTtlMs: 2_000,
      heartbeatIntervalMs: 100,
      pollIntervalMs: 20,
      reclaimIntervalMs: 60_000,
    })
    worker.start()

    // ---- 1. the suspension itself, seen in the row ----
    const sleeping = await until(
      'the nap step to be parked asleep',
      () => getSleepingSteps(sql, sleeperRunId),
      (rows) => rows.length === 1
    )
    const napAsleep = sleeping[0]!
    const observedAt = new Date()

    expect(napAsleep.status).toBe('ready') // back in the queue, NOT held as `running`
    expect(napAsleep.run_after.getTime()).toBeGreaterThan(observedAt.getTime())
    expect(napAsleep.sleeping_until).not.toBeNull()
    expect(napAsleep.sleep_seq).toBe(1) // one sleep served
    // The attempt claiming consumed was handed back: a sleep is not an attempt.
    // The step's maxAttempts is 1, so a burnt attempt would strand it forever.
    expect(napAsleep.attempt).toBe(0)
    expect(napAsleep.max_attempts).toBe(1)
    // Nothing is holding it — no lease, no worker, no connection.
    expect(napAsleep.lease_owner).toBeNull()
    expect(napAsleep.lease_expires_at).toBeNull()
    expect(sleeper.counts.bodyRuns).toBe(1)
    expect(sleeper.counts.afterSleepRuns).toBe(0)

    // The worker's single slot is free while the sleep is outstanding.
    expect(worker.inFlight).toBe(0)

    // ---- 2. the freed worker really does other work mid-sleep ----
    const { runId: quickRunId } = await enqueueRun(sql, quick.handle, { namespace })
    const quickRun = await until(
      'the unrelated quick run to complete while the sleeper sleeps',
      () => getRun(sql, quickRunId),
      (run) => terminal(run)
    )
    expect(quickRun!.status).toBe('completed')
    expect(quick.counts.runs).toBe(1)

    // ---- 3. the wake ----
    const sleeperRun = await until(
      'the sleeper run to complete after waking',
      () => getRun(sql, sleeperRunId),
      (run) => terminal(run)
    )
    await stopQuietly(worker)

    expect(sleeperRun!.status).toBe('completed')
    expect(sleeperRun!.output).toEqual({ nap: 'awake' })

    // Durable ordering proof that the sleep genuinely deferred work rather
    // than blocking: the run enqueued *after* the sleep started finished
    // *before* the sleeper did, on the same single-slot worker.
    expect(quickRun!.finished_at).not.toBeNull()
    expect(sleeperRun!.finished_at).not.toBeNull()
    expect(quickRun!.finished_at!.getTime()).toBeLessThan(sleeperRun!.finished_at!.getTime())

    // The step body ran twice — from the top, as designed — and the
    // already-served sleep resolved immediately the second time instead of
    // suspending again (sleep_seq stayed at 1).
    expect(sleeper.counts.bodyRuns).toBe(2)
    expect(sleeper.counts.afterSleepRuns).toBe(1)

    const nap = stepNamed(await getStepsByRun(sql, sleeperRunId), 'nap')
    expect(nap.status).toBe('completed')
    expect(nap.sleep_seq).toBe(1)
    expect(nap.sleeping_until).toBeNull() // marker cleared when it woke
    // Two claims, one of them refunded by the sleep: attempt lands at 1, and
    // never exceeded max_attempts along the way.
    expect(nap.attempt).toBe(1)

    const events = await history(sleeperRunId)
    const types = events.map((e) => e.type)
    expect(types).toContain('step.sleeping')
    expect(types).toContain('step.completed')
    // A sleep is not a failure: nothing on this run was ever retried or failed.
    expect(types).not.toContain('step.retry_scheduled')
    expect(types).not.toContain('step.failed')

    const sleepEvent = events.find((e) => e.type === 'step.sleeping')!
    const data = sleepEvent.data as { durationMs: number; seq: number; wakeAt: string }
    expect(data.durationMs).toBe(sleepMs)
    expect(data.seq).toBe(1)

    expect(worker.inFlight).toBe(0)
  }, 40_000)
})

describe('#10 step timeouts', () => {
  test('a step that blows its budget is marked timed out and follows the retry policy', async () => {
    const namespace = `phase3-timeout-${crypto.randomUUID()}`
    const timeoutMs = 150
    const wf = timeoutWorkflow(timeoutMs)

    const { runId } = await enqueueRun(sql, wf.handle, { namespace })

    const worker = createWorker({
      db: sql,
      handles: [wf.handle],
      namespace,
      concurrency: 1,
      // Comfortably longer than the step's budget: the timeout must be what
      // ends the attempt, not a lease expiring underneath it.
      leaseTtlMs: 10_000,
      heartbeatIntervalMs: 200,
      pollIntervalMs: 20,
      reclaimIntervalMs: 60_000,
      retryPolicy: { baseMs: 50, factor: 2, maxMs: 200, jitter: false },
    })
    worker.start()

    const run = await until(
      'the timing-out run to reach a terminal state',
      () => getRun(sql, runId),
      (r) => terminal(r)
    )
    await stopQuietly(worker)

    expect(run!.status).toBe('completed')
    expect(run!.output).toEqual({ slow: 'quick enough' })

    // The first attempt was abandoned by the timeout, the second succeeded.
    expect(wf.counts.starts).toBe(2)
    // ...and the abandoned one was told to stop, not just left dangling.
    expect(wf.counts.abortsObserved).toBe(1)

    const slow = stepNamed(await getStepsByRun(sql, runId), 'slow')
    expect(slow.status).toBe('completed')
    expect(slow.attempt).toBe(2)
    expect(slow.timeout_ms).toBe(timeoutMs) // the definition's budget reached the row

    const events = await history(runId)
    const timedOut = events.filter((e) => e.type === 'step.timed_out')
    expect(timedOut).toHaveLength(1)
    const timedOutData = timedOut[0]!.data as {
      attempt: number
      timeoutMs: number
      error: { message: string }
    }
    expect(timedOutData.attempt).toBe(1)
    expect(timedOutData.timeoutMs).toBe(timeoutMs)
    expect(timedOutData.error.message).toContain('timeout')

    // It went through the ordinary failure path: retry scheduled with backoff,
    // parked in the future — not failed outright, not spun on immediately.
    const retries = events.filter((e) => e.type === 'step.retry_scheduled')
    expect(retries).toHaveLength(1)
    const retryData = retries[0]!.data as { attempt: number; nextRunAfter: string }
    expect(retryData.attempt).toBe(1)
    expect(new Date(retryData.nextRunAfter).getTime()).toBeGreaterThan(timedOut[0]!.at.getTime())

    expect(events.map((e) => e.type)).toContain('step.completed')
    expect(worker.inFlight).toBe(0)
  }, 40_000)

  test('a step whose retries are exhausted by timeouts dead-letters its run', async () => {
    const namespace = `phase3-timeout-fatal-${crypto.randomUUID()}`
    // maxAttempts is 1 here (the builder default), so the very first timeout
    // is terminal — the "no retries left" branch of the same path.
    const starts = { n: 0 }
    const doomed = defineWorkflow(`phase3-timeout-fatal-${crypto.randomUUID()}`, (builder) => {
      builder.step(
        'never-finishes',
        async (ctx) => {
          starts.n++
          await new Promise<void>((resolve) => {
            const timer = setTimeout(resolve, 30_000)
            ctx.signal.addEventListener('abort', () => {
              clearTimeout(timer)
              resolve()
            }, { once: true })
          })
          return 'unreachable'
        },
        { timeoutMs: 120 }
      )
      builder.step('downstream', async () => 'never runs', { dependsOn: ['never-finishes'] })
    })

    const { runId } = await enqueueRun(sql, doomed, { namespace })
    const worker = createWorker({
      db: sql,
      handles: [doomed],
      namespace,
      concurrency: 1,
      leaseTtlMs: 10_000,
      heartbeatIntervalMs: 200,
      pollIntervalMs: 20,
      reclaimIntervalMs: 60_000,
    })
    worker.start()

    const run = await until(
      'the doomed run to reach a terminal state',
      () => getRun(sql, runId),
      (r) => terminal(r)
    )
    await stopQuietly(worker)

    // Phase 7 #26: an exhausted fail_fast run is routed to the dead-letter
    // queue rather than left in a bare `failed`.
    expect(run!.status).toBe('dead_letter')
    expect(starts.n).toBe(1)

    const steps = await getStepsByRun(sql, runId)
    expect(stepNamed(steps, 'never-finishes').status).toBe('failed')
    // The rest of a dead run stops being claimable.
    expect(stepNamed(steps, 'downstream').status).toBe('cancelled')

    const types = (await history(runId)).map((e) => e.type)
    expect(types).toContain('step.timed_out')
    expect(types).toContain('step.failed')
    // #26: the run-level terminal event is now the dead-letter transition.
    expect(types).toContain('run.dead_lettered')
    expect(types).not.toContain('step.retry_scheduled')
  }, 40_000)
})

describe('#11 cancellation', () => {
  test('a run cancelled mid-flight is finalized by the worker that owns the step', async () => {
    const namespace = `phase3-cancel-${crypto.randomUUID()}`
    const wf = cancellableWorkflow()

    const { runId } = await enqueueRun(sql, wf.handle, { namespace })

    const worker = createWorker({
      db: sql,
      handles: [wf.handle],
      namespace,
      concurrency: 1,
      leaseTtlMs: 10_000,
      // The cancel check rides this tick — keep it short so the test doesn't
      // wait a third of a 10s TTL to observe it.
      heartbeatIntervalMs: 50,
      pollIntervalMs: 20,
      reclaimIntervalMs: 60_000,
    })
    worker.start()

    // Wait until the step is genuinely in flight under this worker's lease —
    // cancelling before the claim would prove the easy case, not this one.
    const inFlightStep = await until(
      'the long step to be running under this worker',
      async () => stepNamed(await getStepsByRun(sql, runId), 'long'),
      (s) => s.status === 'running' && s.lease_owner === worker.id
    )
    expect(inFlightStep.lease_owner).toBe(worker.id)
    expect(wf.counts.started).toBe(1)

    // The canceller only records intent. It never writes to the running step.
    const cancelled = await requestRunCancellation(sql, runId)
    expect(cancelled).toBeDefined()
    expect(cancelled!.cancel_requested_at).not.toBeNull()
    expect(cancelled!.status).toBe('running') // still running: intent, not a kill

    const run = await until(
      'the run to be finalized as cancelled',
      () => getRun(sql, runId),
      (r) => terminal(r)
    )
    await stopQuietly(worker)

    expect(run!.status).toBe('cancelled')
    expect(run!.finished_at).not.toBeNull()

    const steps = await getStepsByRun(sql, runId)
    const long = stepNamed(steps, 'long')
    expect(long.status).toBe('cancelled')
    expect(long.lease_owner).toBeNull()
    expect(long.lease_expires_at).toBeNull()
    // The pending downstream step is cancelled too — nothing left to claim.
    expect(stepNamed(steps, 'next').status).toBe('cancelled')
    expect(wf.counts.nextRan).toBe(0)

    // The step cooperated (its signal aborted) and never reached its own end.
    expect(wf.counts.aborted).toBe(1)
    expect(wf.counts.finished).toBe(0)
    // Cancellation is not a failure: no retry was scheduled, so the step was
    // not re-claimed and re-run after the run was already doomed.
    expect(wf.counts.started).toBe(1)

    const events = await history(runId)
    const stepCancelled = events.filter((e) => e.type === 'step.cancelled')
    expect(stepCancelled).toHaveLength(1)
    const cancelData = stepCancelled[0]!.data as { phase: string; workerId: string }
    // The proof that the owning worker finalized its own step rather than the
    // canceller stomping it: the history row is stamped with the lease holder.
    expect(cancelData.workerId).toBe(worker.id)
    expect(cancelData.phase).toBe('in-flight')

    const types = events.map((e) => e.type)
    expect(types).toContain('run.cancelled')
    expect(types).not.toContain('step.failed')
    expect(types).not.toContain('run.failed')
    expect(types).not.toContain('step.retry_scheduled')

    expect(worker.inFlight).toBe(0)
  }, 40_000)

  test('a step sleeping on a cancelled run is cancelled on wake instead of running', async () => {
    const namespace = `phase3-cancel-sleep-${crypto.randomUUID()}`
    const sleeper = sleeperWorkflow(700)

    const { runId } = await enqueueRun(sql, sleeper.handle, { namespace })

    const worker = createWorker({
      db: sql,
      handles: [sleeper.handle],
      namespace,
      concurrency: 1,
      leaseTtlMs: 5_000,
      heartbeatIntervalMs: 50,
      pollIntervalMs: 20,
      reclaimIntervalMs: 60_000,
    })
    worker.start()

    await until(
      'the nap step to be parked asleep',
      () => getSleepingSteps(sql, runId),
      (rows) => rows.length === 1
    )
    expect(sleeper.counts.bodyRuns).toBe(1)

    // Cancel while nothing is in flight at all — there is no worker to abort,
    // no signal to fire. The only thing standing between this run and it
    // waking up and doing more work is the post-claim cancellation check.
    const requested = await requestRunCancellation(sql, runId)
    expect(requested).toBeDefined()

    const run = await until(
      'the sleeping run to be cancelled on wake',
      () => getRun(sql, runId),
      (r) => terminal(r)
    )
    await stopQuietly(worker)

    expect(run!.status).toBe('cancelled')

    // The decisive assertion: the step woke and was claimed, but its body
    // never ran a second time.
    expect(sleeper.counts.bodyRuns).toBe(1)
    expect(sleeper.counts.afterSleepRuns).toBe(0)

    const nap = stepNamed(await getStepsByRun(sql, runId), 'nap')
    expect(nap.status).toBe('cancelled')
    expect(nap.lease_owner).toBeNull()

    const events = await history(runId)
    const cancelEvents = events.filter((e) => e.type === 'step.cancelled')
    expect(cancelEvents).toHaveLength(1)
    expect((cancelEvents[0]!.data as { phase: string }).phase).toBe('pre-run')
    expect(events.map((e) => e.type)).toContain('run.cancelled')
    expect(worker.inFlight).toBe(0)
  }, 40_000)
})
