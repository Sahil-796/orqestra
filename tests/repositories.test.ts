import { describe, expect, test, beforeAll, afterAll } from 'bun:test'
import { createDb } from '../src/store/client.ts'
import { migrate } from '../src/store/migrate.ts'
import {
  insertWorkflow,
  createRun,
  getRun,
  insertSteps,
  getStepsByRun,
  claimNextStep,
  sleepStep,
  clearSleepMarker,
  requestRunCancellation,
  isCancellationRequested,
  finalizeCancelledRun,
  cancelRunningStep,
  getCancelRequestedRuns,
  getSleepingSteps,
  type NewStep,
  type RunRow,
  type StepRow,
} from '../src/store/repositories.ts'
import type { WorkflowDefinition } from '../src/types.ts'

const sql = createDb()

beforeAll(async () => {
  await migrate(sql)
})

afterAll(async () => {
  await sql.end()
})

describe('repositories', () => {
  test('create a run and read it back', async () => {
    const dag: WorkflowDefinition = {
      name: `repo-round-trip-test-${crypto.randomUUID()}`,
      version: 1,
      steps: [{ name: 'only', dependsOn: [], maxAttempts: 1, priority: 0 }],
    }
    const workflow = await insertWorkflow(sql, { name: dag.name, dag })

    const { run: created, created: wasCreated } = await createRun(sql, {
      workflowId: workflow.id,
      input: { hello: 'world' },
    })
    expect(wasCreated).toBe(true)
    expect(created.status).toBe('queued')
    expect(created.namespace).toBe('default')

    const fetched = await getRun(sql, created.id)
    expect(fetched).toBeDefined()
    expect(fetched?.id).toBe(created.id)
    expect(fetched?.input).toEqual({ hello: 'world' })
    expect(fetched?.workflow_id).toBe(workflow.id)
  })
})

// ---- Phase 3: execution control -------------------------------------------
//
// Every seeded run gets its own random namespace and every claim passes it,
// for the same reason tests/queue.test.ts does: the step queue is global, so
// namespacing is what keeps these rows from competing with leftovers of
// other test files against the same long-lived dev Postgres.

interface Seed {
  run: RunRow
  steps: StepRow[]
  namespace: string
}

async function seedRun(specs: Partial<NewStep>[]): Promise<Seed> {
  const namespace = `exec-control-test-${crypto.randomUUID()}`
  const name = `exec-control-wf-${crypto.randomUUID()}`
  const dag: WorkflowDefinition = {
    name,
    version: 1,
    steps: specs.map((spec, i) => ({
      name: spec.name ?? `step-${i}`,
      dependsOn: spec.dependsOn ?? [],
      maxAttempts: spec.maxAttempts ?? 1,
      timeoutMs: spec.timeoutMs,
      priority: spec.priority ?? 0,
    })),
  }
  const workflow = await insertWorkflow(sql, { name, dag })
  const { run } = await createRun(sql, { workflowId: workflow.id, namespace })
  const steps = await insertSteps(
    sql,
    run.id,
    dag.steps.map((s, i) => ({
      name: s.name,
      dependsOn: s.dependsOn,
      maxAttempts: s.maxAttempts,
      timeoutMs: s.timeoutMs,
      priority: s.priority,
      status: specs[i]?.status ?? 'ready',
    }))
  )
  return { run, steps, namespace }
}

async function claim(namespace: string, workerId: string): Promise<StepRow> {
  const claimed = await claimNextStep(sql, { workerId, leaseTtlMs: 60_000, namespace })
  if (!claimed) throw new Error('claim: expected a claimable step')
  return claimed
}

describe('insertSteps: timeout_ms plumbing', () => {
  // Feature #10 is dead on arrival if the column is never populated, so
  // assert the definition -> NewStep -> column path directly.
  test('a step definition timeoutMs lands in step.timeout_ms', async () => {
    const { run } = await seedRun([{ name: 'with-timeout', timeoutMs: 1500 }, { name: 'without' }])
    const steps = await getStepsByRun(sql, run.id)
    const withTimeout = steps.find((s) => s.name === 'with-timeout')
    const without = steps.find((s) => s.name === 'without')
    expect(withTimeout?.timeout_ms).toBe(1500)
    expect(without?.timeout_ms).toBeNull()
  })
})

describe('sleepStep', () => {
  test('suspends a claimed step: ready, future run_after, sleep_seq bumped, lease released', async () => {
    const { steps, namespace } = await seedRun([{}])
    const claimed = await claim(namespace, 'w1')
    expect(claimed.status).toBe('running')
    expect(claimed.sleep_seq).toBe(0)

    const wakeAt = new Date(Date.now() + 60_000)
    const slept = await sleepStep(sql, { stepId: claimed.id, workerId: 'w1', wakeAt })
    expect(slept).toBeDefined()
    expect(slept?.status).toBe('ready')
    expect(slept?.sleep_seq).toBe(1)
    expect(slept?.lease_owner).toBeNull()
    expect(slept?.lease_expires_at).toBeNull()
    expect(slept?.run_after.getTime()).toBe(wakeAt.getTime())
    expect(slept?.sleeping_until?.getTime()).toBe(wakeAt.getTime())
    expect(slept?.id).toBe(steps[0]!.id)
  })

  test('a sleeping step is not claimable until its wake time', async () => {
    const { namespace } = await seedRun([{}])
    const claimed = await claim(namespace, 'w1')
    await sleepStep(sql, {
      stepId: claimed.id,
      workerId: 'w1',
      wakeAt: new Date(Date.now() + 60_000),
    })

    expect(
      await claimNextStep(sql, { workerId: 'w2', leaseTtlMs: 60_000, namespace })
    ).toBeUndefined()

    // ...and is claimable again the moment the wake time has passed.
    await sql`update step set run_after = now() - interval '1 second' where id = ${claimed.id}`
    const woken = await claimNextStep(sql, { workerId: 'w2', leaseTtlMs: 60_000, namespace })
    expect(woken?.id).toBe(claimed.id)
  })

  test('does not burn a retry attempt: sleeping and waking leaves attempt where it started', async () => {
    // maxAttempts: 1 — if a sleep consumed the attempt the step could never
    // finish after waking, which is the whole point of the decrement.
    const { namespace } = await seedRun([{ maxAttempts: 1 }])
    const first = await claim(namespace, 'w1')
    expect(first.attempt).toBe(1)

    const slept = await sleepStep(sql, {
      stepId: first.id,
      workerId: 'w1',
      wakeAt: new Date(Date.now() - 1000),
    })
    expect(slept?.attempt).toBe(0)

    const woken = await claim(namespace, 'w2')
    expect(woken.id).toBe(first.id)
    expect(woken.attempt).toBe(1)
    expect(woken.attempt).toBeLessThanOrEqual(woken.max_attempts)
    expect(woken.sleep_seq).toBe(1)
  })

  test('sleep_seq accumulates across successive sleeps', async () => {
    const { namespace } = await seedRun([{ maxAttempts: 5 }])
    const step = await claim(namespace, 'w1')
    for (let i = 1; i <= 3; i++) {
      const slept = await sleepStep(sql, {
        stepId: step.id,
        workerId: 'w1',
        wakeAt: new Date(Date.now() - 1000),
      })
      expect(slept?.sleep_seq).toBe(i)
      await claim(namespace, 'w1')
    }
  })

  test('fence: a worker that does not hold the lease cannot suspend the step', async () => {
    const { namespace } = await seedRun([{}])
    const claimed = await claim(namespace, 'owner')

    const stolen = await sleepStep(sql, {
      stepId: claimed.id,
      workerId: 'impostor',
      wakeAt: new Date(Date.now() + 60_000),
    })
    expect(stolen).toBeUndefined()

    const steps = await getStepsByRun(sql, claimed.run_id)
    expect(steps[0]?.status).toBe('running')
    expect(steps[0]?.lease_owner).toBe('owner')
    expect(steps[0]?.sleep_seq).toBe(0)
  })
})

describe('clearSleepMarker / getSleepingSteps', () => {
  test('sleeping steps are listed while asleep and unmarked once running again', async () => {
    const { run, namespace } = await seedRun([{}])
    const claimed = await claim(namespace, 'w1')
    await sleepStep(sql, {
      stepId: claimed.id,
      workerId: 'w1',
      wakeAt: new Date(Date.now() + 60_000),
    })

    const sleeping = await getSleepingSteps(sql, run.id)
    expect(sleeping.map((s) => s.id)).toEqual([claimed.id])

    await clearSleepMarker(sql, claimed.id)
    expect(await getSleepingSteps(sql, run.id)).toEqual([])
    const steps = await getStepsByRun(sql, run.id)
    expect(steps[0]?.sleeping_until).toBeNull()
  })

  test('a step in retry backoff is not reported as sleeping', async () => {
    const { run, namespace } = await seedRun([{}])
    const claimed = await claim(namespace, 'w1')
    // retry backoff shape: ready + future run_after, but no sleeping_until.
    await sql`
      update step set status = 'ready', run_after = now() + interval '1 minute',
        lease_owner = null, lease_expires_at = null
      where id = ${claimed.id}
    `
    expect(await getSleepingSteps(sql, run.id)).toEqual([])
  })
})

describe('cancellation', () => {
  test('requestRunCancellation records intent once and is visible to pollers', async () => {
    const { run } = await seedRun([{}])
    expect(await isCancellationRequested(sql, run.id)).toBe(false)

    const requested = await requestRunCancellation(sql, run.id)
    expect(requested?.cancel_requested_at).toBeInstanceOf(Date)
    expect(await isCancellationRequested(sql, run.id)).toBe(true)

    // second request is a no-op and must not move the timestamp
    expect(await requestRunCancellation(sql, run.id)).toBeUndefined()
    const after = await getRun(sql, run.id)
    expect(after?.cancel_requested_at?.getTime()).toBe(requested!.cancel_requested_at!.getTime())
  })

  test('requestRunCancellation is a no-op on a terminal run', async () => {
    const { run } = await seedRun([{}])
    await sql`update run set status = 'completed', finished_at = now() where id = ${run.id}`
    expect(await requestRunCancellation(sql, run.id)).toBeUndefined()
    expect(await isCancellationRequested(sql, run.id)).toBe(false)
  })

  test('getCancelRequestedRuns returns pending requests and drops finalized ones', async () => {
    const { run } = await seedRun([{}])
    await requestRunCancellation(sql, run.id)
    const pending = await getCancelRequestedRuns(sql)
    expect(pending.map((r) => r.id)).toContain(run.id)

    await finalizeCancelledRun(sql, run.id)
    const after = await getCancelRequestedRuns(sql)
    expect(after.map((r) => r.id)).not.toContain(run.id)
  })

  test('finalizeCancelledRun cancels pending/ready steps and leaves a running step untouched', async () => {
    const { run, namespace } = await seedRun([
      { name: 'a' },
      { name: 'b' },
      { name: 'c', status: 'pending' },
    ])
    const running = await claim(namespace, 'owner')

    await requestRunCancellation(sql, run.id)
    const { run: cancelled, cancelledSteps } = await finalizeCancelledRun(sql, run.id)
    expect(cancelled?.status).toBe('cancelled')
    expect(cancelled?.finished_at).toBeInstanceOf(Date)

    // the in-flight step is its worker's problem, not ours
    expect(cancelledSteps.map((s) => s.id)).not.toContain(running.id)
    expect(cancelledSteps).toHaveLength(2)

    const steps = await getStepsByRun(sql, run.id)
    const stillRunning = steps.find((s) => s.id === running.id)
    expect(stillRunning?.status).toBe('running')
    expect(stillRunning?.lease_owner).toBe('owner')
    for (const step of steps.filter((s) => s.id !== running.id)) {
      expect(step.status).toBe('cancelled')
    }
  })

  test('finalizeCancelledRun is a no-op on an already-terminal run', async () => {
    const { run } = await seedRun([{}])
    await sql`update run set status = 'failed', finished_at = now() where id = ${run.id}`
    const result = await finalizeCancelledRun(sql, run.id)
    expect(result.run).toBeUndefined()
    expect(result.cancelledSteps).toEqual([])
    const steps = await getStepsByRun(sql, run.id)
    expect(steps[0]?.status).toBe('ready')
  })

  test('cancelRunningStep finalizes the owning worker own step and releases the lease', async () => {
    const { namespace } = await seedRun([{}])
    const claimed = await claim(namespace, 'owner')
    const cancelled = await cancelRunningStep(sql, { stepId: claimed.id, workerId: 'owner' })
    expect(cancelled?.status).toBe('cancelled')
    expect(cancelled?.lease_owner).toBeNull()
    expect(cancelled?.lease_expires_at).toBeNull()
  })

  test('fence: cancelRunningStep rejects a worker that does not own the lease', async () => {
    const { namespace } = await seedRun([{}])
    const claimed = await claim(namespace, 'owner')
    expect(
      await cancelRunningStep(sql, { stepId: claimed.id, workerId: 'impostor' })
    ).toBeUndefined()

    const steps = await getStepsByRun(sql, claimed.run_id)
    expect(steps[0]?.status).toBe('running')
    expect(steps[0]?.lease_owner).toBe('owner')
  })
})
