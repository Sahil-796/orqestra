// API-level semantics of the Phase 3 cancellation surface (src/control/cancel.ts)
// against real Postgres. Deliberately no live worker here: seeding step rows
// directly lets us pin down every requested/finalized/pending combination
// (including the ones a real worker would only hit under a race), while the
// end-to-end "cancel a step in flight on a running worker" proof lives with
// the worker tests.
//
// Same isolation trick as queue.test.ts: every run gets its own random
// namespace and workflow name, so these rows can't collide with anything
// else in the shared dev database.

import { describe, expect, test, beforeAll, afterAll } from 'bun:test'
import { createDb } from '../src/store/client.ts'
import { migrate } from '../src/store/migrate.ts'
import { cancelRun, isRunCancelled, sweepCancelledRuns } from '../src/control/cancel.ts'
import {
  countStepsByStatus,
  createRun,
  getRun,
  getStepsByRun,
  insertSteps,
  insertWorkflow,
  markStepRunning,
  requestRunCancellation,
  updateRunStatus,
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

/** Seed a `queued` run with `n` independent steps, all `ready` and claimable. */
async function seedRun(n = 3): Promise<{ run: RunRow; steps: StepRow[] }> {
  const name = `cancel-test-wf-${crypto.randomUUID()}`
  const dag: WorkflowDefinition = {
    name,
    version: 1,
    steps: Array.from({ length: n }, (_, i) => ({
      name: `step-${i}`,
      dependsOn: [],
      maxAttempts: 3,
      priority: 0,
    })),
  }
  const workflow = await insertWorkflow(sql, { name, dag })
  const { run } = await createRun(sql, {
    workflowId: workflow.id,
    namespace: `cancel-test-${crypto.randomUUID()}`,
  })
  const rows: NewStep[] = dag.steps.map((s) => ({
    name: s.name,
    dependsOn: s.dependsOn,
    maxAttempts: s.maxAttempts,
    priority: s.priority,
    status: 'ready',
  }))
  const steps = await insertSteps(sql, run.id, rows)
  return { run, steps }
}

describe('cancelRun', () => {
  test('a queued run with nothing in flight is finalized synchronously', async () => {
    const { run, steps } = await seedRun(3)

    const result = await cancelRun(sql, run.id)

    expect(result.requested).toBe(true)
    expect(result.finalized).toBe(true)
    expect(result.pending).toBe(false)
    expect(result.run?.status).toBe('cancelled')
    expect(result.run?.cancel_requested_at).not.toBeNull()
    expect(result.run?.finished_at).not.toBeNull()

    const counts = await countStepsByStatus(sql, run.id)
    expect(counts.cancelled).toBe(steps.length)
  })

  test('a run with a step still running is only requested, not finalized', async () => {
    const { run, steps } = await seedRun(2)
    const first = steps[0]
    expect(first).toBeDefined()
    if (!first) return
    await markStepRunning(sql, first.id)

    const result = await cancelRun(sql, run.id)

    expect(result.requested).toBe(true)
    expect(result.pending).toBe(true)
    expect(result.finalized).toBe(false)

    // The run stays live so the owning worker can finalize it at a safe
    // point, and its running step is untouched by this path.
    const after = await getRun(sql, run.id)
    expect(after?.status).toBe('queued')
    expect(after?.cancel_requested_at).not.toBeNull()
    const stepRows = await getStepsByRun(sql, run.id)
    expect(stepRows.find((s) => s.id === first.id)?.status).toBe('running')
  })

  test('cancelling twice is idempotent; the second call reports requested: false', async () => {
    const { run } = await seedRun(2)

    const first = await cancelRun(sql, run.id)
    const second = await cancelRun(sql, run.id)

    expect(first.requested).toBe(true)
    expect(first.finalized).toBe(true)

    expect(second.requested).toBe(false)
    expect(second.finalized).toBe(false)
    expect(second.pending).toBe(false)
    expect(second.run?.status).toBe('cancelled')

    // The first ask is the one we report latency against — a repeat must not
    // move the timestamp or the terminal transition.
    expect(second.run?.cancel_requested_at?.getTime()).toBe(
      first.run?.cancel_requested_at?.getTime()
    )
    expect(second.run?.finished_at?.getTime()).toBe(first.run?.finished_at?.getTime())
  })

  test('cancelling a completed run neither resurrects nor mutates it', async () => {
    const { run, steps } = await seedRun(1)
    await updateRunStatus(sql, run.id, 'completed', {
      output: { done: true },
      finishedAt: new Date(),
    })
    const before = await getRun(sql, run.id)

    const result = await cancelRun(sql, run.id)

    expect(result.requested).toBe(false)
    expect(result.finalized).toBe(false)
    expect(result.pending).toBe(false)
    expect(result.run?.status).toBe('completed')

    const after = await getRun(sql, run.id)
    expect(after?.status).toBe('completed')
    expect(after?.cancel_requested_at).toBeNull()
    expect(after?.finished_at?.getTime()).toBe(before?.finished_at?.getTime())
    expect(after?.output).toEqual({ done: true })

    // Its steps are equally off-limits — a terminal run's history is history.
    const stepRows = await getStepsByRun(sql, run.id)
    expect(stepRows.map((s) => s.status)).toEqual(steps.map(() => 'ready'))
  })
})

describe('isRunCancelled', () => {
  test('tracks the request, not the terminal status', async () => {
    const { run, steps } = await seedRun(1)
    expect(await isRunCancelled(sql, run.id)).toBe(false)

    // Requested but not finalizable — a step is in flight, so the run is
    // still `queued`. isRunCancelled must already say true here.
    const first = steps[0]
    expect(first).toBeDefined()
    if (!first) return
    await markStepRunning(sql, first.id)
    await cancelRun(sql, run.id)

    expect(await getRun(sql, run.id).then((r) => r?.status)).toBe('queued')
    expect(await isRunCancelled(sql, run.id)).toBe(true)
  })

  test('is false for an unknown run id', async () => {
    expect(await isRunCancelled(sql, crypto.randomUUID())).toBe(false)
  })
})

describe('sweepCancelledRuns', () => {
  test('finalizes an orphaned request whose worker never came back', async () => {
    // Simulates the crash window: the request landed, then the process that
    // was going to act on it died, leaving nothing running and a run that
    // would otherwise sit `queued` with cancel_requested_at set forever.
    const { run } = await seedRun(2)
    await requestRunCancellation(sql, run.id)
    expect((await getRun(sql, run.id))?.status).toBe('queued')

    const swept = await sweepCancelledRuns(sql)

    expect(swept).toContain(run.id)
    expect((await getRun(sql, run.id))?.status).toBe('cancelled')
    const counts = await countStepsByStatus(sql, run.id)
    expect(counts.cancelled).toBe(2)
  })

  test('leaves a run alone while one of its steps is still running', async () => {
    const { run, steps } = await seedRun(2)
    const first = steps[0]
    expect(first).toBeDefined()
    if (!first) return
    await markStepRunning(sql, first.id)
    await requestRunCancellation(sql, run.id)

    const swept = await sweepCancelledRuns(sql)

    expect(swept).not.toContain(run.id)
    expect((await getRun(sql, run.id))?.status).toBe('queued')
  })

  test('is a no-op on a second pass — nothing left to finalize', async () => {
    const { run } = await seedRun(1)
    await requestRunCancellation(sql, run.id)

    await sweepCancelledRuns(sql)
    const second = await sweepCancelledRuns(sql)

    expect(second).not.toContain(run.id)
    expect((await getRun(sql, run.id))?.status).toBe('cancelled')
  })
})
