import { describe, expect, test, beforeAll, afterAll } from 'bun:test'
import { createDb } from '../src/store/client.ts'
import { migrate } from '../src/store/migrate.ts'
import {
  insertWorkflow,
  createRun,
  insertSteps,
  updateRunStatus,
  failStep,
  insertHistory,
  listRuns,
  getRunTimeline,
  getRunLogs,
  getRunMetrics,
  getRunErrors,
  upsertWorkerHeartbeat,
  listWorkerHealth,
  getQueueDepth,
  getThroughput,
  type NewStep,
  type RunRow,
  type StepRow,
} from '../src/store/repositories.ts'
import { serializeError, type WorkflowDefinition } from '../src/types.ts'

const sql = createDb()

beforeAll(async () => {
  await migrate(sql)
})

afterAll(async () => {
  await sql.end()
})

// Every seeded run gets its own random namespace/workflow name so these rows
// never compete with leftovers of other test files against the shared dev
// Postgres — same discipline as repositories.test.ts / failure-store.test.ts.
interface Seed {
  workflowName: string
  run: RunRow
  steps: StepRow[]
}

async function seedRun(
  specs: Partial<NewStep>[],
  opts: { namespace?: string } = {}
): Promise<Seed> {
  const name = `observability-store-wf-${crypto.randomUUID()}`
  const dag: WorkflowDefinition = {
    name,
    version: 1,
    steps: specs.map((spec, i) => ({
      name: spec.name ?? `step-${i}`,
      dependsOn: spec.dependsOn ?? [],
      maxAttempts: spec.maxAttempts ?? 1,
      priority: spec.priority ?? 0,
    })),
  }
  const workflow = await insertWorkflow(sql, { name, dag })
  const { run } = await createRun(sql, { workflowId: workflow.id, namespace: opts.namespace })
  const steps = await insertSteps(
    sql,
    run.id,
    dag.steps.map((s, i) => ({
      name: s.name,
      dependsOn: s.dependsOn,
      maxAttempts: s.maxAttempts,
      priority: s.priority,
      status: specs[i]?.status ?? 'ready',
    }))
  )
  return { workflowName: name, run, steps }
}

// Directly stamp a step's created_at/updated_at so duration-metrics tests can
// assert on exact numbers instead of racing the clock.
async function stampStep(stepId: string, createdAt: Date, updatedAt: Date): Promise<void> {
  await sql`
    update step set created_at = ${createdAt}, updated_at = ${updatedAt}
    where id = ${stepId}
  `
}

async function stampRun(runId: string, createdAt: Date, startedAt: Date, finishedAt: Date): Promise<void> {
  await sql`
    update run set created_at = ${createdAt}, started_at = ${startedAt}, finished_at = ${finishedAt}
    where id = ${runId}
  `
}

describe('listRuns', () => {
  test('filters by status/workflow/namespace and computes durationMs', async () => {
    const { workflowName, run, steps } = await seedRun([{ name: 'a' }])
    const step = steps[0]!
    const startedAt = new Date('2026-01-01T00:00:00Z')
    const finishedAt = new Date('2026-01-01T00:00:05Z')
    await sql`update step set status = 'completed' where id = ${step.id}`
    await stampRun(run.id, new Date('2025-12-31T23:59:00Z'), startedAt, finishedAt)
    await updateRunStatus(sql, run.id, 'completed')

    const byWorkflow = await listRuns(sql, { workflowName })
    expect(byWorkflow).toHaveLength(1)
    expect(byWorkflow[0]?.id).toBe(run.id)
    expect(byWorkflow[0]?.workflowName).toBe(workflowName)
    expect(byWorkflow[0]?.durationMs).toBe(5000)

    const byStatus = await listRuns(sql, { workflowName, status: 'completed' })
    expect(byStatus).toHaveLength(1)
    const byWrongStatus = await listRuns(sql, { workflowName, status: 'failed' })
    expect(byWrongStatus).toHaveLength(0)

    const byNamespace = await listRuns(sql, { namespace: 'default', workflowName })
    expect(byNamespace).toHaveLength(1)
    const byWrongNamespace = await listRuns(sql, { namespace: 'nope-does-not-exist', workflowName })
    expect(byWrongNamespace).toHaveLength(0)
  })

  test('durationMs is null when the run has not finished', async () => {
    const { workflowName } = await seedRun([{ name: 'a' }])
    const rows = await listRuns(sql, { workflowName })
    expect(rows).toHaveLength(1)
    expect(rows[0]?.durationMs).toBeNull()
  })

  test('respects limit/offset and orders newest-first', async () => {
    const { workflowName, run: run1 } = await seedRun([{ name: 'a' }])
    // second run under the same workflow name+version would collide on
    // (name, version) unique — reuse seedRun's own random name per call
    // instead by seeding a second, independent run tagged the same way via
    // namespace so we can filter both together.
    const ns = `obs-order-${crypto.randomUUID()}`
    const { run: r1 } = await seedRun([{ name: 'a' }], { namespace: ns })
    await stampRun(r1.id, new Date('2026-01-01T00:00:00Z'), new Date('2026-01-01T00:00:00Z'), new Date('2026-01-01T00:00:01Z'))
    const { run: r2 } = await seedRun([{ name: 'a' }], { namespace: ns })
    await stampRun(r2.id, new Date('2026-01-02T00:00:00Z'), new Date('2026-01-02T00:00:00Z'), new Date('2026-01-02T00:00:01Z'))

    const all = await listRuns(sql, { namespace: ns, limit: 10 })
    expect(all.map((r) => r.id)).toEqual([r2.id, r1.id])

    const paged = await listRuns(sql, { namespace: ns, limit: 1, offset: 1 })
    expect(paged.map((r) => r.id)).toEqual([r1.id])

    expect(run1).toBeDefined() // silence unused-var lint on the throwaway seed
  })
})

describe('getRunTimeline / getRunLogs', () => {
  test('timeline returns every history row for a run in order; logs filters to type=log', async () => {
    const { run } = await seedRun([{ name: 'a' }])
    await insertHistory(sql, { runId: run.id, type: 'run.created' })
    await insertHistory(sql, { runId: run.id, type: 'log', data: { level: 'info', msg: 'first' } })
    await insertHistory(sql, { runId: run.id, type: 'log', data: { level: 'error', msg: 'second' } })
    await insertHistory(sql, { runId: run.id, type: 'run.completed' })

    const timeline = await getRunTimeline(sql, run.id)
    // run.created (from seedRun's own bookkeeping is absent here — insertSteps
    // does not log history) plus the 4 rows inserted above.
    expect(timeline.map((h) => h.type)).toEqual(['run.created', 'log', 'log', 'run.completed'])

    const logs = await getRunLogs(sql, run.id)
    expect(logs).toHaveLength(2)
    expect(logs.every((l) => l.type === 'log')).toBe(true)
    expect((logs[0]?.data as { msg: string }).msg).toBe('first')
    expect((logs[1]?.data as { msg: string }).msg).toBe('second')
  })

  test('an unknown run has an empty timeline and empty logs', async () => {
    const fakeId = crypto.randomUUID()
    expect(await getRunTimeline(sql, fakeId)).toEqual([])
    expect(await getRunLogs(sql, fakeId)).toEqual([])
  })
})

describe('getRunMetrics', () => {
  test('aggregates status counts, step duration percentiles, and queue wait', async () => {
    const { workflowName, run, steps } = await seedRun([
      { name: 'a' },
      { name: 'b' },
      { name: 'c' },
      { name: 'd' },
    ])
    const byName = (n: string) => steps.find((s) => s.name === n)!

    // Known step durations (updated_at - created_at): 100ms, 200ms, 300ms, 400ms.
    const base = new Date('2026-02-01T00:00:00.000Z')
    const durations = [100, 200, 300, 400]
    for (const [i, name] of ['a', 'b', 'c', 'd'].entries()) {
      const step = byName(name)
      await sql`update step set status = ${i === 3 ? 'failed' : 'completed'} where id = ${step.id}`
      await stampStep(step.id, base, new Date(base.getTime() + durations[i]!))
    }

    // Run-level queue wait: started 500ms after created.
    const createdAt = new Date('2026-02-01T00:00:00.000Z')
    const startedAt = new Date('2026-02-01T00:00:00.500Z')
    const finishedAt = new Date('2026-02-01T00:00:01.000Z')
    await stampRun(run.id, createdAt, startedAt, finishedAt)
    await updateRunStatus(sql, run.id, 'completed_with_errors')

    const [metrics] = await getRunMetrics(sql, { workflowName })
    expect(metrics).toBeDefined()
    expect(metrics?.runCount).toBe(1)
    expect(metrics?.statusCounts.completed_with_errors).toBe(1)
    expect(metrics?.avgRunQueueWaitMs).toBeCloseTo(500, 0)
    // avg of [100, 200, 300] (only 'completed'/'failed' steps count — all 4 do here)
    expect(metrics?.avgStepDurationMs).toBeCloseTo(250, 0)
    expect(metrics?.p50StepDurationMs).toBeGreaterThan(0)
    expect(metrics?.p95StepDurationMs).toBeGreaterThanOrEqual(metrics?.p50StepDurationMs ?? 0)
    expect(metrics?.totalAttempts).toBeGreaterThanOrEqual(0)
  })

  test('groupByWorkflow returns one row per workflow', async () => {
    const ns = `obs-metrics-group-${crypto.randomUUID()}`
    const { workflowName: wf1, run: r1, steps: s1 } = await seedRun([{ name: 'a' }], { namespace: ns })
    const { workflowName: wf2, run: r2, steps: s2 } = await seedRun([{ name: 'a' }], { namespace: ns })
    await sql`update step set status = 'completed' where id = ${s1[0]!.id}`
    await sql`update step set status = 'completed' where id = ${s2[0]!.id}`
    await updateRunStatus(sql, r1.id, 'completed')
    await updateRunStatus(sql, r2.id, 'completed')

    const grouped = await getRunMetrics(sql, { namespace: ns, groupByWorkflow: true })
    const names = grouped.map((g) => g.workflowName).sort()
    expect(names).toEqual([wf1, wf2].sort())
    for (const g of grouped) expect(g.runCount).toBe(1)
  })

  test('returns a single zeroed row when nothing matches (ungrouped)', async () => {
    const rows = await getRunMetrics(sql, { workflowName: `no-such-workflow-${crypto.randomUUID()}` })
    expect(rows).toHaveLength(1)
    expect(rows[0]?.runCount).toBe(0)
    expect(rows[0]?.workflowName).toBeNull()
  })
})

describe('getRunErrors', () => {
  test('returns failed steps with their persisted error', async () => {
    const { run, steps } = await seedRun([{ name: 'a' }, { name: 'b' }])
    const a = steps.find((s) => s.name === 'a')!
    await failStep(sql, a.id, serializeError(new Error('boom')))

    const errors = await getRunErrors(sql, run.id)
    expect(errors).toHaveLength(1)
    expect(errors[0]?.name).toBe('a')
    expect(errors[0]?.error).toMatchObject({ message: 'boom', name: 'Error' })
  })

  test('a run with no failed steps returns an empty list', async () => {
    const { run } = await seedRun([{ name: 'a' }])
    expect(await getRunErrors(sql, run.id)).toEqual([])
  })
})

describe('worker health', () => {
  test('upsertWorkerHeartbeat inserts then updates in place; listWorkerHealth reports alive/stale', async () => {
    const workerId = `worker-${crypto.randomUUID()}`

    const first = await upsertWorkerHeartbeat(sql, {
      workerId,
      hostname: 'host-a',
      leasedSteps: 2,
      concurrency: 5,
    })
    expect(first.worker_id).toBe(workerId)
    expect(first.leased_steps).toBe(2)
    expect(first.status).toBe('running')
    const startedAt = first.started_at

    const second = await upsertWorkerHeartbeat(sql, {
      workerId,
      hostname: 'host-a',
      leasedSteps: 3,
      concurrency: 5,
      status: 'draining',
    })
    expect(second.leased_steps).toBe(3)
    expect(second.status).toBe('draining')
    // started_at is set once and never moved by later heartbeats
    expect(second.started_at.getTime()).toBe(startedAt.getTime())

    const alive = await listWorkerHealth(sql, 60_000)
    const mine = alive.find((w) => w.workerId === workerId)
    expect(mine).toBeDefined()
    expect(mine?.alive).toBe(true)
    expect(mine?.leasedSteps).toBe(3)

    // Backdate the heartbeat to simulate a worker that has gone silent, then
    // check it's reported stale under a short threshold.
    await sql`update worker_health set last_heartbeat_at = now() - interval '1 hour' where worker_id = ${workerId}`
    const stale = await listWorkerHealth(sql, 1_000)
    const mineStale = stale.find((w) => w.workerId === workerId)
    expect(mineStale?.alive).toBe(false)
  })
})

describe('getQueueDepth', () => {
  test('counts steps by status across the whole table', async () => {
    const before = await getQueueDepth(sql)
    const { steps } = await seedRun([{ name: 'a' }, { name: 'b', status: 'pending', dependsOn: ['a'] }])
    const after = await getQueueDepth(sql)
    // Seeding adds one 'ready' and one 'pending' step; assert the counts moved
    // by exactly that much rather than asserting absolute totals (the table is
    // shared across this whole test file).
    expect((after.ready ?? 0) - (before.ready ?? 0)).toBe(1)
    expect((after.pending ?? 0) - (before.pending ?? 0)).toBe(1)
    expect(steps).toHaveLength(2)
  })
})

describe('getThroughput', () => {
  test('buckets completed/failed run counts by finished_at', async () => {
    const ns = `obs-throughput-${crypto.randomUUID()}`
    const { run: r1 } = await seedRun([{ name: 'a' }], { namespace: ns })
    const { run: r2 } = await seedRun([{ name: 'a' }], { namespace: ns })
    const { run: r3 } = await seedRun([{ name: 'a' }], { namespace: ns })

    const now = new Date()
    const bucketMs = 60_000
    // r1 and r2 land in the same recent bucket; r3 is failed.
    await stampRun(r1.id, now, now, now)
    await stampRun(r2.id, now, now, new Date(now.getTime() + 100))
    await stampRun(r3.id, now, now, now)
    await updateRunStatus(sql, r1.id, 'completed')
    await updateRunStatus(sql, r2.id, 'completed')
    await updateRunStatus(sql, r3.id, 'failed')

    const buckets = await getThroughput(sql, 5 * 60_000, bucketMs)
    const totalCompleted = buckets.reduce((sum, b) => sum + b.completed, 0)
    const totalFailed = buckets.reduce((sum, b) => sum + b.failed, 0)
    expect(totalCompleted).toBeGreaterThanOrEqual(2)
    expect(totalFailed).toBeGreaterThanOrEqual(1)
  })

  test('runs finished outside the window are excluded', async () => {
    const ns = `obs-throughput-old-${crypto.randomUUID()}`
    const { run } = await seedRun([{ name: 'a' }], { namespace: ns })
    // Finished 10 days ago — safely outside any short window regardless of
    // clock skew between the test process and the DB server.
    const longAgo = new Date(Date.now() - 10 * 24 * 60 * 60 * 1000)
    await stampRun(run.id, longAgo, longAgo, longAgo)
    await updateRunStatus(sql, run.id, 'completed')

    const buckets = await getThroughput(sql, 60_000)
    const found = buckets.some((b) => Math.abs(b.bucketStart.getTime() - longAgo.getTime()) < 60_000)
    expect(found).toBe(false)
  })
})
