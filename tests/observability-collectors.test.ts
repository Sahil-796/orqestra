// Phase 8, collection side (#31 structured logs, #32 support, #34 write side).
// This suite owns src/observability/**'s new persisting-logger surface and
// worker.ts's heartbeat/log wiring — it does not touch the read-model tests
// in tests/observability.test.ts (Wave 1, already committed).

import { describe, expect, test, beforeAll, afterAll } from 'bun:test'
import { createDb } from '../src/store/client.ts'
import { migrate } from '../src/store/migrate.ts'
import { defineWorkflow } from '../src/define/workflow.ts'
import { enqueueRun } from '../src/engine/executor.ts'
import { createWorker } from '../src/worker/worker.ts'
import {
  insertWorkflow,
  createRun,
  insertSteps,
  getRun,
  getRunLogs,
  listWorkerHealth,
  upsertWorkerHeartbeat,
} from '../src/store/repositories.ts'
import { createLogger, createRunLogger } from '../src/observability/logger.ts'
import type { WorkflowDefinition } from '../src/types.ts'

const sql = createDb()

beforeAll(async () => {
  await migrate(sql)
})

afterAll(async () => {
  await sql.end()
})

async function makeRun() {
  const dag: WorkflowDefinition = {
    name: `observability-collectors-test-${crypto.randomUUID()}`,
    version: 1,
    steps: [{ name: 'only', dependsOn: [], maxAttempts: 1, priority: 0 }],
  }
  const workflow = await insertWorkflow(sql, { name: dag.name, dag })
  const { run } = await createRun(sql, { workflowId: workflow.id, input: {} })
  await insertSteps(sql, run.id, [
    { name: 'only', dependsOn: [], maxAttempts: 1, priority: 0, status: 'ready' },
  ])
  return run
}

describe('createRunLogger (#31)', () => {
  test('persists a log call as a retrievable type:"log" history row', async () => {
    const run = await makeRun()
    const runLog = createRunLogger(sql, run.id, { component: 'test' })

    runLog.info('hello from the collector', { widget: 42 })

    // Persistence is fire-and-forget — give the insert a beat to land before
    // reading it back.
    await new Promise((r) => setTimeout(r, 100))

    const logs = await getRunLogs(sql, run.id)
    expect(logs.length).toBe(1)
    const row = logs[0]!
    expect(row.type).toBe('log')
    expect(row.run_id).toBe(run.id)
    const data = row.data as Record<string, unknown>
    expect(data.msg).toBe('hello from the collector')
    expect(data.level).toBe('info')
    expect(data.component).toBe('test')
    expect(data.widget).toBe(42)
  })

  test('child() carries base fields (including stepId) into persisted rows', async () => {
    const run = await makeRun()
    const steps = await insertSteps(sql, run.id, [
      { name: 'child-target', dependsOn: [], maxAttempts: 1, priority: 0, status: 'ready' },
    ])
    const stepId = steps[0]!.id
    const base = createRunLogger(sql, run.id, { workerId: 'w-1' })
    const child = base.child({ stepId })

    child.warn('step-scoped warning')
    await new Promise((r) => setTimeout(r, 100))

    const logs = await getRunLogs(sql, run.id)
    expect(logs.length).toBe(1)
    const row = logs[0]!
    expect(row.step_id).toBe(stepId)
    const data = row.data as Record<string, unknown>
    expect(data.level).toBe('warn')
    expect(data.workerId).toBe('w-1')
  })

  test('a storage failure while persisting does not throw or crash the caller', async () => {
    // A run id that satisfies the type but has no matching row: insertHistory's
    // FK constraint will reject the insert, and that rejection must be
    // swallowed (best-effort), not surfaced to the caller.
    const bogusRunId = crypto.randomUUID()
    const runLog = createRunLogger(sql, bogusRunId)

    expect(() => runLog.error('this will fail to persist')).not.toThrow()

    // Give the failed insert's rejection handler a chance to run so it
    // doesn't show up as an unhandled rejection later.
    await new Promise((r) => setTimeout(r, 100))
  })

  test('the plain stdout logger (createLogger) is untouched by the persisting wrapper', () => {
    const log = createLogger('debug')
    expect(() => log.info('plain log, no db involved')).not.toThrow()
  })
})

describe('worker heartbeat (#34 write side)', () => {
  test('upsertWorkerHeartbeat directly inserts and updates a worker_health row', async () => {
    const workerId = `collectors-test-${crypto.randomUUID()}`

    await upsertWorkerHeartbeat(sql, {
      workerId,
      hostname: 'test-host',
      status: 'running',
      leasedSteps: 0,
      concurrency: 4,
    })

    let health = await listWorkerHealth(sql)
    let mine = health.find((w) => w.workerId === workerId)
    expect(mine).toBeDefined()
    expect(mine!.status).toBe('running')
    expect(mine!.concurrency).toBe(4)
    const firstHeartbeat = mine!.lastHeartbeatAt

    await new Promise((r) => setTimeout(r, 10))
    await upsertWorkerHeartbeat(sql, {
      workerId,
      hostname: 'test-host',
      status: 'draining',
      leasedSteps: 2,
      concurrency: 4,
    })

    health = await listWorkerHealth(sql)
    mine = health.find((w) => w.workerId === workerId)
    expect(mine).toBeDefined()
    expect(mine!.status).toBe('draining')
    expect(mine!.leasedSteps).toBe(2)
    // started_at-style semantics: same row, heartbeat timestamp advances.
    expect(mine!.lastHeartbeatAt.getTime()).toBeGreaterThanOrEqual(firstHeartbeat.getTime())
  })

  test('a running worker writes and updates its own heartbeat row while draining a run', async () => {
    const namespace = `collectors-test-${crypto.randomUUID()}`
    const wf = defineWorkflow(`observability-collectors-worker-${crypto.randomUUID()}`, (builder) => {
      builder.step('only', async () => 'done')
    })

    const { runId } = await enqueueRun(sql, wf, { namespace })

    const worker = createWorker({
      db: sql,
      handles: [wf],
      namespace,
      pollIntervalMs: 20,
      heartbeatIntervalMs: 20,
      reclaimIntervalMs: 60_000, // not exercising reclaim here (see worker.test.ts)
    })
    worker.start()

    // Wait for the run to drain and at least one heartbeat tick to land.
    const deadline = Date.now() + 10_000
    let run = await getRun(sql, runId)
    while (run && run.status !== 'completed' && Date.now() < deadline) {
      await new Promise((r) => setTimeout(r, 25))
      run = await getRun(sql, runId)
    }
    expect(run?.status).toBe('completed')

    await worker.stop()
    // Step-completion logs are persisted fire-and-forget (best-effort, never
    // awaited by the worker loop) — give the insert a beat to land before
    // reading it back.
    await new Promise((r) => setTimeout(r, 150))

    const health = await listWorkerHealth(sql)
    const mine = health.find((w) => w.workerId === worker.id)
    expect(mine).toBeDefined()
    // stop() writes a final 'stopped' heartbeat after graceful shutdown.
    expect(mine!.status).toBe('stopped')

    const logs = await getRunLogs(sql, runId)
    // #32: the worker's own step-completion log should have landed.
    expect(logs.length).toBeGreaterThan(0)
    const completed = logs.find((l) => (l.data as Record<string, unknown>).msg === 'step completed')
    expect(completed).toBeDefined()
    const data = completed!.data as Record<string, unknown>
    expect(data.stepName).toBe('only')
    expect(typeof data.durationMs).toBe('number')
  })
})
