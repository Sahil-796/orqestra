// Phase 4 end-to-end proof: one realistic workflow that exercises every
// shipped feature together — dependency gating (#19), fan-out/fan-in
// (#15/#16), conditional branching (#17) and a spawned+awaited child
// workflow (#20) — driven through several genuinely concurrent worker
// instances against real Postgres. No mocks, no single-process shortcuts.
//
// The per-feature mechanics already have dedicated proofs: tests/dag.test.ts
// (gating + branching + skip cascade), tests/fanout.test.ts (10-way
// fan-out/fan-in at the shipping-bar scale), tests/repositories-dag.test.ts
// (storage-layer races), tests/child-workflows.test.ts and
// tests/child-blocking.test.ts (spawn/await, the event-driven block, crash
// resume). This file does not re-derive any of those — it proves the
// features compose: a fan-in feeding a data-dependent branch, whose taken
// side blocks a step on a child workflow, whose own completion wakes the
// parent and flows into a final fan-in, all while N workers race for every
// step in the graph.
//
// Shape of the graph:
//
//   seed --> shard-0..4 --> aggregate --(skip one branch)--> fast-path (spawns+awaits a child) --\
//                                                          \                                       --> final
//                                                            slow-path (skipped, never runs) ------/

import { describe, expect, test, beforeAll, afterAll } from 'bun:test'
import { createDb } from '../src/store/client.ts'
import { migrate } from '../src/store/migrate.ts'
import { defineWorkflow } from '../src/define/workflow.ts'
import { enqueueRun } from '../src/engine/executor.ts'
import { createWorker, type Worker } from '../src/worker/worker.ts'
import { runChildWorkflow } from '../src/control/child.ts'
import {
  getRun,
  getStepsByRun,
  getChildRuns,
  countStepsByStatus,
  type RunRow,
  type StepRow,
} from '../src/store/repositories.ts'
import { decodeResult } from '../src/types.ts'

// step.result is persisted as an encoded Result<T> envelope (Result<T> +
// encodeResult/decodeResult in src/types.ts), not the raw return value —
// only run.output unwraps it. A step reading a sibling's committed value
// back from storage (rather than through ctx, which never carries one
// step's output to another) has to decode it the same way the worker does.
function committedValue<T>(step: StepRow | undefined): T | undefined {
  if (!step || step.status !== 'completed') return undefined
  const result = decodeResult<T>(step.result)
  return result.ok ? result.value : undefined
}

const sql = createDb()

beforeAll(async () => {
  await migrate(sql)
})

afterAll(async () => {
  await sql.end()
})

async function drain(runId: string, workers: Worker[], timeoutMs = 20_000): Promise<RunRow> {
  const deadline = Date.now() + timeoutMs
  let run = await getRun(sql, runId)
  while (
    run &&
    run.status !== 'completed' &&
    run.status !== 'failed' &&
    run.status !== 'cancelled' &&
    Date.now() < deadline
  ) {
    await new Promise((resolve) => setTimeout(resolve, 20))
    run = await getRun(sql, runId)
  }
  await Promise.all(workers.map((w) => w.stop()))
  if (!run) throw new Error(`drain: run "${runId}" vanished`)
  return run
}

function makeWorkerFleet(
  namespace: string,
  handles: Parameters<typeof createWorker>[0]['handles'],
  size: number
): Worker[] {
  const workers = Array.from({ length: size }, (_, i) =>
    createWorker({
      db: sql,
      handles,
      namespace,
      workerId: `e2e-worker-${namespace}-${i}`,
      concurrency: 3,
      leaseTtlMs: 5_000,
      pollIntervalMs: 15,
      reclaimIntervalMs: 60_000,
    })
  )
  for (const w of workers) w.start()
  return workers
}

describe('phase 4 end-to-end: gating + fan-out/fan-in + branching + child workflow, concurrently', () => {
  test('a run that fans out, branches on real data, awaits a child, and fans back in across concurrent workers', async () => {
    const namespace = `orch-e2e-${crypto.randomUUID()}`
    const N = 5

    const runCounts: Record<string, number> = {}
    const bump = (name: string) => (runCounts[name] = (runCounts[name] ?? 0) + 1)

    let maxConcurrentShards = 0
    let inFlightShards = 0
    let slowPathRan = false

    const childWf = defineWorkflow(`orch-e2e-child-${crypto.randomUUID()}`, (builder) => {
      builder.step('compute', async (ctx) => {
        bump('child:compute')
        const { total } = ctx.input as { total: number }
        return total * 10
      })
      builder.step(
        'finalize',
        async () => {
          bump('child:finalize')
          return 'child-finalized'
        },
        { dependsOn: ['compute'] }
      )
    })

    const parentWf = defineWorkflow(`orch-e2e-parent-${crypto.randomUUID()}`, (builder) => {
      builder.step('seed', async (ctx) => {
        bump('seed')
        return (ctx.input as { multiplier: number }).multiplier
      })

      const shardNames = builder.fanOut(
        'shard',
        N,
        async (i, ctx) => {
          bump(`shard-${i}`)
          inFlightShards++
          maxConcurrentShards = Math.max(maxConcurrentShards, inFlightShards)
          // Hold the step open briefly so genuine overlap across the worker
          // fleet is observable, not just theoretically possible.
          await new Promise((resolve) => setTimeout(resolve, 40))
          inFlightShards--
          return i * (ctx.input as { multiplier: number }).multiplier
        },
        { dependsOn: ['seed'] }
      )

      // Fan-in that makes a genuine data-dependent branching decision: it
      // reads the actual committed fan-out results back from storage (a
      // step function only ever sees ctx.input, never a sibling's output
      // directly — see define/context.ts) and sums them.
      builder.step(
        'aggregate',
        async (ctx) => {
          bump('aggregate')
          const steps = await getStepsByRun(sql, ctx.runId)
          const sum = shardNames.reduce((acc, name) => {
            const s = steps.find((st) => st.name === name)
            return acc + (committedValue<number>(s) ?? 0)
          }, 0)
          // multiplier = 2, shards 0..4 -> sum = 2*(0+1+2+3+4) = 20, always
          // even, so this is deterministic across runs of this test.
          if (sum % 2 === 0) {
            ctx.skip('slow-path')
          } else {
            ctx.skip('fast-path')
          }
          return sum
        },
        { dependsOn: shardNames }
      )

      // The taken branch: spawns and awaits a full child workflow, which
      // means this step blocks (Phase 4 #20) and is woken by the child's
      // terminal transition, not a timer.
      builder.step(
        'fast-path',
        async (ctx) => {
          bump('fast-path')
          const steps = await getStepsByRun(sql, ctx.runId)
          const aggregate = steps.find((s) => s.name === 'aggregate')
          const total = committedValue<number>(aggregate) ?? 0
          const result = await runChildWorkflow<{ finalize: string; compute: number }>(
            sql,
            ctx,
            childWf,
            { input: { total } }
          )
          return result
        },
        { dependsOn: ['aggregate'] }
      )

      // The untaken branch: must never execute.
      builder.step(
        'slow-path',
        async () => {
          slowPathRan = true
          bump('slow-path')
          return 'should-never-run'
        },
        { dependsOn: ['aggregate'] }
      )

      // Final fan-in past one real branch and one cascade-skipped branch.
      builder.step(
        'final',
        async (ctx) => {
          bump('final')
          const steps = await getStepsByRun(sql, ctx.runId)
          const fastPath = steps.find((s) => s.name === 'fast-path')
          return { fromChild: committedValue(fastPath), status: 'done' }
        },
        { dependsOn: ['fast-path', 'slow-path'] }
      )
    })

    const { runId } = await enqueueRun(sql, parentWf, {
      namespace,
      input: { multiplier: 2 },
    })

    // A genuine fleet: several independent worker processes (in-process,
    // each its own poll loop / lease), each capable of running either the
    // parent or the child workflow, racing for every step in the graph.
    const workers = makeWorkerFleet(namespace, [parentWf, childWf], 6)

    const run = await drain(runId, workers)

    expect(run.status).toBe('completed')
    expect(slowPathRan).toBe(false)
    // real overlap across the fleet, not one worker draining serially
    expect(maxConcurrentShards).toBeGreaterThan(1)

    // ---- every step ran exactly once (or, for the untaken branch, zero) ----
    expect(runCounts['seed']).toBe(1)
    for (let i = 0; i < N; i++) expect(runCounts[`shard-${i}`]).toBe(1)
    expect(runCounts['aggregate']).toBe(1)
    // fast-path calls runChildWorkflow, which blocks on the child (#20): the
    // step body runs once to spawn+block, then re-runs from the top on wake
    // to replay through to the child's result — two executions, one attempt
    // (see tests/child-blocking.test.ts). Only the attempt count, checked
    // below via the step row, is the "ran exactly once" guarantee here.
    expect(runCounts['fast-path']).toBe(2)
    expect(runCounts['final']).toBe(1)
    expect(runCounts['slow-path']).toBeUndefined()
    expect(runCounts['child:compute']).toBe(1)
    expect(runCounts['child:finalize']).toBe(1)

    // ---- row-level shape ----
    const steps = await getStepsByRun(sql, runId)
    const byName = Object.fromEntries(steps.map((s) => [s.name, s]))
    expect(byName['seed']?.status).toBe('completed')
    for (let i = 0; i < N; i++) expect(byName[`shard-${i}`]?.status).toBe('completed')
    expect(byName['aggregate']?.status).toBe('completed')
    expect(byName['fast-path']?.status).toBe('completed')
    expect(byName['slow-path']?.status).toBe('skipped')
    expect(byName['slow-path']?.skip_reason).toBe('branch not taken')
    expect(byName['final']?.status).toBe('completed')
    // the block/wake left no dangling link once resolved
    expect(byName['fast-path']?.awaited_child_run_id).toBeNull()
    // blocking on the child gave back the attempt the claim consumed, so
    // two executions of the body still cost exactly one attempt
    expect(byName['fast-path']?.attempt).toBe(1)

    const counts = await countStepsByStatus(sql, runId)
    expect(counts.completed).toBe(N + 4) // seed + N shards + aggregate + fast-path + final
    expect(counts.skipped).toBe(1) // slow-path
    expect(counts.blocked ?? 0).toBe(0) // resolved, none left blocked

    // ---- the child run really happened, end to end ----
    const children = await getChildRuns(sql, runId)
    expect(children).toHaveLength(1)
    const childRun = children[0]!
    expect(childRun.status).toBe('completed')
    expect(childRun.output).toEqual({ compute: 200, finalize: 'child-finalized' })
    const childSteps = await getStepsByRun(sql, childRun.id)
    expect(childSteps.every((s) => s.status === 'completed')).toBe(true)

    // ---- the final output threaded all the way through ----
    const output = run.output as Record<string, unknown>
    expect(output.final).toEqual({
      fromChild: { compute: 200, finalize: 'child-finalized' },
      status: 'done',
    })
  }, 30_000)
})
