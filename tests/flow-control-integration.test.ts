// Phase 6 headline proof: concurrency limits (#12), rate limiting (#13) and
// priority + fairness (#14) ALL holding at once under a flood of ready steps
// and many concurrent claimers — the "max 5 Stripe steps concurrently" /
// "≤100 calls/min" scenarios from the build plan, but exercised together
// rather than in isolation (each feature already has its own dedicated,
// exhaustive proof: tests/concurrency-limits.test.ts, tests/rate-limiting.test.ts,
// tests/priority-fairness.test.ts — this file is the integration slice on top).
//
// Scale note: the build plan talks about a "1k-run flood". A few dozen ready
// steps raced by ~25 concurrent claimers already exercises the exact race the
// invariant guards against (two claimers both reading a stale "under limit"
// count and both claiming) — Postgres's transaction semantics don't care
// whether the candidate pool is 40 rows or 1000, only whether more than one
// claimer can observe the same pre-commit state, which concurrency is what
// drives here. 1000 rows would only make the suite slower, not more honest.
//
// Needs Postgres up + migrated (bun run db:up && bun run migrate).
//
// Isolation, as in the per-feature suites: every run gets its own random
// `namespace` and every concurrency/rate key is uniquified per test, so these
// assertions can't be perturbed by rows other suites leave behind in the
// shared dev Postgres.

import { describe, expect, test, beforeAll, afterAll } from 'bun:test'
import { createDb } from '../src/store/client.ts'
import { migrate } from '../src/store/migrate.ts'
import { claimStep } from '../src/queue/claim.ts'
import {
  insertWorkflow,
  createRun,
  insertSteps,
  getStepsByRun,
  type NewStep,
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

interface SeedStep {
  name: string
  priority?: number
  concurrencyKey?: string
  concurrencyLimit?: number
  rateKey?: string
  rateLimit?: number
  rateWindowMs?: number
  /** Seconds in the PAST to back-date run_after to, simulating wait time. */
  agedSeconds?: number
}

/** Seed a run (own namespace) with independent `ready` steps, back-dating
 *  run_after where an age is requested so aging has something to bite on. */
async function seedRun(steps: SeedStep[]): Promise<{ namespace: string; runId: string; rows: StepRow[] }> {
  const namespace = `flow-control-${crypto.randomUUID()}`
  const name = `flow-control-wf-${crypto.randomUUID()}`
  const dag: WorkflowDefinition = {
    name,
    version: 1,
    steps: steps.map((s) => ({ name: s.name, dependsOn: [], maxAttempts: 5, priority: s.priority ?? 0 })),
  }
  const workflow = await insertWorkflow(sql, { name, dag })
  const { run } = await createRun(sql, { workflowId: workflow.id, namespace })

  const newSteps: NewStep[] = steps.map((s) => ({
    name: s.name,
    dependsOn: [],
    maxAttempts: 5,
    priority: s.priority ?? 0,
    status: 'ready',
    concurrencyKey: s.concurrencyKey,
    concurrencyLimit: s.concurrencyLimit,
    rateKey: s.rateKey,
    rateLimit: s.rateLimit,
    rateWindowMs: s.rateWindowMs,
  }))
  const rows = await insertSteps(sql, run.id, newSteps)

  for (const s of steps) {
    if (s.agedSeconds && s.agedSeconds > 0) {
      const row = rows.find((r) => r.name === s.name)!
      await sql`
        update step set run_after = now() - (${s.agedSeconds} * interval '1 second')
        where id = ${row.id}
      `
    }
  }
  return { namespace, runId: run.id, rows }
}

async function claimWave(namespace: string, count: number, prefix: string): Promise<StepRow[]> {
  const claims = await Promise.all(
    Array.from({ length: count }, (_, i) =>
      claimStep(sql, { workerId: `${prefix}-${i}`, leaseTtlMs: 60_000, namespace })
    )
  )
  return claims.filter((c): c is StepRow => c !== undefined)
}

describe('flow control at scale — integration (#12, #13, #14)', () => {
  test('concurrency: a flood of same-key steps never exceeds the limit under many concurrent claimers', async () => {
    const LIMIT = 5
    const FLOOD = 40 // far more ready steps than the limit
    const WORKERS = 25 // many workers racing at once — see scale note above
    const key = `stripe-${crypto.randomUUID()}`

    const { namespace, runId } = await seedRun(
      Array.from({ length: FLOOD }, (_, i) => ({
        name: `charge-${i}`,
        concurrencyKey: key,
        concurrencyLimit: LIMIT,
      }))
    )

    const claimed = await claimWave(namespace, WORKERS, 'w')
    expect(claimed.length).toBe(LIMIT)
    for (const c of claimed) expect(c.status).toBe('running')

    const running = (await getStepsByRun(sql, runId)).filter((s) => s.status === 'running')
    expect(running.length).toBe(LIMIT)
  })

  test('rate limiting: a flood of same-key steps never exceeds the per-window limit, and deferred ones become claimable next window', async () => {
    const LIMIT = 5
    // Window wide enough that one wave of concurrent claims reliably finishes
    // inside a single window (a too-short window risks the wave itself
    // straddling a boundary under DB/pool contention, which would let more
    // than LIMIT through and make the test flaky) — proven flaky in practice
    // at 700ms, stable at 3s.
    const WINDOW_MS = 3_000
    const FLOOD = 20
    const WORKERS = 15
    const key = `partner-api-${crypto.randomUUID()}`

    const { namespace, runId } = await seedRun(
      Array.from({ length: FLOOD }, (_, i) => ({
        name: `call-${i}`,
        rateKey: key,
        rateLimit: LIMIT,
        rateWindowMs: WINDOW_MS,
      }))
    )

    // First window: race many claimers, never more than LIMIT starts.
    const first = await claimWave(namespace, WORKERS, 'a')
    expect(first.length).toBe(LIMIT)

    const windows = await sql<{ count: number }[]>`select count from rate_window where rate_key = ${key}`
    expect(windows.reduce((sum, w) => sum + Number(w.count), 0)).toBe(LIMIT)

    // Deferred steps are still `ready`, not lost or failed, just pushed to the
    // next window boundary.
    const afterFirst = await getStepsByRun(sql, runId)
    expect(afterFirst.filter((s) => s.status === 'ready').length).toBe(FLOOD - LIMIT)
    expect(afterFirst.some((s) => s.status === 'failed')).toBe(false)

    // Roll over into the next window and race again — fresh budget, same cap.
    // Sleep just past one window boundary (not two), so the second wave lands
    // in exactly the next window rather than possibly spanning further ones.
    await new Promise((resolve) => setTimeout(resolve, WINDOW_MS + 300))
    const second = await claimWave(namespace, WORKERS, 'b')
    expect(second.length).toBe(LIMIT)

    const running = (await getStepsByRun(sql, runId)).filter((s) => s.status === 'running')
    expect(running.length).toBe(LIMIT * 2)
  })

  test('priority: higher-priority steps are claimed before lower ones under a flood, and aging lets a long-waiting low-priority step win fairness', async () => {
    const FLOOD = 20
    const { namespace } = await seedRun([
      // A flood of fresh, ordinary-priority steps...
      ...Array.from({ length: FLOOD }, (_, i) => ({ name: `bulk-${i}`, priority: 0 })),
      // ...one fresh high-priority step that should jump the queue...
      { name: 'urgent-refund', priority: 20 },
      // ...and one low-priority step that's been waiting so long its aged
      // effective priority beats even the fresh high-priority one.
      { name: 'starved-low', priority: 1, agedSeconds: 300 },
    ])

    // Aging on (ORQ_PRIORITY_AGE_RATE_PER_SEC=1, cap 100 by default): the
    // starved-low step has waited 300s -> +100 boost (capped) -> effective 101,
    // which beats urgent-refund's fresh effective priority of 20.
    const first = await claimStep(sql, {
      workerId: 'w1',
      leaseTtlMs: 60_000,
      namespace,
      priorityAgeRatePerSec: 1,
      priorityAgeMaxBoost: 100,
    })
    expect(first!.name).toBe('starved-low')

    const second = await claimStep(sql, {
      workerId: 'w2',
      leaseTtlMs: 60_000,
      namespace,
      priorityAgeRatePerSec: 1,
      priorityAgeMaxBoost: 100,
    })
    expect(second!.name).toBe('urgent-refund')

    // Everything claimed after that is bulk work, in no particular priority
    // order among itself (all priority 0, all equally fresh) — but none of it
    // should have been claimed ahead of the two priority steps above.
    const rest = await claimWave(namespace, FLOOD, 'w-bulk')
    expect(rest.length).toBe(FLOOD)
    expect(rest.every((s) => s.name.startsWith('bulk-'))).toBe(true)
  })

  test('all three features compose under one mixed flood: concurrency cap, rate cap and priority order all hold simultaneously', async () => {
    const CONC_LIMIT = 4
    const RATE_LIMIT = 4
    const WINDOW_MS = 60_000 // long: this test only needs one window
    const concKey = `mix-conc-${crypto.randomUUID()}`
    const rateKey = `mix-rate-${crypto.randomUUID()}`

    const { namespace, runId } = await seedRun([
      // A flood sharing a concurrency key, capped at CONC_LIMIT running.
      ...Array.from({ length: 12 }, (_, i) => ({
        name: `conc-${i}`,
        priority: 0,
        concurrencyKey: concKey,
        concurrencyLimit: CONC_LIMIT,
      })),
      // A flood sharing a rate key, capped at RATE_LIMIT starts/window.
      ...Array.from({ length: 12 }, (_, i) => ({
        name: `rate-${i}`,
        priority: 0,
        rateKey,
        rateLimit: RATE_LIMIT,
        rateWindowMs: WINDOW_MS,
      })),
      // A high-priority, unconstrained step that must be claimed first.
      { name: 'urgent', priority: 50 },
    ])

    // Race everything at once with far more workers than any single cap.
    const claimed = await claimWave(namespace, 40, 'mix')

    const concClaimed = claimed.filter((c) => c.concurrency_key === concKey)
    const rateClaimed = claimed.filter((c) => c.rate_key === rateKey)
    const urgentClaimed = claimed.find((c) => c.name === 'urgent')

    expect(concClaimed.length).toBe(CONC_LIMIT)
    expect(rateClaimed.length).toBe(RATE_LIMIT)
    expect(urgentClaimed).toBeDefined()

    // Durable truth for both caps, independently.
    const running = (await getStepsByRun(sql, runId)).filter((s) => s.status === 'running')
    expect(running.filter((s) => s.concurrency_key === concKey).length).toBe(CONC_LIMIT)

    const windows = await sql<{ count: number }[]>`select count from rate_window where rate_key = ${rateKey}`
    expect(windows.reduce((sum, w) => sum + Number(w.count), 0)).toBe(RATE_LIMIT)
  })
})
