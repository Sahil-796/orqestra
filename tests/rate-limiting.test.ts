// Feature #13 — rate limiting, the correctness proof. The invariant from the
// build plan: under a flood of ready steps sharing one rate key with limit N
// per window, NEVER more than N are claimed (i.e. START) within one window —
// even when many workers race the claim transaction concurrently, where a naive
// read-then-increment would let two claimers both see budget and both consume
// it. And the second half: steps deferred because a window was exhausted become
// claimable again once the window rolls over. Needs Postgres up + migrated
// (bun run db:up && bun run migrate).
//
// Isolation, as in concurrency-limits.test.ts: every run gets its own random
// `namespace` and every rate key is uniquified per test, so these assertions
// can't be perturbed by rows other suites leave behind in the shared dev
// Postgres (the rate budget is global by key, by design).

import { describe, expect, test, beforeAll, afterAll } from 'bun:test'
import { createDb } from '../src/store/client.ts'
import { migrate } from '../src/store/migrate.ts'
import { claimStep } from '../src/queue/claim.ts'
import { windowStartMs, nextWindowStartMs } from '../src/control/ratelimit.ts'
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
  rateKey?: string
  rateLimit?: number
  rateWindowMs?: number
}

/** Seed a run (own namespace) with the given steps, all independent and `ready`. */
async function seedRun(steps: SeedStep[]): Promise<{ namespace: string; runId: string }> {
  const namespace = `rate-test-${crypto.randomUUID()}`
  const name = `rate-test-wf-${crypto.randomUUID()}`
  const dag: WorkflowDefinition = {
    name,
    version: 1,
    steps: steps.map((_, i) => ({ name: `step-${i}`, dependsOn: [], maxAttempts: 5, priority: 0 })),
  }
  const workflow = await insertWorkflow(sql, { name, dag })
  const { run } = await createRun(sql, { workflowId: workflow.id, namespace })

  const newSteps: NewStep[] = steps.map((s, i) => ({
    name: `step-${i}`,
    dependsOn: [],
    maxAttempts: 5,
    priority: 0,
    status: 'ready',
    rateKey: s.rateKey,
    rateLimit: s.rateLimit,
    rateWindowMs: s.rateWindowMs,
  }))
  await insertSteps(sql, run.id, newSteps)
  return { namespace, runId: run.id }
}

describe('rate limiting (#13)', () => {
  test('pure window math: helpers agree with a fixed-window carve-up', () => {
    expect(windowStartMs(1234, 1000)).toBe(1000)
    expect(windowStartMs(2000, 1000)).toBe(2000)
    expect(nextWindowStartMs(1234, 1000)).toBe(2000)
    expect(nextWindowStartMs(2000, 1000)).toBe(3000)
  })

  test('a flood of same-key steps never exceeds the per-window limit under concurrent claims', async () => {
    const LIMIT = 5
    const WINDOW_MS = 60_000 // long window: everything below happens inside it
    const FLOOD = 40 // far more ready steps than the limit
    const WORKERS = 25 // many workers racing at once
    const key = `api-${crypto.randomUUID()}`

    const { namespace, runId } = await seedRun(
      Array.from({ length: FLOOD }, () => ({ rateKey: key, rateLimit: LIMIT, rateWindowMs: WINDOW_MS }))
    )

    // Race WORKERS concurrent claims. Each claimed step consumed exactly one
    // unit of the window's budget, so the number that succeed is how many starts
    // the window granted — which must be capped at LIMIT.
    const claims = await Promise.all(
      Array.from({ length: WORKERS }, (_, i) =>
        claimStep(sql, { workerId: `w-${i}`, leaseTtlMs: 60_000, namespace })
      )
    )

    const claimed = claims.filter((c): c is StepRow => c !== undefined)
    expect(claimed.length).toBe(LIMIT)
    for (const c of claimed) {
      expect(c.status).toBe('running')
      expect(c.rate_key).toBe(key)
    }

    // Durable truth: the window counter holds exactly LIMIT.
    const windows = await sql<{ count: number }[]>`
      select count from rate_window where rate_key = ${key}
    `
    const total = windows.reduce((sum, w) => sum + Number(w.count), 0)
    expect(total).toBe(LIMIT)

    // The remaining steps were not lost or failed — they are all still `ready`
    // (claimable again once the window rolls over). Each claim call that hit the
    // exhausted window deferred the candidate it picked (run_after pushed to the
    // next boundary); the boundary-crossing itself is proven in a later test.
    const steps = await getStepsByRun(sql, runId)
    const stillReady = steps.filter((s) => s.status === 'ready')
    expect(stillReady.length).toBe(FLOOD - LIMIT)
    expect(steps.some((s) => s.status === 'failed')).toBe(false)
    // At least some ready steps were deferred to a future window boundary.
    expect(stillReady.some((s) => s.run_after.getTime() > Date.now())).toBe(true)
  })

  test('the invariant holds across repeated concurrent waves within one window', async () => {
    const LIMIT = 3
    const WINDOW_MS = 60_000
    const key = `wave-${crypto.randomUUID()}`
    const { namespace, runId } = await seedRun(
      Array.from({ length: 30 }, () => ({ rateKey: key, rateLimit: LIMIT, rateWindowMs: WINDOW_MS }))
    )

    // Several waves of concurrent claims, all inside one long window. The first
    // wave spends the whole budget; every later wave must claim nothing.
    let totalClaimed = 0
    for (let wave = 0; wave < 4; wave++) {
      const claims = await Promise.all(
        Array.from({ length: 10 }, (_, i) =>
          claimStep(sql, { workerId: `w-${wave}-${i}`, leaseTtlMs: 60_000, namespace })
        )
      )
      totalClaimed += claims.filter((c): c is StepRow => c !== undefined).length
    }
    expect(totalClaimed).toBe(LIMIT)

    const running = (await getStepsByRun(sql, runId)).filter((s) => s.status === 'running')
    expect(running.length).toBe(LIMIT)
  })

  test('un-keyed steps are unaffected by an exhausted rate key', async () => {
    const LIMIT = 2
    const WINDOW_MS = 60_000
    const key = `mixed-${crypto.randomUUID()}`
    const KEYED = 6
    const UNKEYED = 5

    const { namespace } = await seedRun([
      ...Array.from({ length: KEYED }, () => ({ rateKey: key, rateLimit: LIMIT, rateWindowMs: WINDOW_MS })),
      ...Array.from({ length: UNKEYED }, () => ({})),
    ])

    const claims = await Promise.all(
      Array.from({ length: KEYED + UNKEYED }, (_, i) =>
        claimStep(sql, { workerId: `w-${i}`, leaseTtlMs: 60_000, namespace })
      )
    )
    const claimed = claims.filter((c): c is StepRow => c !== undefined)

    const keyedClaimed = claimed.filter((c) => c.rate_key === key)
    const unkeyedClaimed = claimed.filter((c) => c.rate_key === null)

    // The key is capped at LIMIT per window, but every un-keyed step is claimable.
    expect(keyedClaimed.length).toBe(LIMIT)
    expect(unkeyedClaimed.length).toBe(UNKEYED)
  })

  test('deferred steps become claimable again in the next window', async () => {
    const LIMIT = 2
    const WINDOW_MS = 700 // short window so the test can cross a boundary quickly
    const key = `roll-${crypto.randomUUID()}`
    const { namespace, runId } = await seedRun(
      Array.from({ length: 6 }, () => ({ rateKey: key, rateLimit: LIMIT, rateWindowMs: WINDOW_MS }))
    )

    // First window: spend the whole budget.
    const first = await Promise.all(
      Array.from({ length: 6 }, (_, i) =>
        claimStep(sql, { workerId: `a-${i}`, leaseTtlMs: 60_000, namespace })
      )
    )
    const firstClaimed = first.filter((c): c is StepRow => c !== undefined)
    expect(firstClaimed.length).toBe(LIMIT)

    // Nothing more claimable while the window is exhausted (steps are deferred).
    const blocked = await claimStep(sql, { workerId: 'b', leaseTtlMs: 60_000, namespace })
    expect(blocked).toBeUndefined()

    // Wait for the window to roll over (plus slack for clock skew / boundary).
    await new Promise((resolve) => setTimeout(resolve, WINDOW_MS + 300))

    // Fresh budget: deferred steps are claimable again, still capped at LIMIT.
    const second = await Promise.all(
      Array.from({ length: 6 }, (_, i) =>
        claimStep(sql, { workerId: `c-${i}`, leaseTtlMs: 60_000, namespace })
      )
    )
    const secondClaimed = second.filter((c): c is StepRow => c !== undefined)
    expect(secondClaimed.length).toBe(LIMIT)

    // Across both windows, exactly 2 * LIMIT distinct steps have started.
    const running = (await getStepsByRun(sql, runId)).filter((s) => s.status === 'running')
    expect(running.length).toBe(LIMIT * 2)
  })
})
