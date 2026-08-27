// Feature #12 — concurrency limits, the correctness proof. The invariant under
// test is the hard one from the build plan: under a flood of ready steps
// sharing one concurrency key with limit N, NEVER more than N are `running` at
// once — even when many workers race the claim transaction concurrently, where
// a naive `count(*) < limit` subquery would let two claimers both read
// "N-1 < N" and both claim. Needs Postgres up + migrated (bun run db:up &&
// bun run migrate).
//
// Isolation, as in queue.test.ts: every run gets its own random `namespace`
// and every claim passes it, and every concurrency key is uniquified per test,
// so these assertions can't be perturbed by rows other suites leave behind in
// the shared dev Postgres (the concurrency count is global by key, by design).

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
  concurrencyKey?: string
  concurrencyLimit?: number
}

/** Seed a run (own namespace) with the given steps, all independent and `ready`. */
async function seedRun(steps: SeedStep[]): Promise<{ namespace: string; stepRows: StepRow[] }> {
  const namespace = `conc-test-${crypto.randomUUID()}`
  const name = `conc-test-wf-${crypto.randomUUID()}`
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
    concurrencyKey: s.concurrencyKey,
    concurrencyLimit: s.concurrencyLimit,
  }))
  const stepRows = await insertSteps(sql, run.id, newSteps)
  return { namespace, stepRows }
}

describe('concurrency limits (#12)', () => {
  test('a flood of same-key steps never exceeds the limit under concurrent claims', async () => {
    const LIMIT = 5
    const FLOOD = 40 // far more ready steps than the limit
    const WORKERS = 25 // many workers racing at once
    const key = `stripe-${crypto.randomUUID()}`

    const { namespace } = await seedRun(
      Array.from({ length: FLOOD }, () => ({ concurrencyKey: key, concurrencyLimit: LIMIT }))
    )

    // Race WORKERS concurrent claims. Claimed steps flip to `running` and are
    // NOT released, so the number that succeed is exactly how many can be
    // `running` simultaneously — which must be capped at LIMIT.
    const claims = await Promise.all(
      Array.from({ length: WORKERS }, (_, i) =>
        claimStep(sql, { workerId: `w-${i}`, leaseTtlMs: 60_000, namespace })
      )
    )

    const claimed = claims.filter((c): c is StepRow => c !== undefined)
    expect(claimed.length).toBe(LIMIT)
    for (const c of claimed) {
      expect(c.status).toBe('running')
      expect(c.concurrency_key).toBe(key)
    }

    // And the durable truth: exactly LIMIT rows are `running` for this key.
    const running = (await getStepsByRun(sql, claimed[0]!.run_id)).filter(
      (s) => s.status === 'running'
    )
    expect(running.length).toBe(LIMIT)
  })

  test('the invariant holds across repeated concurrent waves', async () => {
    const LIMIT = 3
    const key = `wave-${crypto.randomUUID()}`
    const { namespace } = await seedRun(
      Array.from({ length: 30 }, () => ({ concurrencyKey: key, concurrencyLimit: LIMIT }))
    )

    let runId: string | undefined
    // Several waves of concurrent claims. Nothing is ever released, so after
    // the very first wave the key is already saturated and every later wave
    // must claim nothing — running count stays pinned at LIMIT throughout.
    for (let wave = 0; wave < 4; wave++) {
      const claims = await Promise.all(
        Array.from({ length: 10 }, (_, i) =>
          claimStep(sql, { workerId: `w-${wave}-${i}`, leaseTtlMs: 60_000, namespace })
        )
      )
      const claimed = claims.filter((c): c is StepRow => c !== undefined)
      runId ??= claimed[0]?.run_id
      const running = (await getStepsByRun(sql, runId!)).filter((s) => s.status === 'running')
      expect(running.length).toBeLessThanOrEqual(LIMIT)
    }

    const running = (await getStepsByRun(sql, runId!)).filter((s) => s.status === 'running')
    expect(running.length).toBe(LIMIT)
  })

  test('un-keyed steps are unaffected by a saturated key', async () => {
    const LIMIT = 2
    const key = `mixed-${crypto.randomUUID()}`
    const KEYED = 6
    const UNKEYED = 5

    const { namespace } = await seedRun([
      ...Array.from({ length: KEYED }, () => ({ concurrencyKey: key, concurrencyLimit: LIMIT })),
      ...Array.from({ length: UNKEYED }, () => ({})),
    ])

    // More workers than un-keyed + limit, all at once.
    const claims = await Promise.all(
      Array.from({ length: KEYED + UNKEYED }, (_, i) =>
        claimStep(sql, { workerId: `w-${i}`, leaseTtlMs: 60_000, namespace })
      )
    )
    const claimed = claims.filter((c): c is StepRow => c !== undefined)

    const keyedClaimed = claimed.filter((c) => c.concurrency_key === key)
    const unkeyedClaimed = claimed.filter((c) => c.concurrency_key === null)

    // The key is capped at LIMIT, but every un-keyed step is still claimable.
    expect(keyedClaimed.length).toBe(LIMIT)
    expect(unkeyedClaimed.length).toBe(UNKEYED)
  })

  test('as running keyed steps complete, blocked ones become claimable again', async () => {
    const LIMIT = 2
    const key = `drain-${crypto.randomUUID()}`
    const { namespace } = await seedRun(
      Array.from({ length: 5 }, () => ({ concurrencyKey: key, concurrencyLimit: LIMIT }))
    )

    // First wave saturates the key.
    const first = await Promise.all(
      Array.from({ length: 5 }, (_, i) =>
        claimStep(sql, { workerId: `a-${i}`, leaseTtlMs: 60_000, namespace })
      )
    )
    const firstClaimed = first.filter((c): c is StepRow => c !== undefined)
    expect(firstClaimed.length).toBe(LIMIT)

    // Nothing new claimable while the key is full.
    const blocked = await claimStep(sql, { workerId: 'b', leaseTtlMs: 60_000, namespace })
    expect(blocked).toBeUndefined()

    // Complete one running step, freeing a slot.
    await sql`update step set status = 'completed', updated_at = now() where id = ${firstClaimed[0]!.id}`

    const freed = await claimStep(sql, { workerId: 'c', leaseTtlMs: 60_000, namespace })
    expect(freed).toBeDefined()
    expect(freed!.concurrency_key).toBe(key)

    // Still capped: one running + one just-claimed = LIMIT; no more.
    const another = await claimStep(sql, { workerId: 'd', leaseTtlMs: 60_000, namespace })
    expect(another).toBeUndefined()
  })
})
