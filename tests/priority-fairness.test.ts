// Feature #14 — priorities / queue fairness. Two things to prove:
//   1. plain priority still works: a higher-priority step is claimed first;
//   2. aging works: a low-priority step that has waited long enough is claimed
//      AHEAD of fresher, higher-priority work — the anti-starvation property
//      the build plan calls for ("naive priority DESC starves normal runs when
//      high-priority work floods in").
//
// The pure aging math is also pinned directly against control/priority.ts so a
// formula change can't drift the SQL and the TS apart silently. Needs Postgres
// up + migrated (bun run db:up && bun run migrate).

import { describe, expect, test, beforeAll, afterAll } from 'bun:test'
import { createDb } from '../src/store/client.ts'
import { migrate } from '../src/store/migrate.ts'
import { claimStep } from '../src/queue/claim.ts'
import { insertWorkflow, createRun, insertSteps, type NewStep, type StepRow } from '../src/store/repositories.ts'
import { ageBoost, effectivePriority } from '../src/control/priority.ts'
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
  priority: number
  /** Seconds in the PAST to set run_after to, simulating how long it's been ready. */
  agedSeconds?: number
}

/** Seed a run (own namespace) with independent `ready` steps, back-dating
 *  run_after where an age is requested so aging has something to bite on. */
async function seedRun(steps: SeedStep[]): Promise<{ namespace: string; rows: StepRow[] }> {
  const namespace = `prio-test-${crypto.randomUUID()}`
  const name = `prio-test-wf-${crypto.randomUUID()}`
  const dag: WorkflowDefinition = {
    name,
    version: 1,
    steps: steps.map((s) => ({ name: s.name, dependsOn: [], maxAttempts: 5, priority: s.priority })),
  }
  const workflow = await insertWorkflow(sql, { name, dag })
  const { run } = await createRun(sql, { workflowId: workflow.id, namespace })

  const newSteps: NewStep[] = steps.map((s) => ({
    name: s.name,
    dependsOn: [],
    maxAttempts: 5,
    priority: s.priority,
    status: 'ready',
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
  return { namespace, rows }
}

describe('priority aging math (control/priority.ts)', () => {
  test('boost is zero at age zero and rises linearly until capped', () => {
    const cfg = { ratePerSec: 2, maxBoost: 100 }
    expect(ageBoost(0, cfg)).toBe(0)
    expect(ageBoost(10, cfg)).toBe(20) // 10s * 2
    expect(ageBoost(1000, cfg)).toBe(100) // capped
    expect(ageBoost(-5, cfg)).toBe(0) // never negative
  })

  test('effective priority is base plus boost', () => {
    const cfg = { ratePerSec: 1, maxBoost: 50 }
    expect(effectivePriority(5, 0, cfg)).toBe(5)
    expect(effectivePriority(5, 10, cfg)).toBe(15)
    expect(effectivePriority(5, 1000, cfg)).toBe(55) // 5 + capped 50
  })

  test('rate 0 disables aging — effective collapses to base', () => {
    const cfg = { ratePerSec: 0, maxBoost: 100 }
    expect(effectivePriority(7, 9999, cfg)).toBe(7)
  })
})

describe('priority ordering (#14)', () => {
  test('higher priority is claimed before lower when both are fresh', async () => {
    const { namespace } = await seedRun([
      { name: 'low', priority: 0 },
      { name: 'high', priority: 10 },
    ])

    // Aging on (defaults), but both are fresh so base priority decides.
    const first = await claimStep(sql, { workerId: 'w1', leaseTtlMs: 60_000, namespace })
    const second = await claimStep(sql, { workerId: 'w2', leaseTtlMs: 60_000, namespace })

    expect(first!.name).toBe('high')
    expect(second!.name).toBe('low')
  })

  test('an aged low-priority step is claimed ahead of fresh higher-priority work (aging)', async () => {
    // Without aging, `fresh-high` (base 3) outranks `aged-low` (base 1). With
    // aging on, aged-low has waited 300s → +100 boost → effective 101, so it
    // must be claimed FIRST. This is the anti-starvation guarantee.
    const { namespace } = await seedRun([
      { name: 'fresh-high', priority: 3 },
      { name: 'aged-low', priority: 1, agedSeconds: 300 },
    ])

    const first = await claimStep(sql, {
      workerId: 'w1',
      leaseTtlMs: 60_000,
      namespace,
      priorityAgeRatePerSec: 1,
      priorityAgeMaxBoost: 100,
    })
    const second = await claimStep(sql, {
      workerId: 'w2',
      leaseTtlMs: 60_000,
      namespace,
      priorityAgeRatePerSec: 1,
      priorityAgeMaxBoost: 100,
    })

    expect(first!.name).toBe('aged-low')
    expect(second!.name).toBe('fresh-high')
  })

  test('control: with aging disabled the same setup claims fresh-high first', async () => {
    // Same shape as above but rate 0 — proves the flip in the previous test is
    // aging's doing, not the run_after tiebreak.
    const { namespace } = await seedRun([
      { name: 'fresh-high', priority: 3 },
      { name: 'aged-low', priority: 1, agedSeconds: 300 },
    ])

    const first = await claimStep(sql, {
      workerId: 'w1',
      leaseTtlMs: 60_000,
      namespace,
      priorityAgeRatePerSec: 0,
      priorityAgeMaxBoost: 0,
    })

    expect(first!.name).toBe('fresh-high')
  })
})
