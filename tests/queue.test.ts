// DB-backed proof of the Phase 2 queue primitives: concurrent SKIP LOCKED
// claiming, lease heartbeat fencing, lease reclaim, and the poison-pill
// ceiling. Needs Postgres up + migrated (bun run db:up && bun run migrate).
//
// Every seeded run gets its own random `namespace`, and every claim in this
// file passes that same namespace. The step queue is global by design (any
// worker can claim any due step), so without a namespace filter these tests
// would compete with rows left behind by other test files / other runs of
// this same file against the same long-lived dev Postgres — namespacing is
// the isolation mechanism the design already provides for exactly this.

import { describe, expect, test, beforeAll, afterAll } from 'bun:test'
import { createDb } from '../src/store/client.ts'
import { migrate } from '../src/store/migrate.ts'
import { claimStep } from '../src/queue/claim.ts'
import { heartbeatLease, releaseLease, reclaimExpiredLeases } from '../src/queue/lease.ts'
import {
  insertWorkflow,
  createRun,
  insertSteps,
  getStepsByRun,
  getRun,
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

/** Seed a run (in its own namespace) with `n` independent no-dep steps, all `ready`. */
async function seedReadyRun(
  n: number
): Promise<{ run: RunRow; steps: StepRow[]; namespace: string }> {
  const namespace = `queue-test-${crypto.randomUUID()}`
  const name = `queue-test-wf-${crypto.randomUUID()}`
  const dag: WorkflowDefinition = {
    name,
    version: 1,
    steps: Array.from({ length: n }, (_, i) => ({
      name: `step-${i}`,
      dependsOn: [],
      maxAttempts: 5,
      priority: 0,
    })),
  }
  const workflow = await insertWorkflow(sql, { name, dag })
  const { run } = await createRun(sql, { workflowId: workflow.id, namespace })

  const steps: NewStep[] = dag.steps.map((s) => ({
    name: s.name,
    dependsOn: s.dependsOn,
    maxAttempts: s.maxAttempts,
    priority: s.priority,
    status: 'ready',
  }))
  const inserted = await insertSteps(sql, run.id, steps)
  return { run, steps: inserted, namespace }
}

describe('claimStep', () => {
  test('concurrent claims never double-claim: every claimed step id is distinct', async () => {
    const N = 10
    const M = 15 // more workers than steps: the extra M-N must come back empty
    const { steps, namespace } = await seedReadyRun(N)

    const claims = await Promise.all(
      Array.from({ length: M }, (_, i) =>
        claimStep(sql, { workerId: `worker-${i}`, leaseTtlMs: 60_000, namespace })
      )
    )

    const successful = claims.filter((c) => c !== undefined)
    const empty = claims.filter((c) => c === undefined)

    expect(successful).toHaveLength(N)
    expect(empty).toHaveLength(M - N)

    const claimedIds = successful.map((c) => c!.id)
    expect(new Set(claimedIds).size).toBe(N) // no duplicates
    expect(new Set(claimedIds)).toEqual(new Set(steps.map((s) => s.id)))

    for (const c of successful) {
      expect(c!.status).toBe('running')
      expect(c!.attempt).toBe(1)
      expect(c!.lease_owner).not.toBeNull()
      expect(c!.lease_expires_at).not.toBeNull()
    }
  })

  test('returns undefined once the (namespaced) queue is drained', async () => {
    const { namespace } = await seedReadyRun(1)
    const first = await claimStep(sql, { workerId: 'w1', leaseTtlMs: 60_000, namespace })
    expect(first).toBeDefined()

    const second = await claimStep(sql, { workerId: 'w2', leaseTtlMs: 60_000, namespace })
    expect(second).toBeUndefined()
  })
})

describe('heartbeatLease', () => {
  test('extends the lease when called by the owning worker', async () => {
    const { namespace } = await seedReadyRun(1)
    const claimed = await claimStep(sql, { workerId: 'owner', leaseTtlMs: 5_000, namespace })
    expect(claimed).toBeDefined()

    const ok = await heartbeatLease(sql, claimed!.id, 'owner', 60_000)
    expect(ok).toBe(true)

    const [refetched] = (await getStepsByRun(sql, claimed!.run_id)).filter((s) => s.id === claimed!.id)
    expect(refetched!.lease_expires_at!.getTime()).toBeGreaterThan(claimed!.lease_expires_at!.getTime())
  })

  test('returns false when called by a worker that does not own the lease', async () => {
    const { namespace } = await seedReadyRun(1)
    const claimed = await claimStep(sql, { workerId: 'real-owner', leaseTtlMs: 60_000, namespace })
    expect(claimed).toBeDefined()

    const ok = await heartbeatLease(sql, claimed!.id, 'impostor', 60_000)
    expect(ok).toBe(false)
  })
})

describe('releaseLease', () => {
  test('clears lease ownership without changing status', async () => {
    const { namespace } = await seedReadyRun(1)
    const claimed = await claimStep(sql, { workerId: 'owner', leaseTtlMs: 60_000, namespace })
    expect(claimed).toBeDefined()

    await releaseLease(sql, claimed!.id)

    const [refetched] = (await getStepsByRun(sql, claimed!.run_id)).filter((s) => s.id === claimed!.id)
    expect(refetched!.lease_owner).toBeNull()
    expect(refetched!.lease_expires_at).toBeNull()
    expect(refetched!.status).toBe('running') // status is the caller's job, not releaseLease's
  })
})

describe('reclaimExpiredLeases', () => {
  test('an expired lease is reclaimed back to ready', async () => {
    const { run, namespace } = await seedReadyRun(1)
    // Negative TTL: the lease is already expired the instant it's granted,
    // no need to sleep past a real TTL in the test.
    const claimed = await claimStep(sql, { workerId: 'doomed-worker', leaseTtlMs: -1_000, namespace })
    expect(claimed?.status).toBe('running')

    const result = await reclaimExpiredLeases(sql)
    expect(result.reclaimed).toContain(claimed!.id)
    expect(result.deadLettered).not.toContain(claimed!.id)

    const [refetched] = (await getStepsByRun(sql, run.id)).filter((s) => s.id === claimed!.id)
    expect(refetched!.status).toBe('ready')
    expect(refetched!.lease_owner).toBeNull()
    expect(refetched!.lease_expires_at).toBeNull()
    expect(refetched!.reclaim_count).toBe(1)
  })

  test('a lease that is not expired is left alone', async () => {
    const { namespace } = await seedReadyRun(1)
    const claimed = await claimStep(sql, { workerId: 'alive-worker', leaseTtlMs: 60_000, namespace })
    expect(claimed).toBeDefined()

    const result = await reclaimExpiredLeases(sql)
    expect(result.reclaimed).not.toContain(claimed!.id)
    expect(result.deadLettered).not.toContain(claimed!.id)
  })

  test('the poison-pill ceiling fails the step (and its run) instead of reclaiming forever', async () => {
    const maxReclaims = 2

    // Build the run by hand rather than via seedReadyRun: `sibling` starts
    // `pending` on `poison` so it can never itself be claimed — this keeps
    // the claim queue single-candidate throughout the loop below (no race
    // over which of two `ready` steps gets picked each pass) while still
    // exercising cancelPendingSteps on a genuinely `pending` row.
    const namespace = `queue-test-${crypto.randomUUID()}`
    const wfName = `queue-test-wf-${crypto.randomUUID()}`
    const dag: WorkflowDefinition = {
      name: wfName,
      version: 1,
      steps: [
        { name: 'poison', dependsOn: [], maxAttempts: 5, priority: 0 },
        { name: 'sibling', dependsOn: ['poison'], maxAttempts: 5, priority: 0 },
      ],
    }
    const workflow = await insertWorkflow(sql, { name: wfName, dag })
    const { run } = await createRun(sql, { workflowId: workflow.id, namespace })
    await insertSteps(sql, run.id, [
      { name: 'poison', dependsOn: [], maxAttempts: 5, priority: 0, status: 'ready' },
      { name: 'sibling', dependsOn: ['poison'], maxAttempts: 5, priority: 0, status: 'pending' },
    ])

    // Drive `poison` through claim -> instantly-expired lease -> reclaim
    // sweep, `maxReclaims + 1` times. The first `maxReclaims` passes should
    // reclaim it back to `ready`; the pass that pushes reclaim_count past
    // the ceiling should dead-letter it instead.
    let lastResult: Awaited<ReturnType<typeof reclaimExpiredLeases>> | undefined
    for (let i = 0; i < maxReclaims + 1; i++) {
      const claimed = await claimStep(sql, { workerId: `crasher-${i}`, leaseTtlMs: -1_000, namespace })
      expect(claimed?.name).toBe('poison')
      lastResult = await reclaimExpiredLeases(sql, { maxReclaims })
    }

    const finalSteps = await getStepsByRun(sql, run.id)
    const poisoned = finalSteps.find((s) => s.name === 'poison')!
    const sibling = finalSteps.find((s) => s.name === 'sibling')!

    expect(lastResult!.deadLettered).toContain(poisoned.id)
    expect(poisoned.status).toBe('failed')
    expect(poisoned.reclaim_count).toBe(maxReclaims + 1)
    expect((poisoned.error as { message?: string } | null)?.message).toMatch(/poison-pill ceiling/)

    const finalRun = await getRun(sql, run.id)
    expect(finalRun!.status).toBe('failed')

    // never claimable (pending on a step that never completed) — must be
    // cancelled once its run is failed, so nothing tries to run it later.
    expect(sibling.status).toBe('cancelled')
  })
})
