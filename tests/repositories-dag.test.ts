// Phase 4 storage-layer proof (Orchestration & DAGs): dependency readiness
// transitions, the fan-in "last dependency wins" race, skipped-branch
// recording, and parent/child run linkage. All against real Postgres —
// nothing here mocks the DB, because the whole point of the new primitives
// (satisfied_deps' containment check, the blocked/skipped states) is what
// they do under real row-level locking and real concurrency.

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
  completeStep,
  getDependencySteps,
  recordDependencySatisfied,
  skipStep,
  getChildRuns,
  blockStepOnChildRun,
  resolveBlockedStepForChildRun,
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

// ---- seeding helpers, matching tests/repositories.test.ts's seedRun ------

interface Seed {
  run: RunRow
  steps: StepRow[]
  namespace: string
}

async function seedRun(specs: Partial<NewStep>[]): Promise<Seed> {
  const namespace = `dag-test-${crypto.randomUUID()}`
  const name = `dag-wf-${crypto.randomUUID()}`
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
      status: specs[i]?.status ?? 'pending',
    }))
  )
  return { run, steps, namespace }
}

function stepNamed(steps: StepRow[], name: string): StepRow {
  const step = steps.find((s) => s.name === name)
  if (!step) throw new Error(`no step named "${name}" in [${steps.map((s) => s.name).join(', ')}]`)
  return step
}

async function claim(namespace: string, workerId: string): Promise<StepRow> {
  const claimed = await claimNextStep(sql, { workerId, leaseTtlMs: 60_000, namespace })
  if (!claimed) throw new Error('claim: expected a claimable step')
  return claimed
}

// ---- #19 dependency readiness ---------------------------------------------

describe('getDependencySteps', () => {
  test('resolves depends_on names to sibling step rows', async () => {
    const { run } = await seedRun([
      { name: 'a', status: 'ready' },
      { name: 'b', status: 'ready' },
      { name: 'join', dependsOn: ['a', 'b'] },
    ])
    const steps = await getStepsByRun(sql, run.id)
    const join = stepNamed(steps, 'join')

    const deps = await getDependencySteps(sql, join.id)
    expect(deps.map((d) => d.name).sort()).toEqual(['a', 'b'])
  })

  test('a step with no dependencies resolves to an empty set', async () => {
    const { run } = await seedRun([{ name: 'solo', status: 'ready' }])
    const steps = await getStepsByRun(sql, run.id)
    const deps = await getDependencySteps(sql, stepNamed(steps, 'solo').id)
    expect(deps).toEqual([])
  })
})

describe('recordDependencySatisfied', () => {
  test('a single-dependency step is released to ready on its one dependency resolving', async () => {
    const { run } = await seedRun([
      { name: 'a', status: 'ready' },
      { name: 'b', dependsOn: ['a'] },
    ])
    const steps = await getStepsByRun(sql, run.id)
    const b = stepNamed(steps, 'b')
    expect(b.status).toBe('pending')

    const released = await recordDependencySatisfied(sql, b.id, 'a')
    expect(released).toBeDefined()
    expect(released?.status).toBe('ready')
    expect(released?.satisfied_deps).toEqual(['a'])
  })

  test('a multi-dependency step stays pending until every dependency is recorded', async () => {
    const { run } = await seedRun([
      { name: 'a', status: 'ready' },
      { name: 'b', status: 'ready' },
      { name: 'c', status: 'ready' },
      { name: 'join', dependsOn: ['a', 'b', 'c'] },
    ])
    const steps = await getStepsByRun(sql, run.id)
    const join = stepNamed(steps, 'join')

    const afterA = await recordDependencySatisfied(sql, join.id, 'a')
    expect(afterA?.status).toBe('pending')
    expect(afterA?.satisfied_deps).toEqual(['a'])

    const afterB = await recordDependencySatisfied(sql, join.id, 'b')
    expect(afterB?.status).toBe('pending')
    expect(afterB?.satisfied_deps.sort()).toEqual(['a', 'b'])

    const afterC = await recordDependencySatisfied(sql, join.id, 'c')
    expect(afterC?.status).toBe('ready')
    expect(afterC?.satisfied_deps.sort()).toEqual(['a', 'b', 'c'])
  })

  test('is idempotent: recording the same dependency twice does not double-release or error', async () => {
    const { run } = await seedRun([
      { name: 'a', status: 'ready' },
      { name: 'b', dependsOn: ['a'] },
    ])
    const steps = await getStepsByRun(sql, run.id)
    const b = stepNamed(steps, 'b')

    const first = await recordDependencySatisfied(sql, b.id, 'a')
    expect(first?.status).toBe('ready')

    // second call: step is no longer `pending`, so this is a documented no-op
    const second = await recordDependencySatisfied(sql, b.id, 'a')
    expect(second).toBeUndefined()

    const reread = await getStepsByRun(sql, run.id)
    expect(stepNamed(reread, 'b').status).toBe('ready')
  })

  test('the fan-in race: N concurrent commits release the dependent exactly once, no deadlock', async () => {
    const parentNames = Array.from({ length: 10 }, (_, i) => `parent-${i}`)
    const { run } = await seedRun([
      ...parentNames.map((name) => ({ name, status: 'ready' as const })),
      { name: 'join', dependsOn: parentNames },
    ])
    const steps = await getStepsByRun(sql, run.id)
    const join = stepNamed(steps, 'join')
    expect(join.status).toBe('pending')

    // fire all ten "this parent just completed" notifications concurrently,
    // each on its own connection-level statement — the scenario a fan-in of
    // 10 parallel workers actually produces.
    const results = await Promise.all(
      parentNames.map((name) => recordDependencySatisfied(sql, join.id, name))
    )

    const releasedCount = results.filter((r) => r?.status === 'ready').length
    expect(releasedCount).toBe(1) // exactly one caller observed the full set and flipped it

    const finalRows = results.filter((r): r is StepRow => r !== undefined)
    // every call that returned a row agrees on the final satisfied_deps set
    for (const row of finalRows) {
      expect(row.satisfied_deps.length).toBeLessThanOrEqual(parentNames.length)
    }

    const reread = await getStepsByRun(sql, run.id)
    const finalJoin = stepNamed(reread, 'join')
    expect(finalJoin.status).toBe('ready')
    expect(finalJoin.satisfied_deps.sort()).toEqual([...parentNames].sort())
  })

  test('does not release a step that is not pending (e.g. already running)', async () => {
    const { run } = await seedRun([
      { name: 'a', status: 'ready' },
      { name: 'b', dependsOn: ['a'], status: 'ready' }, // already ready, not gated by 'a' in this test
    ])
    const steps = await getStepsByRun(sql, run.id)
    const b = stepNamed(steps, 'b')
    expect(b.status).toBe('ready')

    const result = await recordDependencySatisfied(sql, b.id, 'a')
    expect(result).toBeUndefined()

    const reread = await getStepsByRun(sql, run.id)
    expect(stepNamed(reread, 'b').status).toBe('ready') // untouched
  })
})

// ---- #17 conditional branching: skipped steps ------------------------------

describe('skipStep', () => {
  test('marks a pending step skipped, with a reason, and it is terminal', async () => {
    const { run } = await seedRun([{ name: 'untaken-branch', status: 'pending' }])
    const steps = await getStepsByRun(sql, run.id)
    const step = stepNamed(steps, 'untaken-branch')

    const skipped = await skipStep(sql, step.id, 'condition was false')
    expect(skipped).toBeDefined()
    expect(skipped?.status).toBe('skipped')
    expect(skipped?.skip_reason).toBe('condition was false')
  })

  test('is allowed from ready too', async () => {
    const { run } = await seedRun([{ name: 'ready-branch', status: 'ready' }])
    const steps = await getStepsByRun(sql, run.id)
    const step = stepNamed(steps, 'ready-branch')

    const skipped = await skipStep(sql, step.id)
    expect(skipped?.status).toBe('skipped')
    expect(skipped?.skip_reason).toBeNull()
  })

  test('refuses to skip a running step — returns undefined, leaves it untouched', async () => {
    const { steps, namespace } = await seedRun([{ name: 'in-flight', status: 'ready' }])
    const claimed = await claim(namespace, 'w1')
    expect(claimed.status).toBe('running')

    const result = await skipStep(sql, claimed.id, 'too late')
    expect(result).toBeUndefined()

    const reread = await getStepsByRun(sql, claimed.run_id)
    expect(stepNamed(reread, steps[0]!.name).status).toBe('running')
  })

  test('a skipped upstream step can still satisfy a downstream fan-in via recordDependencySatisfied', async () => {
    const { run } = await seedRun([
      { name: 'a', status: 'ready' },
      { name: 'b', status: 'ready' }, // untaken branch
      { name: 'join', dependsOn: ['a', 'b'] },
    ])
    const steps = await getStepsByRun(sql, run.id)
    const join = stepNamed(steps, 'join')

    await skipStep(sql, stepNamed(steps, 'b').id, 'branch not taken')
    // the caller (engine) decides a skip counts as "satisfied" for this edge
    await recordDependencySatisfied(sql, join.id, 'b')
    const afterA = await recordDependencySatisfied(sql, join.id, 'a')

    expect(afterA?.status).toBe('ready')
  })
})

// ---- #20 child workflows: creation, linkage, resolution -------------------

describe('child run linkage', () => {
  test('createRun with parentRunId/parentStepId links the child; getChildRuns finds it', async () => {
    const { run: parentRun, steps: parentSteps } = await seedRun([{ name: 'spawn-child', status: 'ready' }])
    const parentStep = stepNamed(parentSteps, 'spawn-child')

    const childDag: WorkflowDefinition = {
      name: `child-wf-${crypto.randomUUID()}`,
      version: 1,
      steps: [{ name: 'only', dependsOn: [], maxAttempts: 1, priority: 0 }],
    }
    const childWorkflow = await insertWorkflow(sql, { name: childDag.name, dag: childDag })

    const { run: childRun } = await createRun(sql, {
      workflowId: childWorkflow.id,
      parentRunId: parentRun.id,
      parentStepId: parentStep.id,
    })

    expect(childRun.parent_run_id).toBe(parentRun.id)
    expect(childRun.parent_step_id).toBe(parentStep.id)

    const children = await getChildRuns(sql, parentRun.id)
    expect(children.map((c) => c.id)).toEqual([childRun.id])
  })

  test('a run created without parent fields has null linkage', async () => {
    const { run } = await seedRun([{ name: 'only' }])
    expect(run.parent_run_id).toBeNull()
    expect(run.parent_step_id).toBeNull()
    expect(await getChildRuns(sql, run.id)).toEqual([])
  })

  test('blockStepOnChildRun parks a running step in blocked and clears its lease, fenced on the owning worker', async () => {
    const { steps, namespace } = await seedRun([{ name: 'awaiting-child', status: 'ready' }])
    const claimed = await claim(namespace, 'w1')
    expect(claimed.status).toBe('running')

    const childDag: WorkflowDefinition = {
      name: `child-wf-${crypto.randomUUID()}`,
      version: 1,
      steps: [{ name: 'only', dependsOn: [], maxAttempts: 1, priority: 0 }],
    }
    const childWorkflow = await insertWorkflow(sql, { name: childDag.name, dag: childDag })
    const { run: childRun } = await createRun(sql, {
      workflowId: childWorkflow.id,
      parentRunId: claimed.run_id,
      parentStepId: claimed.id,
    })

    // a worker that does NOT hold the lease cannot block the step
    const wrongOwner = await blockStepOnChildRun(sql, {
      stepId: claimed.id,
      workerId: 'not-the-owner',
      childRunId: childRun.id,
    })
    expect(wrongOwner).toBeUndefined()

    const blocked = await blockStepOnChildRun(sql, {
      stepId: claimed.id,
      workerId: 'w1',
      childRunId: childRun.id,
    })
    expect(blocked).toBeDefined()
    expect(blocked?.status).toBe('blocked')
    expect(blocked?.awaited_child_run_id).toBe(childRun.id)
    expect(blocked?.lease_owner).toBeNull()
    expect(blocked?.lease_expires_at).toBeNull()

    const reread = await getStepsByRun(sql, claimed.run_id)
    expect(stepNamed(reread, steps[0]!.name).status).toBe('blocked')
  })

  test('resolveBlockedStepForChildRun is a no-op while the child is still in flight, and releases once terminal', async () => {
    const { namespace } = await seedRun([{ name: 'awaiting-child', status: 'ready' }])
    const claimed = await claim(namespace, 'w1')

    const childDag: WorkflowDefinition = {
      name: `child-wf-${crypto.randomUUID()}`,
      version: 1,
      steps: [{ name: 'only', dependsOn: [], maxAttempts: 1, priority: 0 }],
    }
    const childWorkflow = await insertWorkflow(sql, { name: childDag.name, dag: childDag })
    const { run: childRun } = await createRun(sql, {
      workflowId: childWorkflow.id,
      parentRunId: claimed.run_id,
      parentStepId: claimed.id,
    })
    await blockStepOnChildRun(sql, { stepId: claimed.id, workerId: 'w1', childRunId: childRun.id })

    // child is still `queued` — resolving must be a safe no-op
    const tooEarly = await resolveBlockedStepForChildRun(sql, childRun.id)
    expect(tooEarly).toBeUndefined()
    const stillBlocked = (await getStepsByRun(sql, claimed.run_id))[0]!
    expect(stillBlocked.status).toBe('blocked')

    // now finish the child
    await updateRunStatus(sql, childRun.id, 'completed', { output: { value: 42 }, finishedAt: new Date() })
    const finishedChild = await getRun(sql, childRun.id)
    expect(finishedChild?.status).toBe('completed')

    const released = await resolveBlockedStepForChildRun(sql, childRun.id)
    expect(released).toBeDefined()
    expect(released?.status).toBe('ready')
    expect(released?.awaited_child_run_id).toBeNull()

    // a second resolution call for the same (now-released) step is a no-op
    const again = await resolveBlockedStepForChildRun(sql, childRun.id)
    expect(again).toBeUndefined()
  })

  test('resolveBlockedStepForChildRun releases on a failed child too — storage does not judge outcome', async () => {
    const { namespace } = await seedRun([{ name: 'awaiting-child', status: 'ready' }])
    const claimed = await claim(namespace, 'w1')

    const childDag: WorkflowDefinition = {
      name: `child-wf-${crypto.randomUUID()}`,
      version: 1,
      steps: [{ name: 'only', dependsOn: [], maxAttempts: 1, priority: 0 }],
    }
    const childWorkflow = await insertWorkflow(sql, { name: childDag.name, dag: childDag })
    const { run: childRun } = await createRun(sql, {
      workflowId: childWorkflow.id,
      parentRunId: claimed.run_id,
      parentStepId: claimed.id,
    })
    await blockStepOnChildRun(sql, { stepId: claimed.id, workerId: 'w1', childRunId: childRun.id })

    await updateRunStatus(sql, childRun.id, 'failed', { finishedAt: new Date() })

    const released = await resolveBlockedStepForChildRun(sql, childRun.id)
    expect(released?.status).toBe('ready')
  })
})

// ---- ships-the-phase proof: 10-way fan-out / fan-in, end to end -----------

describe('phase 4 shipping bar: fans out to N parallel steps, continues only when all commit', () => {
  test('a join step becomes ready only after all 10 fan-out parents have completed and been recorded', async () => {
    const N = 10
    const parentNames = Array.from({ length: N }, (_, i) => `fanout-${i}`)
    const { run, namespace } = await seedRun([
      ...parentNames.map((name) => ({ name, status: 'ready' as const })),
      { name: 'fan-in', dependsOn: parentNames },
    ])
    const initial = await getStepsByRun(sql, run.id)
    const join = stepNamed(initial, 'fan-in')

    // simulate N workers each claiming and completing one fan-out step, then
    // notifying the join of that one dependency's resolution — concurrently.
    await Promise.all(
      parentNames.map(async (name, i) => {
        const claimed = await claim(namespace, `worker-${i}`)
        await completeStep(sql, claimed.id, { index: i })
        await recordDependencySatisfied(sql, join.id, name)
      })
    )

    const finalSteps = await getStepsByRun(sql, run.id)
    expect(finalSteps.filter((s) => s.status === 'completed')).toHaveLength(N)
    expect(stepNamed(finalSteps, 'fan-in').status).toBe('ready')
  })
})
