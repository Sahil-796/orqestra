import { describe, expect, test, beforeAll, afterAll } from 'bun:test'
import { createDb } from '../src/store/client.ts'
import { migrate } from '../src/store/migrate.ts'
import {
  insertWorkflow,
  createRun,
  getRun,
  insertSteps,
  getStepsByRun,
  updateRunStatus,
  failStep,
  cancelPendingSteps,
  deadLetterRun,
  listDeadLetterRuns,
  getDeadLetterRun,
  resetRunForRetry,
  setRunFailurePolicy,
  getRunFailurePolicy,
  recordCompensation,
  getExecutedCompensations,
  hasCompensationRun,
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
// Postgres — same discipline as repositories.test.ts.
interface Seed {
  run: RunRow
  steps: StepRow[]
}

async function seedRun(specs: Partial<NewStep>[]): Promise<Seed> {
  const name = `failure-store-wf-${crypto.randomUUID()}`
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
  const { run } = await createRun(sql, { workflowId: workflow.id })
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
  return { run, steps }
}

describe('deadLetterRun', () => {
  test('parks a failed run in dead_letter with reason + timestamp', async () => {
    const { run } = await seedRun([{ name: 'a' }])
    await updateRunStatus(sql, run.id, 'failed')

    const dead = await deadLetterRun(sql, run.id, 'retries exhausted')
    expect(dead).toBeDefined()
    expect(dead?.status).toBe('dead_letter')
    expect(dead?.dead_letter_reason).toBe('retries exhausted')
    expect(dead?.dead_lettered_at).not.toBeNull()
    expect(dead?.finished_at).not.toBeNull()
  })

  test('is a no-op on an already-terminal-clean run and on a second call', async () => {
    const { run } = await seedRun([{ name: 'a' }])
    await updateRunStatus(sql, run.id, 'completed')

    // completed run cannot be dead-lettered
    expect(await deadLetterRun(sql, run.id, 'nope')).toBeUndefined()

    // a fresh failed run can be dead-lettered exactly once
    const { run: run2 } = await seedRun([{ name: 'a' }])
    await updateRunStatus(sql, run2.id, 'failed')
    const first = await deadLetterRun(sql, run2.id, 'first')
    expect(first?.dead_letter_reason).toBe('first')
    // second call matches zero rows — reason/timestamp are not overwritten
    expect(await deadLetterRun(sql, run2.id, 'second')).toBeUndefined()
    const reread = await getRun(sql, run2.id)
    expect(reread?.dead_letter_reason).toBe('first')
  })

  test('listDeadLetterRuns and getDeadLetterRun surface parked runs', async () => {
    const { run } = await seedRun([{ name: 'a' }])
    await updateRunStatus(sql, run.id, 'failed')
    await deadLetterRun(sql, run.id, 'for listing')

    const listed = await listDeadLetterRuns(sql, 500)
    expect(listed.some((r) => r.id === run.id)).toBe(true)

    const fetched = await getDeadLetterRun(sql, run.id)
    expect(fetched?.id).toBe(run.id)

    // a run that isn't dead-lettered is invisible to getDeadLetterRun
    const { run: live } = await seedRun([{ name: 'a' }])
    expect(await getDeadLetterRun(sql, live.id)).toBeUndefined()
  })
})

describe('resetRunForRetry', () => {
  test('round-trip: dead_letter run + failed/cancelled steps become re-runnable', async () => {
    // step 'a' fails, 'b' depends on 'a' and gets cancelled when the run aborts,
    // 'c' completed and must be preserved across the retry.
    const { run, steps } = await seedRun([
      { name: 'a' },
      { name: 'b', dependsOn: ['a'], status: 'pending' },
      { name: 'c' },
    ])
    const a = steps.find((s) => s.name === 'a')!
    const c = steps.find((s) => s.name === 'c')!

    await failStep(sql, a.id, serializeError(new Error('boom')))
    // c succeeded
    await sql`update step set status = 'completed' where id = ${c.id}`
    // aborting the run cancels the remaining pending step 'b'
    await cancelPendingSteps(sql, run.id)
    await updateRunStatus(sql, run.id, 'failed')
    await deadLetterRun(sql, run.id, 'exhausted')

    const revived = await resetRunForRetry(sql, run.id)
    expect(revived).toBeDefined()
    expect(revived?.status).toBe('queued')
    expect(revived?.dead_lettered_at).toBeNull()
    expect(revived?.dead_letter_reason).toBeNull()
    expect(revived?.finished_at).toBeNull()

    const after = await getStepsByRun(sql, run.id)
    const byName = (n: string) => after.find((s) => s.name === n)!
    // failed step 'a' → ready, fresh attempt budget, error cleared
    expect(byName('a').status).toBe('ready')
    expect(byName('a').attempt).toBe(0)
    expect(byName('a').error).toBeNull()
    // cancelled step 'b' (has a dep) → pending, to be released on re-run
    expect(byName('b').status).toBe('pending')
    // completed step 'c' preserved — never re-run
    expect(byName('c').status).toBe('completed')
  })

  test('is a no-op on a run that is not dead-lettered', async () => {
    const { run } = await seedRun([{ name: 'a' }])
    await updateRunStatus(sql, run.id, 'failed')
    expect(await resetRunForRetry(sql, run.id)).toBeUndefined()
  })
})

describe('failure policy', () => {
  test('defaults to fail_fast and can be set/read back', async () => {
    const { run } = await seedRun([{ name: 'a' }])
    // createRun never sets it, so getRunFailurePolicy coerces null → fail_fast
    expect(await getRunFailurePolicy(sql, run.id)).toBe('fail_fast')

    const updated = await setRunFailurePolicy(sql, run.id, 'continue_on_error')
    expect(updated?.failure_policy).toBe('continue_on_error')
    expect(await getRunFailurePolicy(sql, run.id)).toBe('continue_on_error')

    expect(await getRunFailurePolicy(sql, crypto.randomUUID())).toBeUndefined()
  })
})

describe('compensation idempotency', () => {
  test('records once; a re-execution never records or runs the same compensation twice', async () => {
    const { run } = await seedRun([{ name: 'charge' }])

    // first record wins and would perform the real side effect (created: true)
    const first = await recordCompensation(sql, {
      runId: run.id,
      stepName: 'charge',
      result: { refunded: true },
    })
    expect(first.created).toBe(true)
    expect(first.compensation.step_name).toBe('charge')
    expect(first.compensation.status).toBe('executed')

    // a replay / manual retry tries again — no second row, no second refund
    const second = await recordCompensation(sql, {
      runId: run.id,
      stepName: 'charge',
      result: { refunded: 'AGAIN' },
    })
    expect(second.created).toBe(false)
    expect(second.compensation.id).toBe(first.compensation.id)
    // the original result is preserved, not clobbered by the retry
    expect(second.compensation.result).toEqual({ refunded: true })

    const all = await getExecutedCompensations(sql, run.id)
    expect(all.filter((c) => c.step_name === 'charge')).toHaveLength(1)

    expect(await hasCompensationRun(sql, run.id, 'charge')).toBe(true)
    expect(await hasCompensationRun(sql, run.id, 'never-ran')).toBe(false)
  })

  test('distinct steps in the same run each get their own compensation row', async () => {
    const { run } = await seedRun([{ name: 'a' }, { name: 'b' }])
    await recordCompensation(sql, { runId: run.id, stepName: 'a' })
    await recordCompensation(sql, {
      runId: run.id,
      stepName: 'b',
      status: 'failed',
      error: serializeError(new Error('refund failed')),
    })

    const all = await getExecutedCompensations(sql, run.id)
    expect(all).toHaveLength(2)
    const b = all.find((c) => c.step_name === 'b')!
    expect(b.status).toBe('failed')
    expect(b.error).toBeDefined()
  })
})
