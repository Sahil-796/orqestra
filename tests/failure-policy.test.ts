// Phase 7 #28 — failure policies. A workflow declares `failurePolicy` at
// definition time (default `fail_fast`). The executor reads it from the
// in-process handle and stamps it onto the run row for observability.
//
//   fail_fast          — first terminal step failure rolls back + dead-letters.
//   continue_on_error  — a failed step is left failed, independent steps keep
//                        running, and the run finishes `completed_with_errors`.

import { describe, expect, test, beforeAll, afterAll } from 'bun:test'
import { createDb } from '../src/store/client.ts'
import { migrate } from '../src/store/migrate.ts'
import { defineWorkflow } from '../src/define/workflow.ts'
import { startRun } from '../src/engine/executor.ts'
import { getRun, getStepsByRun, getRunFailurePolicy } from '../src/store/repositories.ts'

const sql = createDb()

beforeAll(async () => {
  await migrate(sql)
})

afterAll(async () => {
  await sql.end()
})

describe('failure policies (#28)', () => {
  test('default is fail_fast — persisted on the run, first failure dead-letters', async () => {
    const wf = defineWorkflow(`policy-default-${crypto.randomUUID()}`, (builder) => {
      builder.step('boom', async () => {
        throw new Error('nope')
      })
      builder.step('after', async () => 'ran', { dependsOn: ['boom'] })
    })

    expect(wf.failurePolicy).toBe('fail_fast')

    const result = await startRun(sql, wf)
    expect(result.status).toBe('dead_letter')

    // Persisted for observability even though the executor's source of truth is
    // the handle.
    expect(await getRunFailurePolicy(sql, result.runId)).toBe('fail_fast')

    const steps = await getStepsByRun(sql, result.runId)
    expect(steps.find((s) => s.name === 'boom')?.status).toBe('failed')
    expect(steps.find((s) => s.name === 'after')?.status).toBe('pending') // never ran
  })

  test('continue_on_error keeps independent steps running and ends completed_with_errors', async () => {
    let independentRan = false
    let dependentRan = false

    // a (fails)   -> c (depends on a — must NOT run)
    // b (succeeds, independent of a — MUST run)
    const wf = defineWorkflow(
      `policy-continue-${crypto.randomUUID()}`,
      (builder) => {
        builder.step('a', async () => {
          throw new Error('a exploded')
        })
        builder.step('b', async () => {
          independentRan = true
          return 'b-value'
        })
        builder.step(
          'c',
          async () => {
            dependentRan = true
            return 'c-value'
          },
          { dependsOn: ['a'] }
        )
      },
      { failurePolicy: 'continue_on_error' }
    )

    expect(wf.failurePolicy).toBe('continue_on_error')

    const result = await startRun(sql, wf)

    expect(result.status).toBe('completed_with_errors')
    expect(independentRan).toBe(true) // b ran despite a's failure
    expect(dependentRan).toBe(false) // c never ran — its dep failed

    // Output carries the successful step's value; the failed/never-run steps
    // contribute undefined.
    expect((result.output as Record<string, unknown>).b).toBe('b-value')

    const run = await getRun(sql, result.runId)
    expect(run?.status).toBe('completed_with_errors')
    expect(run?.dead_lettered_at).toBeNull() // NOT dead-lettered
    expect(await getRunFailurePolicy(sql, result.runId)).toBe('continue_on_error')

    const steps = await getStepsByRun(sql, result.runId)
    expect(steps.find((s) => s.name === 'a')?.status).toBe('failed')
    expect(steps.find((s) => s.name === 'b')?.status).toBe('completed')
    expect(steps.find((s) => s.name === 'c')?.status).toBe('pending')
  })

  test('continue_on_error with no failures still completes cleanly', async () => {
    const wf = defineWorkflow(
      `policy-continue-clean-${crypto.randomUUID()}`,
      (builder) => {
        builder.step('x', async () => 1)
        builder.step('y', async () => 2, { dependsOn: ['x'] })
      },
      { failurePolicy: 'continue_on_error' }
    )

    const result = await startRun(sql, wf)
    expect(result.status).toBe('completed')
    expect(result.output).toEqual({ x: 1, y: 2 })
  })
})
