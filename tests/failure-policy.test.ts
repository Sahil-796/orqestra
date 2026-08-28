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
import { startRun, enqueueRun, resumeRun } from '../src/engine/executor.ts'
import {
  getRun,
  getStepsByRun,
  getRunFailurePolicy,
  updateRunStatus,
  failStep,
} from '../src/store/repositories.ts'
import { serializeError } from '../src/types.ts'

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

  // Regression: the inline driver commits a step's `failStep` and the run's
  // dead-letter write in SEPARATE transactions, so a crash between them (or any
  // resumeAll over the resulting state) can re-enter executeRun on a fail_fast
  // run that is `running` with an already-`failed` step and no runnable work.
  // That branch must dead-letter — NOT finalize as `completed_with_errors`,
  // which is a continue_on_error-only terminal state and would hide the run
  // from the operator DLQ + manual retry.
  test('fail_fast: a stranded failed step on resume dead-letters, not completed_with_errors', async () => {
    const wf = defineWorkflow(`policy-strand-${crypto.randomUUID()}`, (builder) => {
      builder.step('boom', async () => 'never runs on resume')
      builder.step('after', async () => 'ran', { dependsOn: ['boom'] })
    })
    expect(wf.failurePolicy).toBe('fail_fast')

    // Create the run + steps without executing (durable mode), then hand-build
    // the exact state the crash window leaves behind: run `running`, `boom`
    // `failed`, `after` still `pending`.
    const { runId } = await enqueueRun(sql, wf)
    await updateRunStatus(sql, runId, 'running', { startedAt: new Date() })
    const steps = await getStepsByRun(sql, runId)
    const boom = steps.find((s) => s.name === 'boom')!
    await failStep(sql, boom.id, serializeError(new Error('boom exploded')))

    const result = await resumeRun(sql, wf, runId)
    expect(result.status).toBe('dead_letter')

    const run = await getRun(sql, runId)
    expect(run?.status).toBe('dead_letter')
    expect(run?.dead_letter_reason).toContain('boom')
    expect(run?.dead_lettered_at).not.toBeNull()
  })
})
