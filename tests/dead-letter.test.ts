// Phase 7 #26 — dead-letter queue. When a run exhausts a step's retry budget
// (the terminal-failure point in the inline executor), the run is routed to the
// DLQ (status `dead_letter`, with a reason + timestamp) instead of being left
// silently `failed`. These tests drive the inline `startRun`/`executeRun` path
// under the default `fail_fast` policy.

import { describe, expect, test, beforeAll, afterAll } from 'bun:test'
import { createDb } from '../src/store/client.ts'
import { migrate } from '../src/store/migrate.ts'
import { defineWorkflow } from '../src/define/workflow.ts'
import { startRun } from '../src/engine/executor.ts'
import {
  getRun,
  getStepsByRun,
  getDeadLetterRun,
  listDeadLetterRuns,
} from '../src/store/repositories.ts'

const sql = createDb()

beforeAll(async () => {
  await migrate(sql)
})

afterAll(async () => {
  await sql.end()
})

describe('dead-letter queue (#26)', () => {
  test('a run whose step exhausts its retry budget is dead-lettered, not left failed', async () => {
    const wf = defineWorkflow(`dlq-basic-${crypto.randomUUID()}`, (builder) => {
      builder.step('boom', async () => {
        throw new Error('kaboom')
      })
    })

    const result = await startRun(sql, wf)

    // The returned result and the persisted row both report dead_letter.
    expect(result.status).toBe('dead_letter')

    const run = await getRun(sql, result.runId)
    expect(run?.status).toBe('dead_letter')
    expect(run?.dead_lettered_at).not.toBeNull()
    expect(run?.dead_letter_reason).toContain('boom')
    expect(run?.finished_at).not.toBeNull()

    // The step itself is `failed`; only the RUN moves to the DLQ.
    const steps = await getStepsByRun(sql, result.runId)
    expect(steps.find((s) => s.name === 'boom')?.status).toBe('failed')

    // It shows up on the operator listing + drill-down surfaces.
    const one = await getDeadLetterRun(sql, result.runId)
    expect(one?.id).toBe(result.runId)
    const listed = await listDeadLetterRuns(sql)
    expect(listed.some((r) => r.id === result.runId)).toBe(true)
  })

  test('the retry budget is actually spent before dead-lettering (maxAttempts attempts)', async () => {
    let attempts = 0
    const wf = defineWorkflow(`dlq-retries-${crypto.randomUUID()}`, (builder) => {
      builder.step(
        'flaky',
        async () => {
          attempts += 1
          throw new Error(`attempt ${attempts} failed`)
        },
        { maxAttempts: 3 }
      )
    })

    const result = await startRun(sql, wf)

    expect(result.status).toBe('dead_letter')
    expect(attempts).toBe(3) // tried its whole budget, then gave up

    const run = await getRun(sql, result.runId)
    expect(run?.dead_letter_reason).toContain('3 attempt')
  })

  test('a step that recovers within its retry budget completes — no DLQ', async () => {
    let attempts = 0
    const wf = defineWorkflow(`dlq-recovers-${crypto.randomUUID()}`, (builder) => {
      builder.step(
        'transient',
        async () => {
          attempts += 1
          if (attempts < 2) throw new Error('transient blip')
          return 'ok'
        },
        { maxAttempts: 3 }
      )
    })

    const result = await startRun(sql, wf)

    expect(result.status).toBe('completed')
    expect(attempts).toBe(2)
    expect((result.output as Record<string, unknown>).transient).toBe('ok')

    const run = await getRun(sql, result.runId)
    expect(run?.status).toBe('completed')
    expect(run?.dead_lettered_at).toBeNull()
  })
})
