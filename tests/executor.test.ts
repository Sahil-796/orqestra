import { describe, expect, test, beforeAll, afterAll } from 'bun:test'
import { createDb } from '../src/store/client.ts'
import { migrate } from '../src/store/migrate.ts'
import { defineWorkflow } from '../src/define/workflow.ts'
import { startRun } from '../src/engine/executor.ts'
import { getStepsByRun } from '../src/store/repositories.ts'

const sql = createDb()

beforeAll(async () => {
  await migrate(sql)
})

afterAll(async () => {
  await sql.end()
})

describe('executor', () => {
  test('runs a multi-step DAG (fan-out + fan-in) to completion', async () => {
    // a -> b -> d
    //   -> c -> d   (d fans in on both b and c)
    const wf = defineWorkflow(`executor-dag-test-${crypto.randomUUID()}`, (builder) => {
      builder.step('a', async () => 1)
      builder.step('b', async (ctx) => (ctx.input as { base: number }).base + 10, {
        dependsOn: ['a'],
      })
      builder.step('c', async () => 100, { dependsOn: ['a'] })
      builder.step('d', async () => 'done', { dependsOn: ['b', 'c'] })
    })

    const result = await startRun(sql, wf, { input: { base: 5 } })

    expect(result.status).toBe('completed')
    expect(result.output).toEqual({ a: 1, b: 15, c: 100, d: 'done' })

    const steps = await getStepsByRun(sql, result.runId)
    expect(steps).toHaveLength(4)
    for (const step of steps) {
      expect(step.status).toBe('completed')
      expect(step.result).toEqual({ ok: true, value: (result.output as Record<string, unknown>)[step.name] })
    }
  })

  test('dead-letters the run fast when a step throws, without running downstream steps', async () => {
    let downstreamRan = false

    const wf = defineWorkflow(`executor-fail-fast-test-${crypto.randomUUID()}`, (builder) => {
      builder.step('boom', async () => {
        throw new Error('kaboom')
      })
      builder.step(
        'never',
        async () => {
          downstreamRan = true
          return 'unreachable'
        },
        { dependsOn: ['boom'] }
      )
    })

    const result = await startRun(sql, wf)

    // Phase 7 #26: a fail_fast run that exhausts its retry budget now lands in
    // the dead-letter queue instead of a bare `failed`. The step row itself
    // stays `failed`; the run is parked as `dead_letter` for manual retry.
    expect(result.status).toBe('dead_letter')
    expect(downstreamRan).toBe(false)

    const steps = await getStepsByRun(sql, result.runId)
    const boom = steps.find((s) => s.name === 'boom')
    const never = steps.find((s) => s.name === 'never')
    expect(boom?.status).toBe('failed')
    expect(never?.status).toBe('pending')
  })

  test('startRun is idempotent: same idempotencyKey runs the workflow exactly once', async () => {
    let executionCount = 0

    const wf = defineWorkflow(`executor-idempotent-test-${crypto.randomUUID()}`, (builder) => {
      builder.step('count', async () => {
        executionCount += 1
        return executionCount
      })
    })

    const key = `idempotency-key-${crypto.randomUUID()}`

    const first = await startRun(sql, wf, { idempotencyKey: key })
    const second = await startRun(sql, wf, { idempotencyKey: key })

    expect(second.runId).toBe(first.runId)
    expect(first.status).toBe('completed')
    expect(second.status).toBe('completed')
    expect(executionCount).toBe(1)

    const steps = await getStepsByRun(sql, first.runId)
    expect(steps).toHaveLength(1)
    expect(steps[0]?.attempt).toBe(1)
  })
})
