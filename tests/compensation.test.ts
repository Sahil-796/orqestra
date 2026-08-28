// Phase 7 #29 — saga compensation / rollback. A step registers a rollback via
// `ctx.compensate(fn)`. When a LATER step fails the run terminally under
// fail_fast, the executor runs the compensations of every COMPLETED step in
// reverse order, each guarded by the idempotent compensation log so a re-driven
// run never issues the same rollback twice.
//
// The shipping example: charge -> provision; provision fails; charge's refund
// runs automatically, exactly once — proven across an original drive AND a
// simulated re-drive.

import { describe, expect, test, beforeAll, afterAll } from 'bun:test'
import { createDb } from '../src/store/client.ts'
import { migrate } from '../src/store/migrate.ts'
import { defineWorkflow } from '../src/define/workflow.ts'
import { startRun, executeRun } from '../src/engine/executor.ts'
import {
  getRun,
  getStepsByRun,
  getExecutedCompensations,
  resetRunForRetry,
} from '../src/store/repositories.ts'

const sql = createDb()

beforeAll(async () => {
  await migrate(sql)
})

afterAll(async () => {
  await sql.end()
})

describe('saga compensation (#29)', () => {
  test('charge -> provision: provision fails, charge refund runs exactly once (incl. re-drive)', async () => {
    let charged = 0
    let refunds = 0

    const wf = defineWorkflow(`saga-charge-provision-${crypto.randomUUID()}`, (builder) => {
      builder.step('charge', async (ctx) => {
        charged += 1
        // Register the rollback for THIS step. It only runs if a later step
        // dooms the run.
        ctx.compensate(() => {
          refunds += 1
        })
        return { chargeId: 'ch_1' }
      })
      builder.step(
        'provision',
        async () => {
          throw new Error('provisioning failed')
        },
        { dependsOn: ['charge'] }
      )
    })

    // --- original drive: provision fails, refund fires once ------------------
    const first = await startRun(sql, wf)
    expect(first.status).toBe('dead_letter')
    expect(charged).toBe(1)
    expect(refunds).toBe(1) // refund ran automatically, exactly once

    const comps = await getExecutedCompensations(sql, first.runId)
    expect(comps).toHaveLength(1)
    expect(comps[0]?.step_name).toBe('charge')
    expect(comps[0]?.status).toBe('executed')

    const steps = await getStepsByRun(sql, first.runId)
    expect(steps.find((s) => s.name === 'charge')?.status).toBe('completed')
    expect(steps.find((s) => s.name === 'provision')?.status).toBe('failed')

    // --- simulated re-drive: revive out of the DLQ and run again -------------
    // charge stays `completed` (its successful side effect must survive), only
    // provision is revived. provision fails again — but the refund must NOT run
    // a second time (idempotency via the compensation log).
    await resetRunForRetry(sql, first.runId)
    const second = await executeRun(sql, wf, first.runId)

    expect(second.status).toBe('dead_letter')
    expect(charged).toBe(1) // charge was memoized, not re-run
    expect(refunds).toBe(1) // STILL exactly one refund after the re-drive

    // Still exactly one compensation row on record.
    expect(await getExecutedCompensations(sql, first.runId)).toHaveLength(1)
  })

  test('completed steps unwind in reverse order', async () => {
    const order: string[] = []

    // s1 -> s2 -> boom.  Completion order: s1, s2.  Unwind order: s2, s1.
    const wf = defineWorkflow(`saga-order-${crypto.randomUUID()}`, (builder) => {
      builder.step('s1', async (ctx) => {
        ctx.compensate(() => {
          order.push('undo-s1')
        })
        return 1
      })
      builder.step(
        's2',
        async (ctx) => {
          ctx.compensate(() => {
            order.push('undo-s2')
          })
          return 2
        },
        { dependsOn: ['s1'] }
      )
      builder.step(
        'boom',
        async () => {
          throw new Error('boom')
        },
        { dependsOn: ['s2'] }
      )
    })

    const result = await startRun(sql, wf)
    expect(result.status).toBe('dead_letter')
    expect(order).toEqual(['undo-s2', 'undo-s1']) // reverse of completion
  })

  test('a compensation that itself throws is recorded as failed; run still dead-letters', async () => {
    const wf = defineWorkflow(`saga-comp-throws-${crypto.randomUUID()}`, (builder) => {
      builder.step('charge', async (ctx) => {
        ctx.compensate(() => {
          throw new Error('refund gateway down')
        })
        return 'charged'
      })
      builder.step(
        'provision',
        async () => {
          throw new Error('provision failed')
        },
        { dependsOn: ['charge'] }
      )
    })

    const result = await startRun(sql, wf)
    expect(result.status).toBe('dead_letter')

    const comps = await getExecutedCompensations(sql, result.runId)
    expect(comps).toHaveLength(1)
    expect(comps[0]?.step_name).toBe('charge')
    expect(comps[0]?.status).toBe('failed')
    expect(comps[0]?.error).not.toBeNull()

    const run = await getRun(sql, result.runId)
    expect(run?.status).toBe('dead_letter')
  })

  test('compensations do NOT run on a clean, fully-successful run', async () => {
    let refunds = 0
    const wf = defineWorkflow(`saga-happy-${crypto.randomUUID()}`, (builder) => {
      builder.step('charge', async (ctx) => {
        ctx.compensate(() => {
          refunds += 1
        })
        return 'charged'
      })
      builder.step('provision', async () => 'provisioned', { dependsOn: ['charge'] })
    })

    const result = await startRun(sql, wf)
    expect(result.status).toBe('completed')
    expect(refunds).toBe(0) // happy path never rolls back

    expect(await getExecutedCompensations(sql, result.runId)).toHaveLength(0)
  })
})
