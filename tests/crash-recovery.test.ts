// The headline Phase 1 "ships" test: a workflow that crashes halfway
// (a real `kill -9`, not a simulated failure) resumes without re-running
// completed steps.
//
// Step `a` durably appends "A" to a file; step `b` (dependsOn `a`) hard-
// kills its own process with SIGKILL on the first pass, before it writes
// or commits anything. We spawn the two-step workflow as its own `bun`
// subprocess (tests/fixtures/crash-workflow.ts), verify the OS actually
// terminated it via SIGKILL, then resume the same run in this process and
// verify `a` was memoized (not re-run) while `b` completed.

import { describe, expect, test, beforeAll, afterAll } from 'bun:test'
import { appendFileSync, existsSync, readFileSync, unlinkSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { createDb } from '../src/store/client.ts'
import { migrate } from '../src/store/migrate.ts'
import { defineWorkflow } from '../src/define/workflow.ts'
import { resumeAll } from '../src/engine/executor.ts'
import { getStepsByRun } from '../src/store/repositories.ts'

const sql = createDb()

const fixturePath = join(import.meta.dir, 'fixtures/crash-workflow.ts')

beforeAll(async () => {
  await migrate(sql)
})

afterAll(async () => {
  await sql.end()
})

describe('crash recovery', () => {
  test('a workflow that crashes mid-run resumes without re-running completed steps', async () => {
    const runKey = crypto.randomUUID()
    const file = join(tmpdir(), `orq-crash-${runKey}.log`)
    const workflowName = `crash-recovery-test-${runKey}`
    const idempotencyKey = `crash-recovery-key-${runKey}`

    try {
      // --- pass 1: real kill -9 partway through the run --------------------
      const proc = Bun.spawn({
        cmd: ['bun', fixturePath],
        env: {
          ...process.env,
          ORQ_CRASH_FILE: file,
          ORQ_WORKFLOW_NAME: workflowName,
          ORQ_IDEMPOTENCY_KEY: idempotencyKey,
          ORQ_CRASH: '1',
        },
        stdout: 'inherit',
        stderr: 'inherit',
      })

      await proc.exited

      // Prove the crash was a real kill -9, not a clean exit or thrown error.
      expect(proc.exitCode).toBeNull()
      expect(proc.signalCode).toBe('SIGKILL')

      expect(existsSync(file)).toBe(true)
      const afterCrash = readFileSync(file, 'utf8')
      expect(afterCrash).toBe('A\n') // a committed; b never wrote or committed

      // --- pass 2: resume in this process -----------------------------------
      // Same workflow name + step fns, registered fresh in this process so
      // resumeAll can find them; ORQ_CRASH is not set here so step `b` runs
      // to completion instead of killing anything.
      const wf = defineWorkflow(workflowName, (builder) => {
        builder.step('a', async () => {
          appendFileSync(file, 'A\n')
          return 'a-done'
        })
        builder.step(
          'b',
          async () => {
            appendFileSync(file, 'B\n')
            return 'b-done'
          },
          { dependsOn: ['a'] }
        )
      })

      const results = await resumeAll(sql, new Map([[workflowName, wf]]))
      expect(results).toHaveLength(1)
      const result = results[0]!
      expect(result.status).toBe('completed')

      const afterResume = readFileSync(file, 'utf8')
      const aCount = (afterResume.match(/^A$/gm) ?? []).length
      const bCount = (afterResume.match(/^B$/gm) ?? []).length
      expect(aCount).toBe(1) // step a was memoized, NOT re-run
      expect(bCount).toBe(1) // step b completed on resume

      const steps = await getStepsByRun(sql, result.runId)
      const stepA = steps.find((s) => s.name === 'a')
      const stepB = steps.find((s) => s.name === 'b')
      expect(stepA?.status).toBe('completed')
      expect(stepB?.status).toBe('completed')
    } finally {
      if (existsSync(file)) unlinkSync(file)
    }
  }, 20_000)
})
