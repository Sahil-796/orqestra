// Standalone fixture, run as its own `bun` subprocess by
// tests/crash-recovery.test.ts. Defines a two-step workflow (a -> b) and
// starts a single run of it.
//
// Step `a` durably appends "A\n" to ORQ_CRASH_FILE — proof it ran, and ran
// exactly once (the resumed run must never re-run a completed step).
//
// Step `b` depends on `a`. When ORQ_CRASH=1 it hard-kills this process with
// SIGKILL the moment it starts — i.e. AFTER `a`'s db-commit + file write
// (executeRun always finishes running `a` to completion, tx and all, before
// `b` becomes ready) but BEFORE `b` writes anything or commits. That's a
// real `kill -9`, not a simulated crash. On the resume pass ORQ_CRASH is
// unset, so `b` instead appends "B\n" and completes normally.
//
// Required env vars: ORQ_CRASH_FILE, ORQ_WORKFLOW_NAME. Optional:
// ORQ_IDEMPOTENCY_KEY, ORQ_CRASH ('1' to crash in step b).

import { appendFileSync } from 'node:fs'
import { defineWorkflow } from '../../src/define/workflow.ts'
import { startRun } from '../../src/engine/executor.ts'
import { createDb } from '../../src/store/client.ts'

const file = process.env.ORQ_CRASH_FILE
const workflowName = process.env.ORQ_WORKFLOW_NAME

if (!file) throw new Error('crash-workflow fixture: ORQ_CRASH_FILE is required')
if (!workflowName) throw new Error('crash-workflow fixture: ORQ_WORKFLOW_NAME is required')

const wf = defineWorkflow(workflowName, (builder) => {
  builder.step('a', async () => {
    appendFileSync(file, 'A\n')
    return 'a-done'
  })

  builder.step(
    'b',
    async () => {
      if (process.env.ORQ_CRASH === '1') {
        // Real kill -9 of this process. Block forever afterward so no
        // further JS executes before the OS actually tears us down —
        // step `b` must not write anything or commit on this pass.
        process.kill(process.pid, 'SIGKILL')
        await new Promise(() => {})
      }
      appendFileSync(file, 'B\n')
      return 'b-done'
    },
    { dependsOn: ['a'] }
  )
})

const sql = createDb()
await startRun(sql, wf, { idempotencyKey: process.env.ORQ_IDEMPOTENCY_KEY })
await sql.end()
