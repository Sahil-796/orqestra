// The Phase 2 "ships" proof (build-plan.html): "3 workers draining one
// queue; kill one mid-step and its lease is reclaimed within the lease
// TTL." Three cases, in the spirit of tests/crash-recovery.test.ts — real
// concurrency and a real `kill -9`, not simulated failures:
//
//   1. Three `createWorker` instances drain one queue of 10 runs (a 4-step
//      diamond DAG each, so dependency ordering actually matters) without
//      any step being double-claimed or double-run, and with the work
//      genuinely spread across more than one worker.
//   2. A worker is spawned as its own `bun` subprocess, claims a step, and
//      is `kill -9`'d mid-step. Its lease is reclaimed within the TTL by a
//      surviving in-process worker, which completes the run.
//   3. A step that fails on attempts 1 and 2 and succeeds on attempt 3
//      self-heals via the retry/backoff path: it ends `completed` with
//      `attempt === 3`, and its `run_after` genuinely moved into the future
//      between attempts (backoff releases the worker instead of pinning it).
//
// Every run in this file gets its own random `namespace` (same isolation
// pattern as tests/queue.test.ts) since the step queue is global and this
// runs against a shared dev Postgres.

import { describe, expect, test, beforeAll, afterAll } from 'bun:test'
import { appendFileSync, existsSync, readFileSync, unlinkSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { createDb, type Db } from '../src/store/client.ts'
import { migrate } from '../src/store/migrate.ts'
import { defineWorkflow } from '../src/define/workflow.ts'
import type { StepFn } from '../src/define/workflow.ts'
import { enqueueRun } from '../src/engine/executor.ts'
import { createWorker, type Worker } from '../src/worker/worker.ts'
import { getRun, getStepsByRun } from '../src/store/repositories.ts'

const sql = createDb()

beforeAll(async () => {
  await migrate(sql)
})

afterAll(async () => {
  await sql.end()
})

/**
 * Poll `check` until it returns true, or throw once `timeoutMs` elapses.
 * Used instead of a fixed `sleep` throughout this file so a broken
 * assertion fails fast rather than hanging the suite.
 */
async function pollUntil(
  check: () => boolean | Promise<boolean>,
  opts: { timeoutMs: number; intervalMs: number }
): Promise<void> {
  const deadline = Date.now() + opts.timeoutMs
  for (;;) {
    if (await check()) return
    if (Date.now() >= deadline) {
      throw new Error(`pollUntil: condition not met within ${opts.timeoutMs}ms`)
    }
    await new Promise((resolve) => setTimeout(resolve, opts.intervalMs))
  }
}

describe('3 workers draining one queue', () => {
  test(
    '10 runs of a 4-step DAG complete with every step run exactly once, spread across workers',
    async () => {
      const namespace = `dist-test-${crypto.randomUUID()}`
      const RUNS = 10
      const STEPS_PER_RUN = 4
      const execFile = join(tmpdir(), `orq-dist-exec-${crypto.randomUUID()}.log`)

      // Each step records its own execution as a durable side effect: one
      // line per (runId, stepName), including which worker's lease owned
      // the row at the moment the step function ran. That's the proof that
      // no step was double-claimed (exactly one line per step) AND that
      // work was genuinely spread across workers (more than one distinct
      // owner across all lines).
      function recordingStep(name: string, value: unknown): StepFn {
        return async (ctx) => {
          const steps = await getStepsByRun(sql, ctx.runId)
          const self = steps.find((s) => s.name === name)
          appendFileSync(execFile, `${ctx.runId}|${name}|${self?.lease_owner ?? 'unknown'}\n`)
          return value
        }
      }

      // A diamond: fetch -> {transform1, transform2} -> finalize. Dependency
      // ordering matters here — transform1/transform2 must not become
      // claimable before fetch completes, and finalize must not become
      // claimable until both transforms have.
      const wf = defineWorkflow(`dist-test-drain-${crypto.randomUUID()}`, (builder) => {
        builder.step('fetch', recordingStep('fetch', 'fetch-value'))
        builder.step('transform1', recordingStep('transform1', 't1-value'), { dependsOn: ['fetch'] })
        builder.step('transform2', recordingStep('transform2', 't2-value'), { dependsOn: ['fetch'] })
        builder.step('finalize', recordingStep('finalize', 'final-value'), {
          dependsOn: ['transform1', 'transform2'],
        })
      })

      try {
        const runIds = await Promise.all(
          Array.from({ length: RUNS }, async () => {
            const { runId } = await enqueueRun(sql, wf, { namespace })
            return runId
          })
        )
        expect(runIds).toHaveLength(RUNS)

        // Three independent connections, three independent worker
        // processes-in-miniature — closer to "3 workers" than sharing one
        // pooled connection across all of them.
        const workerDbs: Db[] = [createDb(), createDb(), createDb()]
        const workers: Worker[] = workerDbs.map((db, i) =>
          createWorker({
            db,
            handles: [wf],
            workerId: `dist-worker-${i}`,
            namespace,
            concurrency: 2,
            leaseTtlMs: 10_000, // generous — this case isn't exercising reclaim
            pollIntervalMs: 15,
            reclaimIntervalMs: 60_000,
          })
        )
        workers.forEach((w) => w.start())

        try {
          await pollUntil(
            async () => {
              const runs = await Promise.all(runIds.map((id) => getRun(sql, id)))
              return runs.every((r) => r?.status === 'completed' || r?.status === 'failed')
            },
            { timeoutMs: 20_000, intervalMs: 50 }
          )
        } finally {
          await Promise.all(workers.map((w) => w.stop()))
          await Promise.all(workerDbs.map((db) => db.end()))
        }

        // Every run actually completed (not failed).
        const finalRuns = await Promise.all(runIds.map((id) => getRun(sql, id)))
        for (const run of finalRuns) {
          expect(run?.status).toBe('completed')
        }

        // Every step, per the DB, is completed exactly once.
        for (const runId of runIds) {
          const steps = await getStepsByRun(sql, runId)
          expect(steps).toHaveLength(STEPS_PER_RUN)
          for (const step of steps) {
            expect(step.status).toBe('completed')
          }
        }

        // The durable side-effect log: exactly one line per step, and more
        // than one distinct worker did the work.
        const lines = readFileSync(execFile, 'utf8').split('\n').filter(Boolean)
        expect(lines).toHaveLength(RUNS * STEPS_PER_RUN)

        const keys = lines.map((line) => line.split('|').slice(0, 2).join('|'))
        expect(new Set(keys).size).toBe(lines.length) // no key repeated -> no step ran twice

        const owners = new Set(lines.map((line) => line.split('|')[2]))
        expect(owners.size).toBeGreaterThan(1) // spread across more than one worker
      } finally {
        if (existsSync(execFile)) unlinkSync(execFile)
      }
    },
    30_000
  )
})

describe('kill one mid-step', () => {
  const fixturePath = join(import.meta.dir, 'fixtures/worker-hang.ts')

  test(
    'a lease held by a kill -9\'d worker is reclaimed within the TTL and a surviving worker finishes the run',
    async () => {
      const testId = crypto.randomUUID()
      const namespace = `dist-test-${testId}`
      const workflowName = `dist-test-hang-${testId}`
      const signalFile = join(tmpdir(), `orq-hang-${testId}.log`)
      const leaseTtlMs = 500
      const crashWorkerId = 'crash-worker'
      const survivorWorkerId = 'surviving-worker'

      let surviveDb: Db | undefined
      let survivor: Worker | undefined

      try {
        // Creation handle: only used to materialize the DAG's step rows via
        // enqueueRun, which never executes anything — its `work` fn is a
        // decoy that must never actually run.
        const createHandle = defineWorkflow(workflowName, (builder) => {
          builder.step(
            'work',
            async () => {
              throw new Error('worker-hang test: the creation handle should never execute a step')
            },
            { maxAttempts: 5 }
          )
        })
        const { runId } = await enqueueRun(sql, createHandle, { namespace })

        // Surviving handle: SAME workflow name, but a DIFFERENT `work`
        // implementation — one that actually completes. It records the
        // lease_owner it sees on its own step row, which is how we prove
        // the lease genuinely changed hands from the crashed worker to
        // this one.
        const surviveHandle = defineWorkflow(workflowName, (builder) => {
          builder.step(
            'work',
            async (ctx) => {
              const steps = await getStepsByRun(sql, ctx.runId)
              const self = steps.find((s) => s.name === 'work')
              appendFileSync(signalFile, `resumed:${self?.lease_owner ?? 'unknown'}\n`)
              return 'done'
            },
            { maxAttempts: 5 }
          )
        })

        surviveDb = createDb()
        survivor = createWorker({
          db: surviveDb,
          handles: [surviveHandle],
          workerId: survivorWorkerId,
          namespace,
          leaseTtlMs,
          pollIntervalMs: 20,
          reclaimIntervalMs: 50, // tight sweep so reclaim happens promptly after the TTL elapses
          concurrency: 1,
        })
        // NOT started yet — see below. If the survivor started polling now,
        // it could win the race for the step's very first claim before the
        // subprocess has even finished booting, defeating the point of this
        // test (which is to prove *reclaim*, not just "a worker eventually
        // runs it").

        // Spawn the doomed worker as its own real OS process.
        const proc = Bun.spawn({
          cmd: ['bun', fixturePath],
          env: {
            ...process.env,
            ORQ_WORKFLOW_NAME: workflowName,
            ORQ_NAMESPACE: namespace,
            ORQ_SIGNAL_FILE: signalFile,
            ORQ_WORKER_ID: crashWorkerId,
            ORQ_LEASE_TTL_MS: String(leaseTtlMs),
          },
          stdout: 'inherit',
          stderr: 'inherit',
        })

        try {
          // Wait for proof the subprocess actually claimed the step and
          // started running it (not an arbitrary sleep).
          await pollUntil(
            () => existsSync(signalFile) && readFileSync(signalFile, 'utf8').includes('started'),
            { timeoutMs: 10_000, intervalMs: 20 }
          )

          // Only NOW start the survivor — the doomed worker already holds
          // the lease, so the survivor's claim loop will find nothing until
          // that lease actually expires and is reclaimed.
          survivor.start()

          // The step is left running, leased by the worker we're about to kill.
          const beforeKill = await getStepsByRun(sql, runId)
          const workBeforeKill = beforeKill.find((s) => s.name === 'work')
          expect(workBeforeKill?.status).toBe('running')
          expect(workBeforeKill?.lease_owner).toBe(crashWorkerId)

          const killedAt = Date.now()
          proc.kill('SIGKILL')
          await proc.exited

          // Prove this was a real kill -9, not a clean exit.
          expect(proc.exitCode).toBeNull()
          expect(proc.signalCode).toBe('SIGKILL')

          // Immediately after the kill (before the TTL has had a chance to
          // elapse), the step is still `running` under the dead worker's
          // lease — nobody has touched it yet.
          const afterKill = await getStepsByRun(sql, runId)
          const workAfterKill = afterKill.find((s) => s.name === 'work')
          expect(workAfterKill?.status).toBe('running')
          expect(workAfterKill?.lease_owner).toBe(crashWorkerId)

          // Bounded poll for the surviving worker's own reclaim sweep +
          // claim loop to pick the step back up and finish the run.
          await pollUntil(
            async () => {
              const run = await getRun(sql, runId)
              return run?.status === 'completed'
            },
            { timeoutMs: 10_000, intervalMs: 20 }
          )
          const reclaimedElapsedMs = Date.now() - killedAt

          const finalSteps = await getStepsByRun(sql, runId)
          const finalWork = finalSteps.find((s) => s.name === 'work')
          expect(finalWork?.status).toBe('completed')
          expect(finalWork?.reclaim_count).toBeGreaterThanOrEqual(1)

          const signalContents = readFileSync(signalFile, 'utf8')
          expect(signalContents).toContain('started')
          // The lease genuinely changed hands: the step that finished was
          // executed under the SURVIVING worker's ownership, not the dead
          // worker's (whose implementation could only ever hang, never
          // write this line).
          expect(signalContents).toContain(`resumed:${survivorWorkerId}`)

          // Reclaimed well within a reasonable multiple of the lease TTL —
          // bounded, not an arbitrary sleep.
          expect(reclaimedElapsedMs).toBeLessThan(leaseTtlMs * 10)

          console.log(
            `[workers-distributed] lease reclaim: TTL=${leaseTtlMs}ms, observed elapsed=${reclaimedElapsedMs}ms`
          )
        } finally {
          if (proc.exitCode === null && proc.signalCode === null) proc.kill('SIGKILL')
        }
      } finally {
        if (survivor) await survivor.stop()
        if (surviveDb) await surviveDb.end()
        if (existsSync(signalFile)) unlinkSync(signalFile)
      }
    },
    30_000
  )
})

describe('retries self-heal', () => {
  test(
    'a step that fails twice then succeeds ends completed with attempt 3, backoff having moved run_after into the future',
    async () => {
      const namespace = `dist-test-${crypto.randomUUID()}`
      let attempts = 0

      const wf = defineWorkflow(`dist-test-retry-${crypto.randomUUID()}`, (builder) => {
        builder.step(
          'flaky',
          async () => {
            attempts++
            if (attempts < 3) throw new Error(`flaky: attempt ${attempts} fails on purpose`)
            return 'flaky-value'
          },
          { maxAttempts: 3 }
        )
      })

      const { runId } = await enqueueRun(sql, wf, { namespace })
      const created = await getStepsByRun(sql, runId)
      const initialRunAfter = created[0]!.run_after

      const worker = createWorker({
        db: sql,
        handles: [wf],
        namespace,
        leaseTtlMs: 5_000,
        pollIntervalMs: 5,
        reclaimIntervalMs: 60_000,
        concurrency: 1,
        // DEFAULT_RETRY_POLICY (100ms base, factor 2, full jitter) — plenty
        // of window for the sampling loop below to observe the parked state.
      })
      worker.start()

      // While polling for completion, also sample the step row: if we ever
      // observe it `ready` with a `run_after` still in the future, that's
      // direct proof the backoff delay genuinely parked the step (released
      // the worker) instead of pinning it in a busy-retry loop.
      let sawFutureRunAfterWhileReady = false
      try {
        await pollUntil(
          async () => {
            const steps = await getStepsByRun(sql, runId)
            const step = steps.find((s) => s.name === 'flaky')
            if (step && step.status === 'ready' && step.run_after.getTime() > Date.now()) {
              sawFutureRunAfterWhileReady = true
            }
            const run = await getRun(sql, runId)
            return run?.status === 'completed' || run?.status === 'failed'
          },
          { timeoutMs: 10_000, intervalMs: 5 }
        )
      } finally {
        await worker.stop()
      }

      const run = await getRun(sql, runId)
      expect(run?.status).toBe('completed')
      expect(attempts).toBe(3) // failed, failed, succeeded

      const finalSteps = await getStepsByRun(sql, runId)
      const finalStep = finalSteps.find((s) => s.name === 'flaky')
      expect(finalStep?.status).toBe('completed')
      expect(finalStep?.attempt).toBe(3)
      // run_after was moved forward by retryStep on each of the two
      // failures; completeStep never touches it, so the final value is
      // still the last-scheduled retry time — strictly after creation.
      expect(finalStep!.run_after.getTime()).toBeGreaterThan(initialRunAfter.getTime())
      expect(sawFutureRunAfterWhileReady).toBe(true)
    },
    15_000
  )
})
