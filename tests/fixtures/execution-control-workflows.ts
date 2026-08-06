// Workflow fixtures for the Phase 3 end-to-end proof. Each factory returns
// both the handle and the mutable counters the test asserts on, so a test can
// see exactly how many times a step body actually executed — the difference
// between "the run finished" and "the run finished for the right reason".
//
// Names are uniquified per call: the workflow registry and the `workflow`
// table are global, and these tests run alongside every other suite.

import { defineWorkflow, type WorkflowHandle } from '../../src/define/workflow.ts'

function unique(prefix: string): string {
  return `${prefix}-${crypto.randomUUID()}`
}

/**
 * One step that runs, sleeps once, then runs again. `bodyRuns` counts full
 * executions of the step function: it must be 2 by the time the run completes
 * (once up to the sleep, once after the wake), and `afterSleepRuns` must be 1.
 */
export function sleeperWorkflow(durationMs: number): {
  handle: WorkflowHandle
  counts: { bodyRuns: number; afterSleepRuns: number }
} {
  const counts = { bodyRuns: 0, afterSleepRuns: 0 }
  const handle = defineWorkflow(unique('phase3-sleeper'), (builder) => {
    builder.step(
      'nap',
      async (ctx) => {
        counts.bodyRuns++
        await ctx.sleep(durationMs)
        counts.afterSleepRuns++
        return 'awake'
      },
      // maxAttempts: 1 on purpose — a sleep must not consume the step's only
      // attempt, so this step can only finish if sleepStep gave the attempt back.
      { maxAttempts: 1 }
    )
  })
  return { handle, counts }
}

/** A trivial single-step workflow, used to prove a worker isn't pinned by a sleeper. */
export function quickWorkflow(): { handle: WorkflowHandle; counts: { runs: number } } {
  const counts = { runs: 0 }
  const handle = defineWorkflow(unique('phase3-quick'), (builder) => {
    builder.step('ping', async () => {
      counts.runs++
      return 'pong'
    })
  })
  return { handle, counts }
}

/**
 * A step that hangs past its timeout on its first attempt and returns
 * promptly on the second. The hang is abort-aware so the test process can
 * exit cleanly — the worker has already given up on it by then, but a stray
 * 30s timer would keep the event loop alive.
 */
export function timeoutWorkflow(timeoutMs: number): {
  handle: WorkflowHandle
  counts: { starts: number; abortsObserved: number }
} {
  const counts = { starts: 0, abortsObserved: 0 }
  const handle = defineWorkflow(unique('phase3-timeout'), (builder) => {
    builder.step(
      'slow',
      async (ctx) => {
        const attempt = ++counts.starts
        if (attempt === 1) {
          await new Promise<void>((resolve) => {
            const timer = setTimeout(resolve, 30_000)
            ctx.signal.addEventListener(
              'abort',
              () => {
                counts.abortsObserved++
                clearTimeout(timer)
                resolve()
              },
              { once: true }
            )
          })
          return 'far too late'
        }
        return 'quick enough'
      },
      { maxAttempts: 2, timeoutMs }
    )
  })
  return { handle, counts }
}

/**
 * Two steps: a long, cooperative one that bails when its signal aborts, and a
 * dependent one that must never run. `finished` proves the cancelled step did
 * not quietly complete anyway.
 */
export function cancellableWorkflow(): {
  handle: WorkflowHandle
  counts: { started: number; aborted: number; finished: number; nextRan: number }
} {
  const counts = { started: 0, aborted: 0, finished: 0, nextRan: 0 }
  const handle = defineWorkflow(unique('phase3-cancel'), (builder) => {
    builder.step(
      'long',
      async (ctx) => {
        counts.started++
        await new Promise<void>((resolve, reject) => {
          const timer = setTimeout(resolve, 30_000)
          ctx.signal.addEventListener(
            'abort',
            () => {
              counts.aborted++
              clearTimeout(timer)
              reject(new Error('step bailed: run cancelled'))
            },
            { once: true }
          )
        })
        counts.finished++
        return 'should not get here'
      },
      { maxAttempts: 3 }
    )
    builder.step(
      'next',
      async () => {
        counts.nextRan++
        return 'should never run'
      },
      { dependsOn: ['long'] }
    )
  })
  return { handle, counts }
}
