// The initial bench scenario catalog. Each scenario's `build(knobs)` mints a
// fresh workflow definition (uuid-suffixed name, so repeated runs never
// collide in the in-process registry / `workflow` table) with TRIVIAL step
// bodies — the point of these benchmarks is engine overhead (claim, lease,
// commit, DAG advance), not user work. `knobs.stepWorkMs`, when set, adds a
// deliberate `await` inside each step so a caller can separate "time spent
// doing work" from "time spent in the engine" by comparing runs with and
// without it.
//
// Storage-boundary note: the only DB access here is `getDb()` from
// `../src/store/client.ts` (the process-wide singleton), used solely by the
// event-wake scenario's `emit` step to call `publishSignal`. That is calling
// through the same typed surface the rest of the engine uses — no raw SQL,
// no `postgres` import.

import { defineWorkflow, type WorkflowHandle } from '../src/define/workflow.ts'
import { getDb } from '../src/store/client.ts'
import { publishSignal } from '../src/control/signal.ts'

export interface ScenarioKnobs {
  runs: number
  workers: number
  concurrency: number
  steps?: number
  width?: number
  fail?: number
  stepWorkMs?: number
  leaseTtlMs?: number
  pollIntervalMs?: number
}

export interface Scenario {
  name: string
  build(knobs: ScenarioKnobs): WorkflowHandle
  defaultKnobs: Partial<ScenarioKnobs>
  stepsPerRun?(knobs: ScenarioKnobs): number
}

function wait(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

/** Honor `knobs.stepWorkMs` — a plain timer, not `ctx.sleep` (which suspends
 * the whole step through the engine's sleep machinery). This just simulates
 * a bit of synchronous-ish user work inside an otherwise-trivial step. */
async function simulateWork(knobs: ScenarioKnobs): Promise<void> {
  if (knobs.stepWorkMs && knobs.stepWorkMs > 0) {
    await wait(knobs.stepWorkMs)
  }
}

function uuid(): string {
  return crypto.randomUUID()
}

// ---------------------------------------------------------------------------
// 1. chain — a linear dependency chain of depth `knobs.steps`.
// ---------------------------------------------------------------------------

const chain: Scenario = {
  name: 'chain',
  defaultKnobs: { steps: 5 },
  stepsPerRun(knobs) {
    return knobs.steps ?? 5
  },
  build(knobs) {
    const depth = knobs.steps ?? 5
    return defineWorkflow(`bench-chain-${uuid()}`, (builder) => {
      for (let i = 0; i < depth; i++) {
        const name = `step-${i}`
        const dependsOn = i === 0 ? [] : [`step-${i - 1}`]
        builder.step(
          name,
          async (ctx) => {
            await simulateWork(knobs)
            return { i, runId: ctx.runId }
          },
          { dependsOn }
        )
      }
    })
  },
}

// ---------------------------------------------------------------------------
// 2. fanout — split -> width parallel shards -> combine.
// ---------------------------------------------------------------------------

const fanout: Scenario = {
  name: 'fanout',
  defaultKnobs: { width: 10 },
  stepsPerRun(knobs) {
    return (knobs.width ?? 10) + 2
  },
  build(knobs) {
    const width = knobs.width ?? 10
    return defineWorkflow(`bench-fanout-${uuid()}`, (builder) => {
      builder.step('split', async (ctx) => {
        await simulateWork(knobs)
        return { runId: ctx.runId }
      })
      const shardNames = builder.fanOut(
        'shard',
        width,
        async (index, ctx) => {
          await simulateWork(knobs)
          return { index, runId: ctx.runId }
        },
        { dependsOn: ['split'] }
      )
      builder.step(
        'combine',
        async (ctx) => {
          await simulateWork(knobs)
          return { combined: shardNames.length, runId: ctx.runId }
        },
        { dependsOn: shardNames }
      )
    })
  },
}

// ---------------------------------------------------------------------------
// 3. retry — a step that fails `knobs.fail` times then succeeds. The run
//    must still end `completed`. Attempt counting mirrors the pattern used
//    throughout tests/dead-letter.test.ts etc: a closure counter keyed by
//    `ctx.runId`, since replayed attempts of the same run all execute in
//    this process (bench workers run in-process) and `ctx.attempt` is not
//    part of the WorkflowContext surface (see src/define/context.ts).
// ---------------------------------------------------------------------------

const retry: Scenario = {
  name: 'retry',
  defaultKnobs: { fail: 2 },
  stepsPerRun() {
    return 1
  },
  build(knobs) {
    const fail = knobs.fail ?? 2
    // +2 headroom above `fail` so the budget covers `fail` failing attempts
    // plus the one that finally succeeds, with a little slack.
    const maxAttempts = fail + 2
    const attemptsByRun = new Map<string, number>()
    return defineWorkflow(`bench-retry-${uuid()}`, (builder) => {
      builder.step(
        'flaky',
        async (ctx) => {
          const n = (attemptsByRun.get(ctx.runId) ?? 0) + 1
          attemptsByRun.set(ctx.runId, n)
          await simulateWork(knobs)
          if (n <= fail) {
            throw new Error(`bench/retry: deliberate failure (attempt ${n} of ${fail})`)
          }
          return { attempts: n, runId: ctx.runId }
        },
        { maxAttempts }
      )
    })
  },
}

// ---------------------------------------------------------------------------
// 4. contention — a single trivial step per run, maximum claim pressure.
// ---------------------------------------------------------------------------

const contention: Scenario = {
  name: 'contention',
  defaultKnobs: {},
  stepsPerRun() {
    return 1
  },
  build(knobs) {
    return defineWorkflow(`bench-contention-${uuid()}`, (builder) => {
      builder.step('only', async (ctx) => {
        await simulateWork(knobs)
        return { runId: ctx.runId }
      })
    })
  },
}

// ---------------------------------------------------------------------------
// 5. sleep-scale — one step that ctx.sleep()s briefly, proving a sleeping
//    step doesn't pin a worker slot. Default sleep is short so a smoke run
//    finishes quickly; override with knobs.stepWorkMs (reused here as the
//    sleep duration in ms) if a longer sleep is wanted.
// ---------------------------------------------------------------------------

const DEFAULT_SLEEP_MS = 200

const sleepScale: Scenario = {
  name: 'sleep-scale',
  defaultKnobs: {},
  stepsPerRun() {
    return 1
  },
  build(knobs) {
    const sleepMs = knobs.stepWorkMs && knobs.stepWorkMs > 0 ? knobs.stepWorkMs : DEFAULT_SLEEP_MS
    return defineWorkflow(`bench-sleep-scale-${uuid()}`, (builder) => {
      builder.step('napper', async (ctx) => {
        await ctx.sleep(sleepMs)
        return { runId: ctx.runId, slept: sleepMs }
      })
    })
  },
}

// ---------------------------------------------------------------------------
// 6. event-wake — `emit` publishes a per-run signal, `wait` blocks on it via
//    `ctx.waitForEvent`. The harness only enqueues runs and drives workers —
//    it never publishes events — so this scenario has to arrange its own
//    publication from *within* the run.
//
//    Ordering hazard (see publishEvent / registerStepEventWait in
//    src/store/repositories.ts): a step only wakes if it is ALREADY
//    `blocked` at the moment the matching event is published. An event
//    published before the waiter ever reached `blocked` is invisible to it —
//    the backstop in `registerStepEventWait` only covers the narrow race
//    between "step decided to throw EventWaitSignal" and "worker commits the
//    blocked row", not "event published entirely before this step's attempt
//    began". So `emit` running to completion before `wait` is even claimed
//    would hang the run forever.
//
//    `emit` and `wait` both depend on nothing, so both are `ready` the
//    instant the run is created (src/engine/executor.ts: dependsOn.length
//    === 0 => 'ready'). Two things make `wait` win the race deterministically
//    regardless of the `--concurrency` knob:
//      1. `wait` is given a higher step priority than `emit`, and
//         claimNextStep's claim query orders candidates by priority DESC
//         first (src/store/repositories.ts claimNextStep) — so even at
//         concurrency=1 (one claim at a time, run-to-completion-or-suspend
//         before the next claim), `wait` is always claimed and reaches
//         `blocked` before `emit` is ever claimed.
//      2. `emit` additionally waits a short, deliberate delay before
//         publishing, as a belt-and-suspenders margin for a `wait` claim
//         that outraces `emit`'s claim in the same poll tick.
//    Verified terminating in the smoke test described in the report below.
// ---------------------------------------------------------------------------

const EVENT_WAKE_PRIORITY = 100
const EMIT_DELAY_MS = 250

const eventWake: Scenario = {
  name: 'event-wake',
  defaultKnobs: {},
  stepsPerRun() {
    return 2
  },
  build(knobs) {
    return defineWorkflow(`bench-event-wake-${uuid()}`, (builder) => {
      builder.step(
        'wait',
        async (ctx) => {
          const key = `bench-event-wake:${ctx.runId}`
          const payload = await ctx.waitForEvent<{ runId: string }>(key)
          return { woken: true, payload }
        },
        { priority: EVENT_WAKE_PRIORITY }
      )
      builder.step('emit', async (ctx) => {
        await simulateWork(knobs)
        await wait(EMIT_DELAY_MS)
        const key = `bench-event-wake:${ctx.runId}`
        const db = getDb()
        await publishSignal(db, { name: key, payload: { runId: ctx.runId }, source: 'api' })
        return { published: true }
      })
    })
  },
}

export const scenarios: Record<string, Scenario> = {
  chain,
  fanout,
  retry,
  contention,
  'sleep-scale': sleepScale,
  'event-wake': eventWake,
}
