// Drives one scenario against the REAL engine: enqueue `knobs.runs` runs
// through `enqueueRun`, spin up a pool of `createWorker`s to drain them, and
// measure. No shortcuts, no toy in-memory engine — this exercises the same
// enqueue/claim/lease/commit path the durability tests do (see
// tests/workers-distributed.test.ts, which this mirrors).
//
// Isolation: every call mints its own random `namespace` so concurrent bench
// runs (or a bench run alongside the test suite) never see each other's rows
// on the shared dev Postgres.

import { createDb, type Db } from '../src/store/client.ts'
import { enqueueRun } from '../src/engine/executor.ts'
import { getRun, type RunRow } from '../src/store/repositories.ts'
import { createWorker, type Worker } from '../src/worker/worker.ts'
import { isTerminalRunStatus } from '../src/engine/child.ts'
import type { WorkflowHandle } from '../src/define/workflow.ts'
import { summarize, type Summary } from './metrics.ts'

/**
 * How a real service actually runs: long-lived workers that are already warm
 * and spinning, a queue that stays full, and the numbers read from the
 * steady middle — never the cold-start ramp or the drain tail where workers
 * go idle. `runScenario` (above) measures a cold enqueue→drain burst, which
 * folds worker boot + first-poll + queue-emptying into every latency figure;
 * `runScenarioSteady` (below) is the honest, product-shaped measurement.
 */
export interface SteadyOptions {
  /**
   * Fraction of completions (ordered by finish time) discarded from the FRONT
   * as warm-up — the ramp where the pool is still filling and the JIT/pool is
   * cold. Default 0.2.
   */
  warmupFrac?: number
  /**
   * Fraction of completions discarded from the BACK as the drain tail — where
   * the backlog runs dry and workers start idling instead of saturating.
   * Default 0.1.
   */
  drainFrac?: number
  /** Idle settle after `start()` so the claim loops are genuinely spinning before work lands. Default 150ms. */
  settleMs?: number
}

export interface SteadyResult {
  scenario: string
  namespace: string
  knobs: ScenarioKnobs
  /** Every run enqueued. */
  totalRuns: number
  /** Runs kept in the steady window after trimming warm-up + drain tail. */
  keptRuns: number
  /** Wall-clock span of the steady window (first→last kept completion), ms. */
  steadyWindowMs: number
  /** Throughput measured over the steady window only. */
  steadyRunsPerSec: number
  steadyStepsPerSec: number
  /** End-to-end per-run time (finished − created): queue wait + processing. */
  endToEnd: Summary
  /** Time a run sat in the queue before a worker started it (started − created). */
  queueWait: Summary
  /** Engine processing time once started (finished − started) — the real work + commit/advance cost. */
  processTime: Summary
  claimAttempts: number
  claimsFound: number
  emptyPollRatio: number
  allTerminal: boolean
  unexpectedFailures: number
}

/** Knobs steer scenario shape + load. All optional except runs/workers/concurrency, which run.ts always fills. */
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
  /** Build a fresh, uniquely-named workflow handle. Knobs steer its shape (depth/width/fail count). */
  build(knobs: ScenarioKnobs): WorkflowHandle
  /** Defaults merged under any CLI-supplied knobs. */
  defaultKnobs: Partial<ScenarioKnobs>
  /** Steps a single run commits, given knobs — used for stepsPerSec. Defaults to steps ?? 1 if omitted. */
  stepsPerRun?(knobs: ScenarioKnobs): number
}

export interface BenchResult {
  scenario: string
  namespace: string
  knobs: ScenarioKnobs
  runs: number
  steps: number
  workers: number
  wallMs: number
  runsPerSec: number
  stepsPerSec: number
  latency: Summary
  claimAttempts: number
  claimsFound: number
  emptyPollRatio: number
  allTerminal: boolean
  unexpectedFailures: number
}

/** Bounded wait so a stuck run can never hang a bench sweep forever. */
const TIMEOUT_MS = 60_000
/** Cheap poll cadence — no tight spin, no meaningful added latency. */
const POLL_INTERVAL_MS = 30

/** Statuses the engine will never move a run out of once reached. */
function isTerminal(status: RunRow['status']): boolean {
  return isTerminalRunStatus(status)
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

/**
 * Drive the real engine end-to-end and measure. Enqueues `knobs.runs` runs of
 * `scenario`, drains them with `knobs.workers` independently-connected
 * workers, and reports throughput/latency/guardrail numbers once every run
 * reaches a terminal status (or the bounded timeout trips first).
 */
export async function runScenario(scenario: Scenario, knobs: ScenarioKnobs): Promise<BenchResult> {
  const namespace = `bench-${scenario.name}-${crypto.randomUUID()}`
  const handle = scenario.build(knobs)

  const controlDb: Db = createDb()
  const workerDbs: Db[] = Array.from({ length: knobs.workers }, () => createDb())
  let workers: Worker[] = []

  try {
    // 1. Enqueue all runs up front — the pool below drains a static backlog,
    // matching how the distributed-worker test shapes its load.
    const runIds: string[] = []
    for (let i = 0; i < knobs.runs; i++) {
      const { runId } = await enqueueRun(controlDb, handle, { namespace })
      runIds.push(runId)
    }

    // 2. One worker per Db connection — never share a connection across
    // workers, so lease/claim contention is genuinely inter-process-shaped.
    workers = workerDbs.map((db) =>
      createWorker({
        db,
        handles: [handle],
        namespace,
        concurrency: knobs.concurrency,
        ...(knobs.leaseTtlMs !== undefined ? { leaseTtlMs: knobs.leaseTtlMs } : {}),
        ...(knobs.pollIntervalMs !== undefined ? { pollIntervalMs: knobs.pollIntervalMs } : {}),
      })
    )

    const start = performance.now()
    for (const worker of workers) worker.start()

    // 3. Poll the shrinking pending set until every run is terminal, or bail
    // out on the bounded timeout guard so one stuck scenario can't hang a
    // whole sweep.
    const pending = new Set(runIds)
    const deadline = Date.now() + TIMEOUT_MS
    while (pending.size > 0 && Date.now() < deadline) {
      for (const runId of [...pending]) {
        const run = await getRun(controlDb, runId)
        if (run && isTerminal(run.status)) pending.delete(runId)
      }
      if (pending.size > 0) await sleep(POLL_INTERVAL_MS)
    }
    const wallMs = performance.now() - start
    const allTerminal = pending.size === 0

    for (const worker of workers) await worker.stop()

    // 4. Latency + guardrail pass: re-read every run's final row.
    const finalRuns = await Promise.all(runIds.map((id) => getRun(controlDb, id)))
    const latencies: number[] = []
    let unexpectedFailures = 0
    for (const run of finalRuns) {
      if (!run) continue
      if (run.finished_at) {
        latencies.push(run.finished_at.getTime() - run.created_at.getTime())
      }
      if (run.status === 'failed' || run.status === 'dead_letter') unexpectedFailures++
    }

    const claimAttempts = workers.reduce((sum, w) => sum + w.claimAttempts, 0)
    const claimsFound = workers.reduce((sum, w) => sum + w.claimsFound, 0)
    const emptyPollRatio = claimAttempts > 0 ? (claimAttempts - claimsFound) / claimAttempts : 0

    const stepsPerRun = scenario.stepsPerRun?.(knobs) ?? knobs.steps ?? 1
    const steps = stepsPerRun * knobs.runs
    const wallSec = wallMs / 1000

    return {
      scenario: scenario.name,
      namespace,
      knobs,
      runs: knobs.runs,
      steps,
      workers: knobs.workers,
      wallMs,
      runsPerSec: wallSec > 0 ? knobs.runs / wallSec : 0,
      stepsPerSec: wallSec > 0 ? steps / wallSec : 0,
      latency: summarize(latencies),
      claimAttempts,
      claimsFound,
      emptyPollRatio,
      allTerminal,
      unexpectedFailures,
    }
  } finally {
    // Every worker Db AND the control Db, always — even on a timeout bail-out.
    await Promise.allSettled(workerDbs.map((db) => db.end()))
    await controlDb.end()
  }
}

/**
 * Product-shaped, steady-state measurement. Unlike `runScenario`'s cold burst,
 * this starts the worker pool FIRST and lets it warm, THEN feeds a large
 * backlog so the queue stays saturated, and reports only the steady middle of
 * the drain — trimming the warm-up ramp and the drain tail where workers idle.
 * Splits per-run time into queue-wait vs engine-processing so a fat wall-clock
 * number can't hide a fast engine behind a polling delay.
 *
 * Latency here is read from durable `run` timestamps (created/started/finished
 * on the `run` row), so it's independent of how often the harness polls.
 */
export async function runScenarioSteady(
  scenario: Scenario,
  knobs: ScenarioKnobs,
  options: SteadyOptions = {}
): Promise<SteadyResult> {
  const warmupFrac = options.warmupFrac ?? 0.2
  const drainFrac = options.drainFrac ?? 0.1
  const settleMs = options.settleMs ?? 150

  const namespace = `bench-steady-${scenario.name}-${crypto.randomUUID()}`
  const handle = scenario.build(knobs)

  const controlDb: Db = createDb()
  const workerDbs: Db[] = Array.from({ length: knobs.workers }, () => createDb())
  let workers: Worker[] = []

  try {
    // 1. Bring the pool up FIRST and let the claim loops spin on an empty
    // queue — this is a warm, already-running server, not a cold start.
    workers = workerDbs.map((db) =>
      createWorker({
        db,
        handles: [handle],
        namespace,
        concurrency: knobs.concurrency,
        ...(knobs.leaseTtlMs !== undefined ? { leaseTtlMs: knobs.leaseTtlMs } : {}),
        ...(knobs.pollIntervalMs !== undefined ? { pollIntervalMs: knobs.pollIntervalMs } : {}),
      })
    )
    for (const worker of workers) worker.start()
    await sleep(settleMs)

    // 2. Drop the whole backlog in. Workers are already draining as we insert,
    // so the queue stays full — the saturated regime we want to measure.
    const runIds: string[] = []
    for (let i = 0; i < knobs.runs; i++) {
      const { runId } = await enqueueRun(controlDb, handle, { namespace })
      runIds.push(runId)
    }

    // 3. Wait for the backlog to fully drain (bounded — scale the deadline
    // with backlog size so a big run isn't cut off, but never unbounded).
    const pending = new Set(runIds)
    const deadline = Date.now() + Math.max(TIMEOUT_MS, knobs.runs * 250)
    while (pending.size > 0 && Date.now() < deadline) {
      for (const runId of [...pending]) {
        const run = await getRun(controlDb, runId)
        if (run && isTerminal(run.status)) pending.delete(runId)
      }
      if (pending.size > 0) await sleep(POLL_INTERVAL_MS)
    }
    const allTerminal = pending.size === 0

    for (const worker of workers) await worker.stop()

    // 4. Read final rows; order completed runs by finish time and keep the
    // steady middle (drop the warm-up ramp and the idle drain tail).
    const finalRuns = await Promise.all(runIds.map((id) => getRun(controlDb, id)))
    let unexpectedFailures = 0
    const completed: RunRow[] = []
    for (const run of finalRuns) {
      if (!run) continue
      if (run.status === 'failed' || run.status === 'dead_letter') unexpectedFailures++
      if (run.finished_at && run.started_at) completed.push(run)
    }
    completed.sort((a, b) => a.finished_at!.getTime() - b.finished_at!.getTime())

    let startIdx = Math.floor(completed.length * warmupFrac)
    let endIdx = Math.ceil(completed.length * (1 - drainFrac))
    // Guard: if trimming would leave too little to measure, keep everything.
    if (endIdx - startIdx < 2) {
      startIdx = 0
      endIdx = completed.length
    }
    const kept = completed.slice(startIdx, endIdx)

    const first = kept[0]
    const last = kept[kept.length - 1]
    const steadyWindowMs =
      first && last ? last.finished_at!.getTime() - first.finished_at!.getTime() : 0
    const steadyWindowSec = steadyWindowMs / 1000

    const endToEnd: number[] = []
    const queueWait: number[] = []
    const processTime: number[] = []
    for (const run of kept) {
      const created = run.created_at.getTime()
      const started = run.started_at!.getTime()
      const finished = run.finished_at!.getTime()
      endToEnd.push(finished - created)
      queueWait.push(started - created)
      processTime.push(finished - started)
    }

    const stepsPerRun = scenario.stepsPerRun?.(knobs) ?? knobs.steps ?? 1
    const steadyRunsPerSec = steadyWindowSec > 0 ? kept.length / steadyWindowSec : 0
    const steadyStepsPerSec = steadyWindowSec > 0 ? (kept.length * stepsPerRun) / steadyWindowSec : 0

    const claimAttempts = workers.reduce((sum, w) => sum + w.claimAttempts, 0)
    const claimsFound = workers.reduce((sum, w) => sum + w.claimsFound, 0)
    const emptyPollRatio = claimAttempts > 0 ? (claimAttempts - claimsFound) / claimAttempts : 0

    return {
      scenario: scenario.name,
      namespace,
      knobs,
      totalRuns: knobs.runs,
      keptRuns: kept.length,
      steadyWindowMs,
      steadyRunsPerSec,
      steadyStepsPerSec,
      endToEnd: summarize(endToEnd),
      queueWait: summarize(queueWait),
      processTime: summarize(processTime),
      claimAttempts,
      claimsFound,
      emptyPollRatio,
      allTerminal,
      unexpectedFailures,
    }
  } finally {
    await Promise.allSettled(workerDbs.map((db) => db.end()))
    await controlDb.end()
  }
}
