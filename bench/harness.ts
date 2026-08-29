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
