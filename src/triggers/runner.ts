// The trigger daemon's lifecycle: a background poll loop tying together
// cron sync (cron.ts), schedule firing (scheduled.ts), and event-trigger
// routing (events.ts). Mirrors the worker's lifecycle shape
// (src/worker/worker.ts's createWorker/Worker) — a `start*` factory
// returning a handle with a graceful `stop()` — and, like createWorker's
// `handles` option, accepts the workflow list as either a Map or an array
// rather than reaching into define/workflow.ts's process-local registry
// itself: that registry only exposes keyed lookups
// (getRegisteredWorkflow(name), getWorkflowTriggers(name)), not an
// enumeration of everything registered, so the caller (whoever wires up the
// daemon, e.g. the orchestrator's index.ts) passes the handles it already
// has — the same shape createWorker already expects them to gather for the
// worker pool.
//
// The loop itself is a short-sleep poller, not a busy loop: each tick awaits
// its work, then sleeps `pollIntervalMs` (or wakes early on stop()) before
// the next tick — same "sleep between ticks, cooperative shutdown" shape as
// the worker's outer loop.

import { loadConfig } from '../config.ts'
import type { Db } from '../store/client.ts'
import type { WorkflowHandle } from '../define/workflow.ts'
import { syncCronSchedules } from './cron.ts'
import { pollDueSchedules, type PollSchedulesResult } from './scheduled.ts'
import { pollUndispatchedEvents, type PollEventsResult } from './events.ts'

export interface TriggerRunnerOptions {
  db: Db
  /**
   * Workflows this daemon should sync cron schedules for and route events
   * to — read via `.triggers` on each handle. Accepts either shape, same as
   * `createWorker`'s `handles` option.
   */
  workflows: Map<string, WorkflowHandle> | WorkflowHandle[]
  /** How long to sleep between poll ticks. Defaults to `ORQ_POLL_INTERVAL_MS` (config.ts). */
  pollIntervalMs?: number
  /** Max schedules claimed per tick (claimDueSchedules' `limit`). */
  scheduleLimit?: number
  /** Max undispatched events claimed per tick (claimUndispatchedEvents' `limit`). */
  eventLimit?: number
  /** Forward guard bump applied by claimDueSchedules (see its doc). */
  guardMs?: number
  /** Called with any error thrown by a tick (schedule/event polling), so the loop itself never dies from one bad tick. */
  onError?: (error: unknown) => void
}

export interface TriggerTickResult {
  schedules: PollSchedulesResult
  events: PollEventsResult
}

export interface TriggerRunner {
  /** Graceful: stop scheduling further ticks, await whatever's in flight, then resolve. */
  stop(): Promise<void>
  /** Whether the loop is currently running (false once stop() has resolved). */
  readonly running: boolean
}

function normalizeWorkflows(input: Map<string, WorkflowHandle> | WorkflowHandle[]): WorkflowHandle[] {
  return input instanceof Map ? Array.from(input.values()) : input
}

/**
 * Run one schedule + event poll tick directly, without the interval loop.
 * Exposed for tests that want to drive a single tick deterministically
 * rather than waiting on the real interval.
 */
export async function runTriggerTick(
  db: Db,
  workflows: readonly WorkflowHandle[],
  opts: { scheduleLimit?: number; eventLimit?: number; guardMs?: number; now?: Date } = {}
): Promise<TriggerTickResult> {
  const schedules = await pollDueSchedules(db, {
    limit: opts.scheduleLimit,
    guardMs: opts.guardMs,
    now: opts.now,
  })
  const events = await pollUndispatchedEvents(db, workflows, { limit: opts.eventLimit })
  return { schedules, events }
}

/**
 * Start the trigger daemon: sync cron schedules once up front, then poll on
 * an interval for due schedules and undispatched events until `stop()` is
 * called. Returns immediately — the loop runs in the background.
 */
export function startTriggerRunner(opts: TriggerRunnerOptions): TriggerRunner {
  const config = loadConfig()
  const pollIntervalMs = opts.pollIntervalMs ?? config.pollIntervalMs
  const db = opts.db
  const workflows = normalizeWorkflows(opts.workflows)

  let stopped = false
  let stopWaiter: (() => void) | undefined
  let sleepTimer: ReturnType<typeof setTimeout> | undefined

  const loopPromise = (async () => {
    try {
      await syncCronSchedules(db, workflows)
    } catch (error) {
      opts.onError?.(error)
    }

    while (!stopped) {
      try {
        await runTriggerTick(db, workflows, {
          scheduleLimit: opts.scheduleLimit,
          eventLimit: opts.eventLimit,
          guardMs: opts.guardMs,
        })
      } catch (error) {
        opts.onError?.(error)
      }

      if (stopped) break

      await new Promise<void>((resolve) => {
        stopWaiter = resolve
        sleepTimer = setTimeout(() => {
          stopWaiter = undefined
          resolve()
        }, pollIntervalMs)
      })
    }
  })()

  return {
    get running() {
      return !stopped
    },
    async stop(): Promise<void> {
      stopped = true
      if (sleepTimer) clearTimeout(sleepTimer)
      stopWaiter?.()
      await loopPromise
    },
  }
}
