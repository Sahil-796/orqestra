// The worker-facing entrypoint into the queue. All the correctness lives in
// repositories.claimNextStep (the SKIP LOCKED transaction, the concurrency
// advisory lock, the priority-aging order-by) — this file just gives it a
// typed, minimal surface so worker.ts doesn't need to know the repositories.ts
// import exists.
//
// It is also where the priority-aging *policy* (the rate/cap, from config) is
// injected: repositories.ts stays a pure mechanism that ages by whatever
// numbers it's handed, and this layer supplies the config defaults so the
// worker path gets aging without every caller threading the knobs through.

import type { Db } from '../store/client.ts'
import { loadConfig, type OrqConfig } from '../config.ts'
import { claimNextStep, type StepRow } from '../store/repositories.ts'

export interface ClaimOptions {
  workerId: string
  leaseTtlMs: number
  namespace?: string
  /** Override the config-derived aging rate (points/sec). Mostly for tests. */
  priorityAgeRatePerSec?: number
  /** Override the config-derived aging cap. Mostly for tests. */
  priorityAgeMaxBoost?: number
}

// Parsed once on first claim and cached — the aging defaults are process-wide
// config, not per-claim state, so re-reading env on every claim would be
// wasteful. Loaded lazily (not at module init) so merely importing this module
// has no side effect and can't throw on bad env before a claim is ever made.
let cachedConfig: OrqConfig | undefined
function agingDefaults(): OrqConfig {
  if (!cachedConfig) cachedConfig = loadConfig()
  return cachedConfig
}

/**
 * Claim the next ready, due, unleased step for `workerId` and flip it to
 * `running` under a lease that expires in `leaseTtlMs`. Returns undefined
 * when there's nothing to claim right now (empty queue, everything ready
 * is still leased, everything ready isn't due yet, or every ready candidate
 * is blocked by a full concurrency key or an exhausted rate window) — that's
 * the normal "poll again later" outcome, not an error.
 *
 * Candidates are ordered by *effective* priority (base priority plus an age
 * boost, #14) so long-waiting low-priority steps aren't starved by a flood of
 * fresh high-priority work; the boost rate/cap come from config unless
 * overridden here.
 *
 * Safe to call from any number of workers at once: two concurrent callers
 * never receive the same step, and a step's concurrency limit is never
 * exceeded even under a flood of concurrent claims (see repositories.ts).
 */
export async function claimStep(db: Db, options: ClaimOptions): Promise<StepRow | undefined> {
  return claimNextStep(db, {
    workerId: options.workerId,
    leaseTtlMs: options.leaseTtlMs,
    namespace: options.namespace,
    priorityAgeRatePerSec: options.priorityAgeRatePerSec ?? agingDefaults().priorityAgeRatePerSec,
    priorityAgeMaxBoost: options.priorityAgeMaxBoost ?? agingDefaults().priorityAgeMaxBoost,
  })
}
