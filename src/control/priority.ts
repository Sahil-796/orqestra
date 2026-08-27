// Feature #14 — priorities / queue fairness: the aging math, in one pure
// place so it's unit-testable without a DB and so the SQL claim query and any
// TS-side reasoning agree on exactly one formula.
//
// The trap from the build plan: a naive `order by priority desc` starves
// normal-priority runs the moment high-priority work floods in — a low-priority
// step can wait forever behind an unending stream of fresh high-priority ones.
// The fix is *aging*: a step's *effective* priority grows the longer it has
// been ready, so a long-waiting low-priority step eventually competes with, and
// then beats, fresh high-priority work.
//
// effective = base + min(maxBoost, max(0, ageSeconds * ratePerSec))
//
// The claim query (repositories.ts `claimNextStep`) computes this same
// expression in SQL and orders by it descending; this function is the
// canonical reference the tests pin the behaviour to. With `ratePerSec = 0` the
// boost is always 0 and effective priority collapses back to the base priority
// (aging disabled), which is why that is the safe do-nothing default.

export interface PriorityAgingConfig {
  /** Priority points added per second a step has been ready. 0 disables aging. */
  ratePerSec: number
  /** Upper bound on the age boost, so aging lifts a step into contention but
   *  never to unbounded priority. */
  maxBoost: number
}

/** The age boost alone (never negative, capped at `maxBoost`). */
export function ageBoost(ageSeconds: number, cfg: PriorityAgingConfig): number {
  return Math.min(cfg.maxBoost, Math.max(0, ageSeconds * cfg.ratePerSec))
}

/** Effective priority a claim orders by: base priority plus the age boost. */
export function effectivePriority(
  basePriority: number,
  ageSeconds: number,
  cfg: PriorityAgingConfig
): number {
  return basePriority + ageBoost(ageSeconds, cfg)
}
