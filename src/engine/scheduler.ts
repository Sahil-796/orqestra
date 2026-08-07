// Pure DAG-readiness logic, extracted from the Phase 1 executor's inline
// loop (see executor.ts's `executeRun`) so it's unit-testable without
// Postgres. Operates on a minimal structural shape rather than the full
// `StepRow` so tests can exercise it with plain objects — any real
// `StepRow` satisfies `StepLike` too, so callers pass rows straight
// through.

export interface StepLike {
  name: string
  status: string
  depends_on: string[]
}

/** A step's dependencies are satisfied once every named dep has completed. */
export function dependenciesSatisfied<T extends StepLike>(
  step: T,
  all: readonly StepLike[]
): boolean {
  return step.depends_on.every((depName) => all.find((s) => s.name === depName)?.status === 'completed')
}

/** Every `pending` step whose deps are all `completed` right now. */
export function newlyReadySteps<T extends StepLike>(all: readonly T[]): T[] {
  return all.filter((step) => step.status === 'pending' && dependenciesSatisfied(step, all))
}

// Vacuously true on an empty list, same as the `.every` check this
// replaces in executor.ts — a workflow with zero steps shouldn't occur in
// practice (defineWorkflow always registers at least one), so this isn't
// specially guarded against.
export function isRunComplete(all: readonly StepLike[]): boolean {
  return all.every((step) => step.status === 'completed')
}

// Blocked = stuck for good, not just "not done yet": nothing is currently
// ready/running (so no progress is imminent), and every remaining pending
// step depends — directly or by a missing name — on something that will
// never complete (`failed`/`cancelled`, or a dep name that doesn't exist
// in this run at all). A pending step whose deps are merely still pending/
// ready/running elsewhere is "on its way", not blocked, even though this
// function can't itself make progress on it.
export function isRunBlocked(all: readonly StepLike[]): boolean {
  if (all.some((s) => s.status === 'ready' || s.status === 'running')) return false

  const pending = all.filter((s) => s.status === 'pending')
  if (pending.length === 0) return false // nothing left pending: complete or empty, not "blocked"

  const isDepAlive = (depName: string): boolean => {
    const dep = all.find((s) => s.name === depName)
    return dep !== undefined && dep.status !== 'failed' && dep.status !== 'cancelled'
  }
  const hasSalvageablePending = pending.some((step) => step.depends_on.every(isDepAlive))
  return !hasSalvageablePending
}
