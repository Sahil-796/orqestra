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

// Phase 4 (#17 conditional branching): a `skipped` dependency counts as
// resolved for readiness purposes exactly like a `completed` one — the
// engine layer's policy call (see engine/dag.ts's module doc for the full
// reasoning), applied here too so the whole-run rescan and the narrow
// per-row primitive (repositories.ts's `recordDependencySatisfied`) agree.
// Without this, an untaken conditional branch would permanently strand any
// downstream fan-in join that names it in `dependsOn` — that's exactly the
// deadlock #17 must not cause.
function isResolved(status: string): boolean {
  return status === 'completed' || status === 'skipped'
}

/** A step's dependencies are satisfied once every named dep has resolved (completed or skipped). */
export function dependenciesSatisfied<T extends StepLike>(
  step: T,
  all: readonly StepLike[]
): boolean {
  return step.depends_on.every((depName) => isResolved(all.find((s) => s.name === depName)?.status ?? ''))
}

/** Every `pending` step whose deps are all `completed` right now. */
export function newlyReadySteps<T extends StepLike>(all: readonly T[]): T[] {
  return all.filter((step) => step.status === 'pending' && dependenciesSatisfied(step, all))
}

// Vacuously true on an empty list, same as the `.every` check this
// replaces in executor.ts — a workflow with zero steps shouldn't occur in
// practice (defineWorkflow always registers at least one), so this isn't
// specially guarded against.
//
// Phase 4: `skipped` is as terminal-and-fine as `completed` here too — a run
// whose every step either ran or was the untaken half of a conditional
// branch is done, not stuck waiting for something that was never going to
// run.
export function isRunComplete(all: readonly StepLike[]): boolean {
  return all.every((step) => step.status === 'completed' || step.status === 'skipped')
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
