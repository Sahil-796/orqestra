// engine/dag.ts — Phase 4 orchestration: turns one step's resolution
// (completed, or the untaken half of a conditional branch resolving as
// skipped) into whatever else in the run becomes runnable — fan-out (#15),
// fan-in (#16), conditional branching (#17) — and detects run completion.
// This is the module the worker's commit path (worker.ts's commitOutcome)
// calls instead of executor.ts's whole-run-rescan `advanceRun`.
//
// ## Design decision: the narrow per-row primitive, not the whole-run rescan
//
// scheduler.ts's `newlyReadySteps`/`dependenciesSatisfied` (Phase 1) and
// executor.ts's `advanceRun` (the Phase 1 inline driver, and the function
// Unit A deliberately left untouched) stay exactly as they are — this file
// does not replace them, and the inline `startRun`/`executeRun` path still
// uses `advanceRun`'s full-run rescan for its own step-to-step readiness
// bookkeeping. That path is single-process and sequential; a rescan is fine
// there and there is no reason to complicate it.
//
// For the WORKER's commit path — the durable, concurrent path this phase's
// shipping bar exercises (ten parallel workers completing ten fan-out
// siblings) — this file uses `repositories.recordDependencySatisfied`
// instead. Two reasons, in order of how much they mattered:
//
//   1. Cost under the lock the worker already takes. `worker.ts`'s
//      `commitOutcome` takes `FOR UPDATE` on the run row on every single
//      step commit, for a reason unrelated to DAG readiness (avoiding a
//      lock-upgrade deadlock against `insertHistory`'s implicit FK lock —
//      see that file's comment). That lock is not this unit's to remove.
//      Given it's taken regardless, what matters is how much work happens
//      while it's held: `advanceRun` re-reads and potentially rewrites
//      every step in the run on every commit (O(run size)); the narrow
//      primitive touches only the direct dependents of the one step that
//      just resolved (O(fan-out width)). For a run with a wide fan-out,
//      that is the difference between a rescan and a point lookup, on
//      every single one of the ten commits.
//   2. It is the primitive Unit A specifically built and load-tested for
//      this exact scenario: `recordDependencySatisfied`'s "single UPDATE,
//      containment check against `satisfied_deps`" shape is what makes ten
//      concurrent callers for the same fan-in join release it exactly once
//      with no deadlock (see repositories-dag.test.ts's 10-way concurrent
//      proof) — a correctness property this file's `advanceDag` inherits
//      for free rather than having to re-derive it against a rescan.
//
// Run completion is the one place both paths must do *something*
// whole-run-shaped — you cannot know "everything is done" without looking
// at everything. `maybeFinalizeRun` keeps that cheap: a lock-free aggregate
// (`countStepsByStatus`) on every call, escalating to the same `lockRun`
// `advanceRun` always takes only at the moment it actually observes
// completion (rare — once per run, not once per commit).
//
// ## Policy decision: does a skip satisfy a downstream edge?
//
// Yes, uniformly. `recordDependencySatisfied` (repositories.ts) is
// explicit that it doesn't judge *why* a dependency resolved — that
// judgment belongs to the caller. This file's answer: a `skipped`
// dependency counts exactly like a `completed` one for releasing a
// dependent. The alternative — skip never satisfies — would let a
// conditional branch's untaken half permanently strand any join downstream
// of it, which is precisely the deadlock #17 must not cause.
//
// A second, narrower policy sits on top of that: if EVERY one of a step's
// named dependencies resolved by being skipped (none of them actually
// completed), this file cascades the skip to that step too, instead of
// handing a worker a step to run against zero real inputs — see
// `cascadeIfAllDepsSkipped` below. A step downstream of a genuine fan-in
// join, where at least one branch actually completed, is unaffected: the
// instant one real dependency completes, the step is a true "ready", never
// a cascade. This is what keeps an untaken branch's whole downstream chain
// from executing dead code while a join *past* that branch still fires.

import type { Db } from '../store/client.ts'
import {
  countStepsByStatus,
  getDependencySteps,
  getIncompleteRuns,
  getStepsByRun,
  insertHistory,
  lockRun,
  recordDependencySatisfied,
  resolveBlockedStepForChildRun,
  skipStep,
  updateRunStatus,
  type RunRow,
  type StepRow,
} from '../store/repositories.ts'
import { decodeResult } from '../types.ts'
import { isRunComplete } from './scheduler.ts'

export interface StepResolution {
  /** Name of the step that just resolved (went to `completed`). */
  completedName: string
  /**
   * Feature #17: names of sibling steps (within the same run) that the
   * just-completed step's function declared, via `ctx.skip(...)`, as the
   * untaken branch. Skipped here, then fed into the same propagation the
   * completed step gets — a skip is a resolution too.
   */
  skipNames?: readonly string[]
}

export interface DagAdvanceResult {
  /** Steps newly released to `ready` by this call — genuinely runnable. */
  readySteps: StepRow[]
  /** Steps this call skipped — the explicit branch skips plus any cascade. */
  skippedSteps: StepRow[]
  /** Set if this call was the one that finished the run. */
  run: RunRow | undefined
}

// Given a dependent step that just became fully satisfied (every named dep
// resolved), decide whether it's real work or a cascade: does at least one
// of its dependencies actually complete? If none did — every one of them
// was itself skipped — this step has no real input and is skipped too,
// rather than handed to a worker. Returns the skip row if cascaded,
// undefined if the step should proceed as a genuine `ready`.
async function cascadeIfAllDepsSkipped(
  sql: Db,
  dependent: StepRow
): Promise<StepRow | undefined> {
  const deps = await getDependencySteps(sql, dependent.id)
  if (deps.length === 0) return undefined // no deps at all — not a cascade candidate
  const anyCompleted = deps.some((d) => d.status === 'completed')
  if (anyCompleted) return undefined
  return skipStep(
    sql,
    dependent.id,
    `all dependencies skipped: ${deps.map((d) => d.name).join(', ')}`
  )
}

// Propagate one resolved name to every sibling step in the run that lists
// it in `depends_on`, recursing through cascade-skips so a whole untaken
// branch chain resolves within one call. Reads the run's steps once per
// queue entry (bounded by chain depth, not fan-out width) — there is no
// dedicated "dependents of" repository query, only `getDependencySteps`
// (the reverse direction), so this is a plain filter over `getStepsByRun`.
async function propagate(
  sql: Db,
  runId: string,
  startName: string
): Promise<{ readySteps: StepRow[]; skippedSteps: StepRow[] }> {
  const readySteps: StepRow[] = []
  const skippedSteps: StepRow[] = []
  const queue: string[] = [startName]

  while (queue.length > 0) {
    const name = queue.shift()!
    const siblings = await getStepsByRun(sql, runId)
    const dependents = siblings.filter((s) => s.status === 'pending' && s.depends_on.includes(name))

    for (const dependent of dependents) {
      const updated = await recordDependencySatisfied(sql, dependent.id, name)
      if (!updated || updated.status !== 'ready') continue // not every dep in yet, or lost a race — safe no-op

      const cascaded = await cascadeIfAllDepsSkipped(sql, updated)
      if (cascaded) {
        skippedSteps.push(cascaded)
        queue.push(cascaded.name)
        continue
      }

      readySteps.push(updated)
    }
  }

  return { readySteps, skippedSteps }
}

/**
 * Whole-run completion check + finalize. Cheap in the common (not-yet-done)
 * case: one aggregate query, no lock. Escalates to `lockRun` (the same
 * `FOR UPDATE` `advanceRun` always takes) only once it looks like every
 * step is done, and re-verifies under that lock before writing anything —
 * so two concurrent callers that both observe "looks complete" at the
 * aggregate-read stage still can't both finalize: the second to acquire the
 * lock finds the run already `completed` (not `queued`/`running`) and
 * backs off.
 *
 * Same caveat as `advanceRun`: this does not open its own transaction (the
 * `postgres` package's transaction-scoped `sql` has no `.begin()` of its
 * own to nest — see client.ts's `withTransaction`), so the lock is only a
 * real mutual-exclusion boundary when the caller already has `sql` open
 * inside one. `worker.ts`'s `commitOutcome` does; a bare call (e.g. from
 * the inline executor, or a test) gets the same "correct for
 * single-writer, not proof against a concurrent double-finalize" behavior
 * `lockRun`'s own doc comment already describes.
 */
export async function maybeFinalizeRun(sql: Db, runId: string): Promise<RunRow | undefined> {
  const counts = await countStepsByStatus(sql, runId)
  const total = Object.values(counts).reduce((a, b) => a + b, 0)
  const done = (counts.completed ?? 0) + (counts.skipped ?? 0)
  if (total === 0 || done < total) return undefined

  const locked = await lockRun(sql, runId)
  if (!locked || (locked.status !== 'queued' && locked.status !== 'running')) return undefined

  const steps = await getStepsByRun(sql, runId)
  if (!isRunComplete(steps)) return undefined // something changed since the aggregate read

  const output: Record<string, unknown> = {}
  for (const step of steps) {
    if (step.status === 'skipped') {
      output[step.name] = undefined
      continue
    }
    const decoded = decodeResult<unknown>(step.result)
    output[step.name] = decoded.ok ? decoded.value : undefined
  }

  const run = await updateRunStatus(sql, runId, 'completed', { output, finishedAt: new Date() })
  await insertHistory(sql, { runId, type: 'run.completed', data: { output } })
  await wakeParentAwaiting(sql, runId)
  return run
}

/**
 * Feature #20's wake side: this run just reached a terminal status, so a
 * step in some OTHER run that is `blocked` on it (control/child.ts) must go
 * back to `ready`. A no-op — one index-backed UPDATE matching nothing — for
 * the overwhelming majority of runs, which nobody is awaiting.
 *
 * **Call this from inside the transaction that wrote the terminal status,
 * or strictly after that transaction committed — never before.** That is
 * what closes the race against a parent step blocking at the same instant:
 * `worker.ts`'s `commitChildBlock` holds `FOR UPDATE` on this run's row
 * while it writes `'blocked'`, so it and the finalizer are serialized on
 * that row, and whichever goes second sees the other's work.
 *
 * Deliberately writes no history row: a history insert takes an implicit
 * FK lock on the *parent's* run row, and the finalizer already holds this
 * (the child's) row — acquiring the two in that order here, while
 * `commitChildBlock` acquires them the other way round, is a textbook
 * deadlock. The wake is legible from the step row itself
 * (`awaited_child_run_id` cleared, `status` back to `ready`).
 */
export async function wakeParentAwaiting(sql: Db, childRunId: string): Promise<StepRow | undefined> {
  return resolveBlockedStepForChildRun(sql, childRunId)
}

/**
 * Reconciliation net for `blocked` steps, meant to be called on a timer
 * (worker.ts runs it on the same tick as the reclaim sweep). Everything
 * that finalizes a run is supposed to call `wakeParentAwaiting` itself, and
 * the paths this repo owns do — but "supposed to" is not a guarantee across
 * every present and future finalization site, and the failure mode of a
 * missed wake is the worst one this phase has: a parent blocked forever on
 * a child that is already done.
 *
 * So this sweeps the other direction, from the awaiting side: for every run
 * still in flight, re-offer each of its `blocked` steps to
 * `resolveBlockedStepForChildRun`, whose `exists (... status in
 * ('completed','failed','cancelled'))` guard makes the call a no-op unless
 * the child really is terminal. It therefore never wakes a step early, and
 * never wakes one twice — which is exactly why running it on a timer does
 * NOT reintroduce polling: a step still blocked on a running child is left
 * untouched, and costs one row read.
 *
 * Cost is O(in-flight runs) reads per tick. Returns the ids of the steps it
 * actually woke, which should normally be zero — a non-empty result means
 * some finalization path skipped its inline wake.
 */
export async function sweepBlockedChildAwaits(sql: Db): Promise<string[]> {
  const woken: string[] = []
  for (const run of await getIncompleteRuns(sql)) {
    const steps = await getStepsByRun(sql, run.id)
    for (const step of steps) {
      if (step.status !== 'blocked' || step.awaited_child_run_id === null) continue
      const resolved = await resolveBlockedStepForChildRun(sql, step.awaited_child_run_id)
      if (resolved) woken.push(resolved.id)
    }
  }
  return woken
}

/**
 * The single entry point worker.ts's `commitOutcome` calls once a step's
 * success is persisted: apply any explicit branch skips (#17), propagate
 * that step's resolution — and every cascaded skip's — to dependents
 * (#15/#16/#19), then check whether the run just finished.
 *
 * `sql` should be an open transaction for the whole thing to be atomic
 * against concurrent callers (see `maybeFinalizeRun`'s doc); worker.ts
 * always calls this from inside `commitOutcome`'s transaction.
 */
export async function advanceDag(
  sql: Db,
  runId: string,
  resolution: StepResolution
): Promise<DagAdvanceResult> {
  const readySteps: StepRow[] = []
  const skippedSteps: StepRow[] = []

  if (resolution.skipNames && resolution.skipNames.length > 0) {
    const siblings = await getStepsByRun(sql, runId)
    for (const name of resolution.skipNames) {
      const target = siblings.find((s) => s.name === name)
      if (!target) continue // named a step that doesn't exist in this run — nothing to skip
      const skipped = await skipStep(sql, target.id, 'branch not taken')
      if (!skipped) continue // already resolved (running/terminal) — safe no-op
      skippedSteps.push(skipped)
      const propagated = await propagate(sql, runId, skipped.name)
      readySteps.push(...propagated.readySteps)
      skippedSteps.push(...propagated.skippedSteps)
    }
  }

  const propagated = await propagate(sql, runId, resolution.completedName)
  readySteps.push(...propagated.readySteps)
  skippedSteps.push(...propagated.skippedSteps)

  const run = await maybeFinalizeRun(sql, runId)
  return { readySteps, skippedSteps, run }
}
