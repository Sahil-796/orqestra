# Phase 4 — Orchestration & DAGs

> Status: **done & verified** · Branch: `phase-4-orchestration-dags`
> Ships (per build plan): *steps run when their dependencies complete; branches fan out to run in parallel and fan back in; results steer conditional paths; a workflow can spawn and await a child workflow.* ✅

Through Phase 3, a workflow was a line: one step after another, occasionally suspended. Phase 4 turns it into a graph. `step.depends_on` — sitting unused in the schema since 0001 — finally earns its keep: a step becomes claimable the moment its dependencies resolve, not the moment its predecessor in some fixed order finishes. That one change is enough to fall out into four shipped features: **#19 step dependencies / DAG, #15 fan-out, #16 fan-in, #17 conditional branching, #20 child workflows.**

No signals or `ctx.waitForEvent` (Phase 5), no triggers or cron (Phase 5), no rate limiting (Phase 6), no dead-letter queue (Phase 7). This phase is entirely about *shape* — what runs, in what order, and what it can wait on.

---

## 1. Fan-out and fan-in are not new primitives

The build plan lists #15 (fan-out) and #16 (fan-in) as separate features, but nothing new had to be built for either. They fall out of `depends_on` for free: N steps naming the same dependency all become `ready` together the instant that dependency resolves — that's a fan-out. One step naming N dependencies becomes `ready` only once every one of them has resolved — that's a fan-in. Phase 2's claim query was already safe under concurrent claims, so N workers picking up N simultaneously-ready siblings was never in question; the only real work was making the *readiness* computation itself safe when N commits race to satisfy the same join.

`WorkflowBuilder.fanOut` (`src/define/workflow.ts`) is sugar, not a primitive:

```ts
fanOut<T>(
  namePrefix: string,
  count: number,
  fn: (index: number, ctx: WorkflowContext) => Promise<T>,
  options: StepOptions = {}
): string[] {
  const names: string[] = []
  for (let i = 0; i < count; i++) {
    const name = `${namePrefix}-${i}`
    names.push(name)
    this.step(name, (ctx) => fn(i, ctx), options)
  }
  return names
}
```

It just registers `count` ordinary steps and hands back their names, so fanning back in is an ordinary `dependsOn`:

```ts
const shardNames = builder.fanOut('shard', 10, processShard, { dependsOn: ['split'] })
builder.step('join', combine, { dependsOn: shardNames })
```

Nothing about the executor or the schema needed to know "this is a fan-out" as a distinct concept. The shipping-bar proof (`tests/fanout.test.ts`) drives this at the scale the build plan cares about: **10 independent `createWorker` instances**, each its own poll loop and lease, racing to claim 10 fan-out siblings, with an explicit `maxConcurrent > 1` assertion so the test can't quietly pass by one worker draining the queue serially. The join runs exactly once no matter how many of the 10 parents commit within milliseconds of each other.

---

## 2. The readiness primitive: a narrow update, not a rescan

This is the part that took actual design work, and it's the center of the phase.

Phase 1's readiness check (`engine/scheduler.ts`'s `newlyReadySteps`) already existed: re-read every step in the run, recompute which `pending` steps now have every dependency `completed`, flip them to `ready`. It's correct, and the Phase 1 inline executor (`startRun`/`executeRun`) still uses it — untouched, on purpose, because that path is single-process and sequential and a rescan there costs nothing worth optimizing.

The worker's durable commit path is a different story. `worker.ts`'s `commitOutcome` takes `FOR UPDATE` on the run row on every single step commit — for a reason that predates this phase entirely (avoiding a lock-upgrade deadlock against `insertHistory`'s implicit FK lock, Phase 2's problem). That lock is already being paid for; the only question this phase controls is how much work happens while it's held. A whole-run rescan is O(run size) per commit. For a run with a 10-way fan-out, that means each of the 10 commits re-reads and re-evaluates all 10+ steps under the run lock — real contention, and it gets worse linearly with fan-out width.

So Unit A built a second, narrower primitive purpose-built for this: `recordDependencySatisfied` (`src/store/repositories.ts`), one UPDATE per commit that only ever touches the one dependent step being satisfied:

```sql
update step
set
  satisfied_deps = (
    select array_agg(distinct d) from unnest(satisfied_deps || array[$depName]::text[]) as d
  ),
  status = case
    when depends_on <@ (
      select array_agg(distinct d) from unnest(satisfied_deps || array[$depName]::text[]) as d
    ) then 'ready'
    else status
  end,
  updated_at = now()
where id = $stepId and status = 'pending'
returning *
```

`satisfied_deps text[]` (added in `0004_orchestration.sql`) is the running tally of which named dependencies have resolved. Each call appends one name and, in the same statement, checks whether `depends_on` is now a subset of that tally (`<@`, Postgres's array-containment operator) — if so, the step flips to `ready` right there. That containment check is what makes "last dependency wins" safe under concurrency without a run-level lock: ten concurrent callers for the same fan-in join each run as their own single-row UPDATE, Postgres's ordinary row-level lock on that one `step` row serializes them, and only the caller whose write happens to complete the set ever observes `status` actually change. No deadlock, because no transaction here ever waits on more than the one row it's already updating.

(One subtlety worth naming because it cost real debugging time: this has to be **one UPDATE**, not two chained CTEs against the same table. Postgres's data-modifying CTEs all execute against the snapshot taken at the *start* of the statement, so a second CTE cannot see a first CTE's write to the same row within one statement — it silently matches zero rows rather than erroring. That was caught by the repository's own tests, not by reading the Postgres docs first.)

`engine/dag.ts`'s `advanceDag` is the policy layer on top: it's what `commitOutcome` calls once a step's success is persisted, and its job is to turn one step's resolution into propagation — walk every dependent that names the resolved step, call `recordDependencySatisfied`, collect whichever come back `ready`, and recurse through any cascade-skips (§3) so a whole untaken branch resolves within one call:

```ts
async function propagate(sql: Db, runId: string, startName: string) {
  const readySteps: StepRow[] = []
  const skippedSteps: StepRow[] = []
  const queue: string[] = [startName]

  while (queue.length > 0) {
    const name = queue.shift()!
    const siblings = await getStepsByRun(sql, runId)
    const dependents = siblings.filter((s) => s.status === 'pending' && s.depends_on.includes(name))

    for (const dependent of dependents) {
      const updated = await recordDependencySatisfied(sql, dependent.id, name)
      if (!updated || updated.status !== 'ready') continue
      // ...cascade check, then push to readySteps
    }
  }
  return { readySteps, skippedSteps }
}
```

Run completion is the one place both paths still have to do something whole-run-shaped — you cannot know "everything is done" without looking at everything. `maybeFinalizeRun` keeps that cheap by staying lock-free until it looks complete: a plain `countStepsByStatus` aggregate on every call, escalating to the same `lockRun` `advanceRun` always took only in the rare case (once per run, not once per commit) where it actually looks finished, and re-verifying under that lock before writing anything.

---

## 3. Conditional branching, and the policy question it raises

`ctx.skip(...stepNames)` (#17) needed no new builder method — it's an ordinary `.step()` whose function happens to call it:

```ts
builder.step('decide', async (ctx) => {
  ctx.skip('branch-b')
  return 'took-a'
})
```

`skip` (`src/define/context.ts`) is deliberately inert by itself: it just records names on a per-context accumulator (a `WeakMap` keyed on the `ctx` object, the same trick `sleepCalls` and `nextChildCallSeq` use). Nothing is written to storage inside the step function — the caller (executor.ts / worker.ts) reads the accumulated names back via `getSkipRequests(ctx)` only *after* the step function has returned successfully, and turns them into `skipStep` calls as part of persisting that step's own outcome. So a skip only takes effect if the deciding step itself actually commits — a step that fails after calling `ctx.skip` skips nothing.

That set up the real design question: **does a skipped dependency satisfy a downstream edge the same way a completed one does?**

The answer has to be yes, uniformly. `recordDependencySatisfied` doesn't judge *why* a dependency resolved — that's a deliberate separation, matching the storage boundary rule (storage tracks state, callers decide what state means). If a skip *didn't* count as satisfying, the untaken half of a conditional branch would permanently strand any join downstream of it — exactly the deadlock #17 must not cause. So `advanceDag` feeds a skip through the identical `propagate()` path a completion goes through; from the fan-in join's perspective, "my dependency resolved" is the only fact that matters, not how.

That answer creates a second problem on its own: what about a step whose *entire* dependency chain was skipped — not a join with one live branch and one dead one, but a step purely downstream of the untaken side? Handing that step to a worker to execute against zero real inputs would be silently running dead code. `cascadeIfAllDepsSkipped` closes this:

```ts
async function cascadeIfAllDepsSkipped(sql: Db, dependent: StepRow): Promise<StepRow | undefined> {
  const deps = await getDependencySteps(sql, dependent.id)
  if (deps.length === 0) return undefined
  const anyCompleted = deps.some((d) => d.status === 'completed')
  if (anyCompleted) return undefined
  return skipStep(sql, dependent.id, `all dependencies skipped: ${deps.map((d) => d.name).join(', ')}`)
}
```

The rule: if at least one dependency actually completed, proceed as a genuine `ready` — a real fan-in join past an untaken branch is unaffected the instant its live sibling completes. Only when *every* dependency resolved by being skipped does this step cascade to `skipped` too, and `propagate()` recurses on that cascade so a whole chain (branch → branch's child → that child's child) resolves within a single `advanceDag` call rather than needing N separate worker claims just to discover N consecutive no-ops.

`tests/dag.test.ts` proves all three shapes: a two-way branch where the untaken side is skipped and the join runs on the taken branch alone; a chain three deep where the middle and the tail both cascade without ever executing; and a join whose *every* branch was skipped, which itself resolves to `skipped` rather than deadlocking.

---

## 4. Child workflows: built twice, on purpose

This is the part of the phase worth telling honestly, because the first version was wrong in a specific, instructive way.

**Attempt one** was a poll loop: `awaitChildRun` would call `ctx.sleep(pollIntervalMs)`, check the child's status, sleep again if not terminal, repeat. It worked. It also meant every parent step waiting on a child was periodically re-claimed, re-executed from the top (replaying everything before the poll, per Phase 3's sleep-replay contract), and burned a `sleep_seq` slot on every iteration — for a child that might take milliseconds or might take hours. Polling ties wake latency to a poll interval the same way Phase 3 §12 already admits sleep does, except here it's not "wakes within about one poll interval of schedule," it's "re-checks, over and over, for the entire lifetime of the child" — real, avoidable churn for no reason other than "the mechanism I already had was `sleep`."

**The rebuild** replaced polling with an actual event: suspend once, get woken by the thing that resolves the wait. Three pieces:

1. **`awaitChildRun`** (`src/control/child.ts`) reads the child's outcome exactly once. If it's not terminal, it throws `ChildBlockSignal` — the same species of control-flow signal `SleepSignal` already is, branded rather than checked with `instanceof` for the same cross-module-identity reason Phase 3 uses for `isSleepSignal`.
2. **`worker.ts`** classifies that signal in its `Attempt` union right alongside sleep/timeout/cancel, and commits `blockStepOnChildRun` — status `'blocked'`, `awaited_child_run_id` set, lease cleared — fenced on `lease_owner` inside the transaction, exactly the way `commitSleep` fences `sleepStep`. `'blocked'` is a new terminal-ish status added in `0004_orchestration.sql`, invisible to `claimNextStep` (which only looks at `ready`) and to the lease reaper (which only scans `running`) — nothing can resurrect it early, and a crash mid-wait leaves it exactly where it was, a row, not a dangling promise.
3. **`resolveBlockedStepForChildRun`** flips it back to `ready` once the child run's own status becomes terminal. The step is then re-claimed like any other `ready` row and replays from the top — where `getChildOutcome` now returns instead of throwing, so the signal is never raised twice.

The event-driven version needs no `seq` counter the way sleep needs `sleep_seq`, and that's worth noting as the one place child-await is *simpler* than sleep despite doing more: a woken sleep would re-suspend on the same `ctx.sleep()` call forever without a counter to know it's already been served, but a woken child-await re-checks the child's actual status first, and the only thing that could have caused the wake is that status having gone terminal. Self-resolving by construction.

`tests/child-blocking.test.ts` is the proof this is real: a child that takes ~700ms of wall clock, watched for half a second while blocked — `attempt` and `awaited_child_run_id` never move, no `step.sleeping` history row is ever written, and the step wakes only once the child actually finishes, not on any interval.

### Closing the block/wake race with a lock, not a retry

The obvious race: the child finishes in the window between `awaitChildRun`'s read (still in flight) and `commitChildBlock`'s write (`'blocked'`) — the finalizer looks for a blocked step that isn't there yet, and never comes back to check again.

This is closed with lock ordering, not a reconciliation retry (though a backstop exists too — see below). `commitChildBlock` takes `FOR UPDATE` on the **child** run row *first*, before it fences the parent step and before it locks the parent run:

```ts
async function commitChildBlock(step: StepRow, run: RunRow, signal: ChildBlockSignal): Promise<void> {
  await withTransaction(db, async (tx) => {
    const child = await lockRun(tx, signal.childRunId)   // 1. child run — FIRST
    const owned = await lockStepIfOwner(tx, step.id, workerId)  // 2. parent step
    await lockRun(tx, run.id)                              // 3. parent run
    const blocked = await blockStepOnChildRun(tx, { stepId: step.id, workerId, childRunId: signal.childRunId })
    if (isTerminalRunStatus(child.status)) {
      // the child went terminal in the gap between the read and this write —
      // un-block what we just blocked, immediately, in the same transaction
      await wakeParentAwaiting(tx, signal.childRunId)
    }
  })
}
```

Every finalization path calls `wakeParentAwaiting` (which is just `resolveBlockedStepForChildRun`) while holding — or strictly after releasing — that same child-run row lock. So the two possible orderings are the *only* two possible orderings, and both self-correct:

- **Block first.** The finalizer's own `lockRun` on the child waits for this transaction to commit, then finds a genuinely `blocked` step and flips it back.
- **Finalize first.** This transaction's `lockRun(tx, signal.childRunId)` waits for the finalizer, then reads the now-terminal status and immediately calls `wakeParentAwaiting` on the block it just wrote, in the same transaction — the step lands `ready`, not `blocked`, without ever externally appearing stuck.

Lock order is child-run → parent-step → parent-run. That specific order matters for a second reason beyond the race: `commitOutcome` (the ordinary success-commit path) already locks step → run, in that order, for its own unrelated deadlock-avoidance purpose (Phase 2/3's `insertHistory`-FK-lock problem). Child-block adds a third row — the child run — ahead of both, rather than wedging it in the middle or after; putting it first means no transaction anywhere in the system ever holds one of {child run, parent step, parent run} while waiting on another in the *opposite* direction, which is the actual definition of "no deadlock cycle," not just "this one path looks safe."

**The wake deliberately writes no history row.** `wakeParentAwaiting`'s doc comment is explicit about why: a `history` insert takes an implicit `FOR KEY SHARE` lock on its `run_id`'s row via the foreign key — and here that would be the *parent's* run row. But the finalizer calling the wake already holds the **child's** row lock at that point (from `maybeFinalizeRun`'s own `lockRun`, or from `commitChildBlock`'s), and taking the parent's row via a history insert on top of that is the same "acquire the two in this order here, the other order there" shape that caused the deadlock this whole ordering exists to avoid — just discovered on the write side instead of the lock side. The wake stays legible without a history row anyway: `awaited_child_run_id` cleared and `status` back to `ready` is a complete, honest audit trail for what happened and when, readable straight off the step row.

### The attempt accounting, shared with sleep

`blockStepOnChildRun` decrements `attempt` the same way `sleepStep` does (`attempt = greatest(attempt - 1, 0)`), and for the identical reason: `attempt` increments at *claim* time, but suspending to wait on a child is not a failed try — it's the step asking to be continued once something else finishes. Without the refund, a step that awaits N children in sequence (or one child, if `maxAttempts: 1`) would silently burn its retry budget on suspensions rather than actual failures and die of "out of retries" having never really failed at anything. `tests/child-blocking.test.ts` pins this directly: a step that runs its body twice — once to spawn-and-block, once to replay through to the result — ends at `attempt === 1`, exactly as if it had executed once.

### Every finalization path has to remember to wake

Because the wake is keyed off "some run went terminal," and runs can go terminal from more places than just the happy path, every one of those places has to call `wakeParentAwaiting` (or, for the crash-sweep case, its cousin) or a parent can wait forever on a child that already finished. The full list, as it stands after this phase:

- `engine/dag.ts`'s `maybeFinalizeRun` — the ordinary completion path.
- `worker.ts`'s failure commit and its cancellation commit.
- `engine/executor.ts`'s inline finalize and both its failure branches (the single-process driver has its own terminal paths, entirely separate from the worker's).
- `control/cancel.ts`'s `cancelRun` (the synchronous-finalize branch) and `sweepCancelledRuns` (the janitor).
- `queue/lease.ts`'s poison-pill branch — a step that's exhausted its retries via repeated lease expiry finalizes the run there too, and that's still a terminal transition a parent could be waiting on.

That's a lot of call sites to keep in sync by hand, which is its own risk, so there's a backstop: `sweepBlockedChildAwaits` runs on the same tick as the reclaim sweep, walks every in-flight run's `blocked` steps, and re-offers each one to `resolveBlockedStepForChildRun` — a no-op unless the child really is terminal, so it can never wake a step early and never wakes one twice. In normal operation it should always find nothing; a non-empty result is logged as a warning, because it means some inline finalization path above missed its call. It's a net, not the primary mechanism — the primary mechanism is still every terminal transition waking its own waiters inline, the same tick it happens.

### What the inline executor does not do

`engine/executor.ts`'s single-process `startRun`/`executeRun` does not support child workflows, for the same reason it doesn't support `ctx.sleep`: it's sequential and single-process, so there is no second actor that could ever run the spawned child. A `ChildBlockSignal` reaching that path is treated as an ordinary step failure rather than a suspend — the inline driver was never meant to run anything but a fully synchronous, single-worker DAG, and orchestration that spans two runs is out of scope for it by construction, not by oversight.

---

## 5. Migration `0004_orchestration.sql`

Append-only, as always — 0001 through 0003 are untouched.

```sql
alter table step add column satisfied_deps text[] not null default '{}';

alter table step drop constraint step_status_check;
alter table step add constraint step_status_check
  check (status in ('pending', 'ready', 'running', 'completed', 'failed', 'cancelled', 'skipped', 'blocked'));

alter table step add column skip_reason text;
alter table step add column awaited_child_run_id uuid references run (id);

alter table run add column parent_run_id uuid references run (id);
alter table run add column parent_step_id uuid references step (id);

create index run_parent_run_id_idx on run (parent_run_id) where parent_run_id is not null;
create index step_awaited_child_run_id_idx on step (awaited_child_run_id) where status = 'blocked';
```

- **`satisfied_deps`** — §2's running tally, read and written only by `recordDependencySatisfied`.
- **`'skipped'` / `'blocked'`** — both slot into 0001's `text` + `CHECK` status columns exactly as designed: new terminal-ish values with no `ALTER TYPE`, no migration risk to existing rows. `skipped` is #17's "never going to run, nothing went wrong" state; `blocked` is #20's "durably waiting on something that isn't a clock."
- **`skip_reason`** — observability, mirroring `error`'s role for `failed`: a skipped row should be able to explain itself without a join into `history`. Never read by the claim path or any readiness check.
- **`awaited_child_run_id`** — unlike `sleeping_until` (Phase 3, deliberately non-load-bearing), this one *is* load-bearing: `resolveBlockedStepForChildRun`'s `WHERE` clause is built on it directly.
- **`parent_run_id` / `parent_step_id`** on `run` — the child-linkage half that doesn't fit on `step`. Both nullable and both optional in `createRun`; every existing caller that omits them is unaffected.
- **Two partial indexes**, same reasoning as Phase 3's `run_cancel_requested_idx`: `run_parent_run_id_idx` isn't partial (most runs *could* plausibly have children looked up, so it's a stable reusable key, not a rare flag), but `step_awaited_child_run_id_idx` is scoped to `status = 'blocked'` — the matching set is always tiny relative to the whole step table, keeping the wake-side lookup an index scan rather than degrading as the table grows.

---

## 6. What landed, file by file

**New:**
- `src/engine/dag.ts` — `advanceDag`, `maybeFinalizeRun`, `wakeParentAwaiting`, `sweepBlockedChildAwaits`, the cascade-skip logic. The worker's entry point for everything this phase adds to the commit path.
- `src/engine/child.ts` — pure: `ChildWorkflowError`, `ChildBlockSignal` (+ brand checks), `classifyChildRun`, `toChildWorkflowError`, `isTerminalRunStatus`. No I/O, unit-testable without Postgres, same posture as `engine/sleep.ts`/`engine/timeout.ts`.
- `src/control/child.ts` — the impure half: `spawnChildRun` (idempotent, keyed so a replay after a block never spawns a child twice), `getChildOutcome`, `awaitChildRun`, `runChildWorkflow` (throws on child failure), `runChildWorkflowResult` (never throws).
- `src/store/migrations/0004_orchestration.sql` — §5.
- `tests/dag.test.ts`, `tests/fanout.test.ts`, `tests/repositories-dag.test.ts` — #19/#15/#16/#17 proofs at the storage and worker-loop layers.
- `tests/child-workflows.test.ts`, `tests/child-blocking.test.ts` — #20's behavioral proof (spawn/await, propagation policy, crash-style resume) and its mechanism proof (the block itself, who wakes it, the race).
- `tests/orchestration-e2e.test.ts` — the phase's own end-to-end proof (§7).

**Changed:**
- `src/store/repositories.ts` — `getDependencySteps`, `recordDependencySatisfied`, `skipStep`, `getChildRuns`, `blockStepOnChildRun`, `resolveBlockedStepForChildRun`; `StepRow` gains `satisfied_deps`/`skip_reason`/`awaited_child_run_id`, `RunRow` gains `parent_run_id`/`parent_step_id`, `createRun` grows two optional fields.
- `src/define/context.ts` — `ctx.skip(...)` (+ `getSkipRequests`), `nextChildCallSeq`/`getContextStepId` (internal, `WeakMap`-keyed, for `control/child.ts`'s replay-safe spawn keys).
- `src/define/workflow.ts` — `WorkflowBuilder.fanOut`.
- `src/worker/worker.ts` — the `Attempt` union gains `'child-block'`; `commitChildBlock` (the lock-ordering §4 describes); `advanceDag`/`wakeParentAwaiting`/`sweepBlockedChildAwaits` wired into the commit paths; the reclaim tick now also runs the blocked-child sweep.
- `src/control/cancel.ts`, `src/queue/lease.ts`, `src/engine/executor.ts` — each gains a `wakeParentAwaiting`/`resolveBlockedStepForChildRun` call at its own terminal-transition point (§4's finalization-path list).

---

## 7. What ships, and how it's proven

Per-feature mechanics have their own dedicated tests, listed above; `tests/orchestration-e2e.test.ts` is the composition proof — one workflow exercising every shipped feature together, not feature-by-feature, driven by **6 concurrent `createWorker` instances** against real Postgres:

```
seed --> shard-0..4 --> aggregate --(skip one branch)--> fast-path (spawns+awaits a child) --\
                                                        \                                       --> final
                                                          slow-path (skipped, never runs) ------/
```

`aggregate` is a genuine fan-in that makes a genuine data-dependent branching decision: it reads its five fan-out siblings' *actual committed results* back out of storage (a step function only ever sees `ctx.input`, never a sibling's output directly — that only exists in the final `run.output`, so the test decodes each dependency's persisted `Result<T>` envelope the same way the worker does) and sums them, then calls `ctx.skip` on whichever branch the sum didn't select. `fast-path` is the taken branch, and it spawns and awaits a full child workflow — so it exercises #20's block/wake mechanism inline, inside a graph that's simultaneously mid-fan-in and mid-branch. `final` fans back in past one real branch and one cascade-skipped one.

Assertions: the run completes; `slow-path` never executes (`skipped`, with `skip_reason`); `maxConcurrentShards > 1` (genuine overlap across the fleet, not one worker serially draining); every step's execution count is exactly what the mechanism predicts — 1 for every ordinary step, 2 for `fast-path` (the documented spawn-and-block execution plus the replay-to-result execution) with its **`attempt` still landing at 1** (the refund proof), 0 for `slow-path`; the child run itself completed with both of its own steps run exactly once; and the final output threads the child's result all the way through `fast-path` into `final`. Run several times back to back — a concurrency proof that only passes once isn't proven — and it was stable across 6 consecutive runs.

### Verification

| Check | Result |
|---|---|
| `bunx tsc --noEmit` (strict) | zero errors |
| `bun test` | 143 pass / 0 fail across 20 files |
| `tests/orchestration-e2e.test.ts` alone, 6 consecutive runs | stable — 1 pass / 0 fail every time, ~450–510ms |

Tests run against the Dockerized Postgres 16 from Phase 0 (port 5433), each case scoped to its own random namespace and workflow name.

---

## 8. What Phase 4 deliberately does NOT do

No signals or `ctx.waitForEvent` — a workflow can wait on a child run's completion (#20), but not on an arbitrary named external event; that's still Phase 5's stub, unchanged from Phase 3. No triggers, cron, or webhooks — everything in this phase is still enqueued the same way Phase 1 always enqueued a run. No rate limiting or concurrency caps on how many steps/runs execute at once (Phase 6) — the 10-way and 6-way fleets in this phase's tests are unthrottled by anything other than however many workers you start. No dead-letter queue or manual retry/compensation UI for a permanently failed step (Phase 7) — a step that exhausts its retries still fails the whole run exactly as it did in Phase 3, and (new to this phase) still wakes any parent blocked on it, so a failed child is at least never a parent stuck waiting forever, just a parent that now has to decide what to do with `ChildWorkflowError`.

One honest limit inside what *did* ship: `sweepBlockedChildAwaits` is a backstop for finalization paths that forget to wake their waiters, not a substitute for getting the inline call right. Every terminal-transition site this phase touched calls `wakeParentAwaiting` (or `resolveBlockedStepForChildRun`) inline; the sweep exists because "every present and future finalization site remembers to call it" is a claim about human diligence, not a proven invariant, and the failure mode of a missed wake — a parent blocked forever on a child that's already done — is the worst one in this phase. The sweep runs on the same tick as the lease reclaim sweep and logs a warning if it ever actually wakes something, which would mean an inline call was missed somewhere.

---

## Next: Phase 5 — Triggers & Events
Cron schedules, webhooks, and `ctx.waitForEvent` land here — the ways a run can start or resume for a reason other than "another step in the same graph resolved," which this phase deliberately kept out of scope.
