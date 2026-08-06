# Phase 3 — Execution control (sleep, timeouts, cancellation)

> Status: **done & verified** · Branch: `phase-2-queue-workers`
> Ships (per build plan): *`await ctx.sleep("24h")` that releases the worker and wakes on schedule.* ✅

Phase 2 turned the step log into a claimable queue that N workers drain with expiring leases. Everything that ended a step was still the step's own doing: it returned, or it threw. Phase 3 adds the three ways a step can end **because of time or because someone asked** — it suspends itself (`ctx.sleep`), it runs out of budget (`timeout_ms`), or its run is cancelled underneath it. Features delivered: **#9 sleep / delayed steps, #10 step timeouts, #11 cancellation.**

All three land in the worker's claim/run/commit loop, because that loop is the only place in the system that holds a lease while user code is executing. None of them weaken Phase 2's fencing: every new write path re-checks lease ownership inside the transaction that writes.

No DAG fan-out, no `waitForEvent`, no dead-letter queue — Phase 4, 5 and 7 respectively.

---

## 1. Sleep is a row, not a held worker

The temptation with `ctx.sleep("24h")` is to implement it as a timer: park the promise, keep the lease alive with heartbeats, resume when the timer fires. That is wrong for the same reason Phase 1 refuses to hold a transaction across user code — it puts durable state in a place that dies with the process. A 24-hour sleep implemented as a `setTimeout` is lost the first time you deploy.

orqestra's sleep is the opposite: it writes the wake time to the row and **gives the worker back**. `sleepStep` (`src/store/repositories.ts`):

```sql
update step set
  status = 'ready',
  run_after = ${args.wakeAt},
  sleeping_until = ${args.wakeAt},
  sleep_seq = sleep_seq + 1,
  attempt = greatest(attempt - 1, 0),
  lease_owner = null,
  lease_expires_at = null,
  updated_at = now()
where id = ${args.stepId} and status = 'running' and lease_owner = ${args.workerId}
returning *
```

That is the entire scheduler. There is no wake sweep, no timer wheel, no separate "sleeping" status. A sleeping step is `ready` with a `run_after` in the future, which the Phase 2 claim query already filters out:

```sql
where s.status = 'ready'
  and s.run_after <= now()
  ...
```

So the whole fleet simply cannot see the row until it is due, and then sees it as an ordinary claimable step. The ships criterion — *"releases the worker and wakes on schedule"* — is satisfied **structurally**, not by bookkeeping: there is nothing to release, because nothing was held. No worker slot, no pooled connection, no in-memory entry. Every worker in the pool can be restarted mid-sleep and the wake time survives, because it was never in anyone's memory.

This is the same mechanism retry backoff already uses (Phase 2 §3), which is exactly why `sleeping_until` exists at all. Without it, a sleeping step and a step in retry backoff are byte-for-byte identical rows — both `ready`, both with a future `run_after` — and "why is this run sitting still?" is unanswerable from the row. `sleeping_until` is **observability only**: nothing in the claim path reads it, so a stale value can never hold a step back from being claimed. It is set on suspend, and `clearSleepMarker` clears it (unfenced, best-effort, deliberately) when the woken step is actually running again.

`getSleepingSteps` is the read side of that marker — `status = 'ready' and sleeping_until > now()` — which is what the tests and the example's observer use to see a nap in progress.

---

## 2. Replay: the confusing part, and why `sleep_seq` exists

Here is the thing that trips everyone up. **A sleeping step is not a suspended coroutine.** We do not freeze a JS stack across a 24-hour sleep and a process restart — on a crash-proof engine we *cannot*. So when the step wakes, the step function is claimed and **re-run from the very top**.

Which means it hits the same `await ctx.sleep(...)` call again. Without a durable counter, that call suspends again, the step goes back to `ready` with another future `run_after`, wakes, re-runs from the top, suspends again — an infinite suspend loop that never advances past the first sleep. The row would look perfectly healthy the whole time.

`step.sleep_seq` is the fix: **how many `ctx.sleep()` calls this step has already served.** `sleepStep` bumps it (`sleep_seq = sleep_seq + 1`) as part of suspending, the worker passes it into the context on the next execution, and `ctx.sleep` replays against it (`src/define/context.ts`):

```ts
sleep: async (duration: string | number): Promise<void> => {
  // Parse before the replay check so a malformed duration fails loudly on
  // every execution, not only the one that would have suspended.
  const durationMs = parseDuration(duration)
  const seq = ++sleepCalls
  if (seq <= alreadyServed) return
  throw new SleepSignal({ wakeAt: new Date(Date.now() + durationMs), durationMs, seq })
}
```

`sleepCalls` is per-context, i.e. per-execution; `alreadyServed` is the durable `sleep_seq`. Any call whose 1-based index is `<= sleep_seq` was already served on a previous execution and resolves immediately. Only the first call *beyond* it actually suspends. A step with two sleeps therefore runs three times total: seq 1 suspends → seq 1 skipped, seq 2 suspends → both skipped, step runs to its return value.

This is the sleep-side equivalent of the memoized-result replay that makes any durable engine's re-execution safe. The cost is a contract on step bodies: **everything before a sleep runs again on every wake**, so it must be idempotent — the same contract retries have imposed since Phase 2.

### Sleep must not consume retry budget

The other half of `sleepStep`'s SET list is `attempt = greatest(attempt - 1, 0)`, and it is not cosmetic. `attempt` is incremented at *claim* time (Phase 2 §3: claiming is committing to an attempt) and checked against `max_attempts`. But a sleep is not a failed attempt — it is the step asking to be continued later. A step with `maxAttempts: 1` that sleeps once would be claimed (attempt → 1), suspend, wake, be claimed again (attempt → 2), and die of "out of retries" without ever having failed at anything.

So the sleep hands the attempt back. `greatest(..., 0)` keeps the column non-negative if it is ever called on a step claimed by some path that didn't increment. The end state of a one-sleep step is `attempt = 1` after two claims — exactly as if it had run once.

`engine/sleep.ts` itself is pure (no I/O, no `postgres` import, same posture as `engine/retry.ts`): a branded `SleepSignal` control-flow signal, `isSleepSignal`, and `parseDuration` for the `ms/s/m/h/d/w` grammar including compound forms (`"1h30m"`) and bare-number milliseconds. The grammar is anchored-scanning, so `"1h30x"` throws rather than quietly meaning 1h.

`isSleepSignal` is a **brand check, not `instanceof`**, and that choice is load-bearing: a module loaded through two different specifiers produces two distinct `SleepSignal` classes, and `instanceof` across them is `false`. Misclassifying a sleep as a step failure would burn an attempt and could fail the whole run. `isStepTimeoutError` uses the same shape for consistency.

---

## 3. The determinism consequence, stated honestly

`ctx.now()` and `ctx.random()` are real implementations, not recorded-and-replayed. Phase 1 §6 deferred that, and Phase 2 didn't change it. Through Phase 2 the deferral was genuinely invisible: the memoization boundary was the *whole step*, so a step either `completed` (its result reused verbatim, its internal `now()`/`random()` never consulted again) or re-ran from scratch (nothing committed to be inconsistent with). Only a failed or crashed attempt ever saw fresh values, and that attempt's results were discarded anyway.

Sleep breaks that. A sleeping step **partially progresses and then re-runs**, so code before the sleep executes twice and observes a different `now()` and a different `random()` the second time. This is now genuinely observable in ordinary, correct usage — not an edge case.

We are not selling this as solved. The contract for now: **a value that must survive a sleep has to come from `ctx.input`, or be persisted by the step itself.** Recording `now()`/`random()` for true deterministic replay is a future refinement, and the note lives at the top of `src/define/context.ts` where a step author will hit it.

---

## 4. Timeouts, and what a timeout can't do

`withTimeout` (`src/engine/timeout.ts`) races the step function against `step.timeout_ms` and hands the worker a decision. It is pure — no I/O, unit-tested without Postgres (`tests/timeout.test.ts`).

Be honest about what this buys: **there is no preemption in JavaScript, and there cannot be.** All a timeout does is (a) abort an `AbortSignal` the step function may be watching, and (b) stop *waiting* for the step's promise so the worker can commit a `timed_out` outcome and move on. A step that ignores its signal keeps running — its promise stays pending (or settles later into a result nobody reads) and a genuinely runaway loop keeps burning CPU in the worker process until it returns on its own. The step row is marked timed out and the worker slot is freed; **the work is only stopped if the step cooperates.** Killing uncooperative code needs process isolation, which this phase does not buy.

### The ordering that has to be right

Inside the timer callback:

```ts
timer = setTimeout(() => {
  const error = new StepTimeoutError(budget)
  // Reject BEFORE aborting, and the order is load-bearing. ...
  reject(error)
  controller.abort(error)
}, budget)
```

Abort first and you fire the step's own abort listener **synchronously**. A cooperative step that rejects from that listener settles the race first, so the caller sees *the step's* bail-out error and never learns it timed out — the step gets blamed for a failure the engine caused, and the `step.timed_out` history row is never written. Settling `expired` first makes the label deterministic: a timeout always reads as a timeout, however fast the step reacts. The abort still lands immediately after, so the step is told to stop either way.

Two other details that matter in a hot loop:

- `Promise.race` attaches a handler to **both** promises, so the loser — which routinely settles *after* the race is decided — can never surface as an unhandled rejection.
- Every exit path clears the timer and unsubscribes from the parent signal. A leaked timer both keeps the event loop alive (a process that won't exit on shutdown) and pins the closure it captured.

`withTimeout` also takes a `parent` signal, which is how cancellation reaches the step (§5). A parent abort deliberately does **not** produce a `StepTimeoutError`: whatever `fn` throws on the way out is what surfaces, so the caller can tell "cancelled" from "out of time". Only this function's own timer ever mints a `StepTimeoutError`. `timeoutMs` that is null/undefined/non-positive means no budget — no timer is created at all, and the step still gets a real signal so parent cancellation still reaches it.

### Persisting a timeout

A timeout is an ordinary failure as far as the retry decision goes — it takes the same `shouldRetry`/`nextRunAfter` path as a throw. The only difference is an extra history row written before the decision:

```ts
if (outcome.timedOut) {
  await insertHistory(tx, {
    runId: run.id, stepId: step.id, type: 'step.timed_out',
    data: { attempt: owned.attempt, timeoutMs: owned.timeout_ms, error },
  })
}
```

Deliberately additive: the `step.retry_scheduled` / `step.failed` row that follows is unchanged, so nothing reading Phase 2's history shape breaks — the timeline just gains the ability to say *why* the attempt ended.

---

## 5. Cancellation is cooperative, and the API says so

You cannot kill a step function executing inside another process. So `cancelRun` does not pretend to. The honest contract is *"the request is durable, the finalization happens at the next safe point"*, and `CancelResult` exposes which of the two happened:

```ts
export interface CancelResult {
  requested: boolean   // a request was newly recorded
  finalized: boolean   // the run reached `cancelled` synchronously
  pending: boolean     // a step is running elsewhere; its worker will finalize
  run: RunRow | undefined
}
```

`cancelRun` records intent via `requestRunCancellation` (whose `WHERE status in ('queued','running') and cancel_requested_at is null` is the entire no-op story: a terminal run has nothing to cancel, and a repeat ask must not move the timestamp), then branches on whether anything is `running`:

- **Nothing running** → `finalizeCancelledRun` right here: lock the run `FOR UPDATE`, flip `pending`/`ready` steps to `cancelled` so nothing else can be claimed, flip the run. One transaction, so a reader never sees a `cancelled` run whose steps are still claimable.
- **A step is running** → record the request and return `pending: true`. **We do not touch the running row.** Stomping it would race that worker's fenced commit and could produce two outcomes for one step — precisely the unfenced double-write Phase 2's `lockStepIfOwner` exists to make impossible.

`finalizeCancelledRun` leaves `running` steps alone for the same reason, which is what makes the benign race in `cancelRun` benign: a worker can claim a `ready` step between the count and the transaction, but that worker still owns its step's outcome and its fenced commit still wins — the run just reaches `cancelled` a beat before that one step does.

### The two checkpoints

The lease holder observes the request at exactly two places in `runClaimedStep`, and both are needed.

**Checkpoint #1 — right after claiming, before a line of the step function runs.** A cancel request is *not* visible in `run.status` (the run stays `running` until someone finalizes it), so this takes its own fresh read rather than trusting the cached run row:

```ts
if (await isCancellationRequested(db, run.id)) {
  await commitCancellation(step, run, 'pre-run')
  return
}
```

Starting work on a doomed run is pure waste — but more importantly, **this is also the gate that stops a step that slept *before* the cancel from waking up and running.** That step has no worker, no lease, no signal to abort; it is a row with a future `run_after`. Nothing can reach it while it sleeps. It wakes, gets claimed like any due step, and lands here.

**Checkpoint #2 — the heartbeat tick.** It lives there because that timer already exists and already round-trips to Postgres every `heartbeatIntervalMs`, so noticing a cancel costs one extra cheap `exists(...)` on a connection we were using anyway. No new timer, no new poll loop. All the worker can then do from the outside is abort the controller; whether the step actually stops is up to the step.

`isCancellationRequested` returns a single boolean computed in Postgres rather than dragging the whole run row (input/output `jsonb` included) across the wire on every tick.

### The janitor

`sweepCancelledRuns` closes the crash window: a request lands, and then the process that was going to act on it dies. The run would otherwise sit `queued`/`running` with `cancel_requested_at` set forever. The sweep scans `getCancelRequestedRuns` (backed by `run_cancel_requested_idx`, a partial index on exactly that predicate, oldest first so a cancel can't be starved by newer ones), skips any run that still has a `running` step, and finalizes the rest. It runs serially, not `Promise.all` — each finalize takes a row lock and the sweep is a background janitor, not a latency path.

---

## 6. Outcome precedence in the worker

One execution of a step function now classifies into five outcomes, and only two of them are failures:

```ts
type Attempt =
  | { kind: 'success'; value: unknown }
  | { kind: 'sleep'; signal: SleepSignal }
  | { kind: 'cancelled' }
  | { kind: 'timeout'; error: unknown }
  | { kind: 'failure'; error: unknown }
```

The classification order in the `catch` is forced, not stylistic:

```ts
if (isSleepSignal(e)) {
  attempt = cancelObserved ? { kind: 'cancelled' } : { kind: 'sleep', signal: e }
} else if (cancelObserved) {
  attempt = { kind: 'cancelled' }
} else if (isStepTimeoutError(e)) {
  attempt = { kind: 'timeout', error: e }
} else {
  attempt = { kind: 'failure', error: e }
}
```

- **Sleep first.** A `SleepSignal` is control flow, not a failure, and must survive the timeout race untouched. `withTimeout` only ever *adds* a `StepTimeoutError` of its own — it never rewrites what `fn` threw — so a sleep that unwound before the budget expired arrives here intact. If it were checked after the timeout branch, a step that slept near its deadline could be recorded as a failed attempt: an attempt burnt and, on a `maxAttempts: 1` step, a dead run.
- **...unless the run is already cancelled**, in which case suspending a step for 24h just to cancel it on wake is silly. Cancel it now.
- **Cancel outranks timeout.** Once we abort the signal, a cooperative step throws (an `AbortError`, or its own error) and an uncooperative one may well go on to blow its budget. Both are *consequences of the cancel*. Recording either as a timeout would blame the step for something we did to it.

The context is built **inside** `withTimeout` so `ctx.signal` is the combined signal (timeout ∪ cancellation) rather than one or the other — a step that watches its signal bails on whichever fires first.

---

## 7. Every Phase 2 fencing invariant still holds

Phase 2's rule was: no worker writes an outcome without re-asserting, *inside the transaction that writes*, that it still owns the lease. Phase 3 adds two more commit paths, and both obey it.

**`commitSleep`** takes `lockStepIfOwner` first; if ownership moved on it writes nothing and logs. Then `lockRun`, then `sleepStep` — whose own `WHERE ... and status = 'running' and lease_owner = ${workerId}` re-asserts the same fence and is the fence of record (under the `FOR UPDATE` above it cannot miss, but a missing row still means "write nothing"). Without this, a zombie worker whose lease was already reclaimed could put a step to sleep that its new owner is currently executing — parking a `running` step back into the queue for 24 hours, under someone else's feet.

**`commitCancellation`** is the same shape: `lockStepIfOwner`, `lockRun`, then `cancelRunningStep` (fenced identically), then a `step.cancelled` history row stamped with the `workerId` and the phase. Only after that transaction commits does it call `finalizeCancelledRun` — a **separate** transaction on purpose, since `finalizeCancelledRun` opens its own to lock the run and flip the remaining steps atomically, and by then this worker's step is durably out of `running`, so there is nothing left in flight for this run from this worker.

**Lock ordering is `lockRun` before anything that touches `run_id`**, in every one of the three paths. This is the Phase 2 §6 deadlock, and it applies unchanged: `insertHistory`'s `INSERT` takes an implicit `FOR KEY SHARE` on the parent `run` row via the FK, and two transactions that each hold that weaker lock and then both request `FOR UPDATE` deadlock on each other. Taking the exclusive lock up front avoids the upgrade entirely — the second transaction simply waits at that line, and its later reads see the first's writes. `commitSleep` and `commitCancellation` are new entrants into exactly that contention, so they take the lock in exactly the same place `commitOutcome` does.

`sleepStep` and `cancelRunningStep` both also clear `lease_owner`/`lease_expires_at` as part of their own `UPDATE`, so neither path calls `releaseLease` separately — the lease release is in the same statement as the state change, and cannot be half-applied.

And the Phase 2 `abandoned` fast path is unchanged and still not the safety mechanism: `if (abandoned) return` before the switch is an optimization; each `commit*` re-checks in the DB regardless, so losing the race between "the step settled" and "the heartbeat noticed" is caught there.

---

## 8. A known, intentional edge case: the cancel that arrived too late

If a step **succeeds** before it observes the cancel — the request lands between the last heartbeat tick and the step's return — the worker commits that success. That is correct: the step really did run to completion, its side effects really happened, and its result is durably true. Pretending otherwise would be a lie in the log.

What happens next depends on where the step sat in the run:

- **Not the last step.** `advanceRun` flips the next step to `ready`, some worker claims it, and cancellation checkpoint #1 fires before its body runs. The step is cancelled `pre-run`, `finalizeCancelledRun` cancels the rest, and the run ends `cancelled`. The successful step stays `completed` in the history, which is exactly the truth.
- **The last step.** `advanceRun` sees every step `completed` and finalizes the run as **`completed`**, not `cancelled`. The cancel arrived after the work it was trying to stop had already finished.

This is intended behaviour, not a gap. Cancellation is cooperative; a cooperative cancel that arrives after the last unit of work has committed has nothing left to cancel. The alternative — retroactively marking a finished run `cancelled` — would mean the run's status disagreed with its own step log.

---

## 9. Migration `0003_execution_control.sql` — three columns, one index

Most of what Phase 3 needed was already reserved in the 0001 schema: `step.run_after` (the claim gate), `step.timeout_ms`, and `'cancelled'` in both status `CHECK` constraints. Append-only as always; 0001 and 0002 are untouched.

- `alter table run add column cancel_requested_at timestamptz` — a *request* flag distinct from the terminal `status = 'cancelled'`, because cooperative cancellation needs somewhere to record intent that isn't the terminal state. Nullable timestamp rather than a boolean: *when* the cancel was asked for is the interesting datum for latency debugging ("we asked at T, the run stopped at T+9s"), and `NULL` is a cheaper "no request" than a `NOT NULL` boolean default.
- `alter table step add column sleep_seq integer not null default 0` — §2. `not null default 0` needs no backfill.
- `alter table step add column sleeping_until timestamptz` — §1, observability only, deliberately kept out of the `CHECK`/claim logic.

**No new index for the wake path**, and that is the point of the design. Waking is not a separate sweep — a sleeping step is just a `ready` row with a future `run_after`, so it re-enters through the ordinary `claimNextStep` query, and 0001's `step_claim_idx` (`(priority desc, run_after) where status = 'ready'`) already covers it exactly: the index is ordered by `run_after` within a priority, so `run_after <= now()` is a range scan over the leading edge and still-sleeping rows are simply never visited. An index on `sleeping_until` would be pure write amplification for a column only humans and tests read.

**One new index, for the cancellation sweep:**

```sql
create index run_cancel_requested_idx on run (cancel_requested_at)
  where cancel_requested_at is not null;
```

The textbook case for a partial index: pending cancellations are a vanishingly small fraction of the run table in any healthy system, so indexing only the rows that can ever match keeps it tiny, costs almost nothing to maintain (no entry is written for the overwhelming majority of runs, which are never cancelled), and turns a full scan of every run ever executed into a lookup proportional to the number of in-flight cancellations.

---

## 10. What landed, file by file

**New:**
- `src/engine/sleep.ts` — pure: `SleepSignal` (branded control-flow signal), `isSleepSignal`, `parseDuration` (`ms/s/m/h/d/w`, compound forms, bare-number ms). No I/O.
- `src/engine/timeout.ts` — pure: `StepTimeoutError`, `isStepTimeoutError`, `withTimeout` (budget race + combined parent signal, reject-before-abort, no leaked timers). No I/O.
- `src/control/cancel.ts` — `cancelRun` (+ `CancelResult`), `isRunCancelled`, `sweepCancelledRuns`. The `control/` directory's first real occupant.
- `src/store/migrations/0003_execution_control.sql` — `run.cancel_requested_at`, `step.sleep_seq`, `step.sleeping_until`, `run_cancel_requested_idx`.
- `tests/sleep.test.ts`, `tests/timeout.test.ts` — pure unit coverage, no Postgres.
- `tests/cancellation.test.ts` — API-level semantics of `control/cancel.ts` against real Postgres, seeding step rows directly so every requested/finalized/pending combination can be pinned down.
- `tests/execution-control.test.ts` + `tests/fixtures/execution-control-workflows.ts` — the Phase 3 ships proof (§11).
- `examples/sleeping-workflow.ts` — runnable: a three-step workflow whose middle step sleeps `2s`, with an observer that prints the sleeping row while the worker is free.

**Changed:**
- `src/store/repositories.ts` — `sleepStep`, `clearSleepMarker`, `getSleepingSteps`, `requestRunCancellation`, `isCancellationRequested`, `finalizeCancelledRun`, `cancelRunningStep`, `getCancelRequestedRuns`; `RunRow`/`StepRow` gain the new columns. All SQL still behind the storage boundary.
- `src/define/context.ts` — `ctx.sleep` is real (replay against `sleepSeq`), `ctx.signal` added (timeout ∪ cancellation, defaulting to a never-aborted signal so callers never null-check). `waitForEvent` still throws, now citing Phase 5. All new `createWorkflowContext` fields are optional, so Phase 1/2 call sites are unchanged.
- `src/worker/worker.ts` — the `Attempt` union and its precedence, `commitSleep`, `commitCancellation`, the two cancellation checkpoints, `withTimeout` around the step call, `clearSleepMarker` on wake.
- `src/index.ts` — exports the sleep/timeout/cancel surface and adds `Orquestra#cancel`.

---

## 11. What ships, and how it's proven

`tests/execution-control.test.ts` is the ships proof, against real Postgres, asserting on rows and history rather than mocks — a sleep that only exists in a worker's memory is a `setTimeout`, not a durable sleep. Every wait is condition-polled against a generous deadline, so a loaded machine makes it slower rather than flaky.

**#9 sleep** — a single worker with **`concurrency: 1`**, which is what makes "the worker was released" a real claim rather than a coincidence of a spare slot:

- The parked row: `status === 'ready'` (not held as `running`), `run_after` in the future, `sleep_seq === 1`, `lease_owner === null`, `lease_expires_at === null`, and `worker.inFlight === 0`.
- **`attempt === 0` with `max_attempts === 1`** — the sleep handed back the attempt claiming consumed. On a `maxAttempts: 1` step, a burnt attempt would strand it forever, so this single assertion is the whole §2 refund argument.
- Mid-sleep, an unrelated run is enqueued and **completes on that same single-slot worker**, proven durably: `quickRun.finished_at < sleeperRun.finished_at`, for a run enqueued *after* the sleep began.
- On wake: the body ran **twice** (`bodyRuns === 2`) and the post-sleep half ran **once** (`afterSleepRuns === 1`) — re-execution from the top, with the served sleep resolving immediately instead of suspending again. Final row: `completed`, `sleep_seq === 1`, `sleeping_until === null`, `attempt === 1`. History contains `step.sleeping` and `step.completed` and contains **neither** `step.retry_scheduled` nor `step.failed`.

**#10 timeouts** — a step that hangs past a 150ms budget on its first attempt and returns promptly on the second, with a lease TTL comfortably longer than the budget so the *timeout* is what ends the attempt, not an expiring lease. Result: run `completed`, `starts === 2`, `abortsObserved === 1` (the abandoned attempt was told to stop, not just left dangling), exactly one `step.timed_out` row carrying `attempt: 1` and the right `timeoutMs`, exactly one `step.retry_scheduled` whose `nextRunAfter` is strictly later than the timeout event — the ordinary backoff path, parked in the future. A second case uses the builder's default `maxAttempts: 1` so the first timeout is terminal: run `failed`, the step `failed`, its downstream step `cancelled`, and no `step.retry_scheduled` anywhere.

**#11 cancellation** — cancelled while the step is genuinely `running` under a known worker's lease. `requestRunCancellation` returns a run still `status: 'running'` with `cancel_requested_at` set: intent, not a kill. The run then reaches `cancelled` on its own; the `long` step is `cancelled` with a null lease, the pending `next` step is `cancelled` and never ran, and the decisive assertion is that the single `step.cancelled` history row is **stamped with the lease holder's `workerId`** and `phase: 'in-flight'` — the canceller recorded intent and the owning worker closed out its own step. No `step.failed`, no `run.failed`, no `step.retry_scheduled`.

The second cancellation case is the sleep interaction: cancel a run whose only step is **asleep**, so there is no worker to abort and no signal to fire. The step wakes, is claimed, and checkpoint #1 cancels it `pre-run` — proven by `bodyRuns` staying at **1** and `afterSleepRuns` at **0**.

`tests/cancellation.test.ts` covers the API's states directly: synchronous finalize when nothing is in flight, `pending: true` with the running step untouched when something is, idempotent double-cancel (the timestamp and `finished_at` do not move), a completed run neither resurrected nor mutated, `isRunCancelled` tracking the *request* rather than the terminal status, and `sweepCancelledRuns` finalizing an orphaned request, skipping a run with a live step, and no-opping on a second pass.

`tests/repositories.test.ts` pins the storage layer: both fenced paths reject a non-owning `workerId`, a sleeping step is unclaimable until its wake time, `sleep_seq` accumulates across successive sleeps, a step in retry backoff is **not** reported as sleeping, `finalizeCancelledRun` leaves a `running` step untouched, and a step definition's `timeoutMs` actually reaches `step.timeout_ms`.

### Verification

| Check | Result |
|---|---|
| `bunx tsc --noEmit` (strict) | ✅ zero errors |
| `bun test` | ✅ 102 pass / 0 fail, 1720 `expect()` calls, across 14 files (~5.5s) |
| `bun run examples/sleeping-workflow.ts` | ✅ run `completed`; `sendReminder` logs twice (re-run from the top), observer sees the step asleep with the worker free |
| Phase 2 regressions | ✅ still green in the same run — `workers-distributed` observed reclaim at 582ms against a 500ms TTL |

Tests run against the Dockerized Postgres 16 from Phase 0 (port 5433), each case scoped to its own random namespace and workflow name since the queue and the registry are global.

---

## 12. What Phase 3 deliberately does NOT do

No DAG dependencies beyond what Phase 1 already expresses, no fan-out/fan-in, no conditional branching, no child workflows (Phase 4). No `waitForEvent` — `ctx.waitForEvent` still throws, citing Phase 5; a sleep is a *time*-based suspension and nothing here can suspend on an external event. No triggers, cron or webhooks (Phase 5). No concurrency limits or rate limiting (Phase 6). No dead-letter queue, manual retry, or compensation (Phase 7) — a step whose retries are exhausted by timeouts still fails the whole run and cancels its siblings, same fail-fast shape as before. No observability UI (Phase 8), though `step.sleeping` / `step.timed_out` / `step.cancelled` history rows and `step.sleeping_until` are laid down for it.

Two honest limits inside the features that *did* ship:

- **Wake latency is bounded below by the worker poll interval.** A sleeping step becomes claimable at `run_after`, but nothing pushes it — some worker has to come around on its next poll cycle and find it due. With the default `ORQ_POLL_INTERVAL_MS` of 200ms that is fine; with a long poll interval and a busy queue, "wakes on schedule" means "wakes no earlier than scheduled, and within about one poll interval after." The build plan flags a pg-notify/next-wake nudge as the eventual fix; it isn't needed at this scale.
- **A timeout does not stop uncooperative work** (§4). The step row is marked and the worker is freed; a step that ignores `ctx.signal` keeps burning CPU in the worker process until it returns.

---

## Next: Phase 4 — Orchestration & DAGs
Steps run when their dependencies complete; branches fan out to run in parallel and fan back in; results steer conditional paths; a workflow can spawn and await a child workflow. This is where `depends_on` earns its keep. Features: #19 step dependencies / DAG, #15 fan-out, #16 fan-in, #17 conditional branching, #20 child workflows.
