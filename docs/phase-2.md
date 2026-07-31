# Phase 2 — Queue, workers & leasing

> Status: **done & verified** · Branch: `phase-1-durable-core`
> Ships (per build plan): *3 workers draining one queue; kill one mid-step and its lease is reclaimed within the lease TTL.* ✅

Phase 1 gave orqestra a durable, crash-recoverable single process: the `step` table was the state machine, and `executeRun` was both the first-run and the resume path. That was correctness in one process. Phase 2 is where orqestra **goes distributed** — the same step table becomes a claimable queue that any number of independent worker processes can pull from concurrently, with expiring leases so a crashed worker's work doesn't just vanish, and with retries + exponential backoff so a step that fails transiently self-heals instead of fail-fasting the whole run. Features delivered: **#5 run queue (Postgres), #6 concurrent workers, #7 leasing + reclaim, #4 retries + backoff.**

No sleep, no timeouts, no cancellation yet — that's Phase 3. Phase 2 is "N workers, one Postgres, no coordinator."

---

## 1. The core idea: the step log becomes a claimable queue

Phase 1's `step` table already held everything a queue needs — `status`, `priority`, `run_after`. Phase 2 adds two columns (`lease_owner`, `lease_expires_at` — actually already present in the 0001 schema, reserved for this phase) and one new one (`reclaim_count`, in 0002), and turns the "flip `ready` → run it" step of `executeRun`'s loop into a query any number of workers can race safely: `claimNextStep` in `src/store/repositories.ts`.

```sql
select s.* from step s
join run r on r.id = s.run_id
where s.status = 'ready'
  and s.run_after <= now()
  and (s.lease_expires_at is null or s.lease_expires_at < now())
  and r.status in ('queued', 'running')
order by s.priority desc, s.run_after
for update of s skip locked
limit 1
```
...immediately followed, in the **same transaction**, by an `update step set status = 'running', attempt = attempt + 1, lease_owner = $workerId, lease_expires_at = now() + $leaseTtlMs where id = $candidate.id`.

`FOR UPDATE ... SKIP LOCKED` is what makes double-claiming structurally impossible, not just unlikely: the `SELECT` locks the row it's about to return, and any *other* transaction running the same query concurrently simply **skips rows currently locked by someone else** instead of blocking on them or (worse) reading a stale, about-to-change value. Combined with running the select-then-flip-to-`running` as one transaction, a row is never visible as `ready` to a second worker between "worker A found it" and "worker A owns it" — there is no window. `tests/queue.test.ts` proves this directly: 15 workers race 10 ready steps concurrently, and the claimed-id set is exactly the 10 step ids, no duplicates, no misses.

The claim query's `WHERE` clause is also where retry backoff and lease expiry are enforced for free: `run_after <= now()` skips steps still in their backoff window (see §3), and `lease_expires_at is null or lease_expires_at < now()` skips steps someone else currently owns. No separate "is this claimable" check exists anywhere else — the query *is* the readiness+ownership gate.

`src/queue/claim.ts` is a thin, typed wrapper (`claimStep`) over `claimNextStep` — it exists purely so `worker.ts` depends on `queue/`, not on `repositories.ts` directly, keeping the storage boundary from CLAUDE.md intact (only `store/{client,migrate,repositories}.ts` import `postgres`).

---

## 2. The lease lifecycle: claim → heartbeat → commit-with-fencing → release

A lease is nothing exotic — it's just `lease_owner` + `lease_expires_at` on the step row, granted at claim time and extended by a heartbeat while the step function runs. The interesting part is what happens at the two ends.

**Claim** sets `lease_owner = workerId`, `lease_expires_at = now() + leaseTtlMs`. **Heartbeat** (`heartbeatStep`, called on a timer inside `worker.ts`'s `runClaimedStep`, every `leaseTtlMs / 3` by default) extends `lease_expires_at`, but *only* if the row is still `running` under that exact `workerId`:

```sql
update step set lease_expires_at = now() + $ttl
where id = $stepId and status = 'running' and lease_owner = $workerId
returning *
```
If this returns no row, the worker's lease is already gone — reclaimed by someone else while this worker was still (unknowingly) running the step. The worker's in-flight heartbeat loop notices (`heartbeatLease` returns `false`) and sets a local `abandoned` flag so it skips committing when the step function eventually returns.

**Commit-with-fencing** is the part that's actually load-bearing, not paranoia. The `abandoned` flag above is a *fast-path* optimization — it can never be the sole safety mechanism, because there's an unavoidable race between "the step function finishes" and "the next heartbeat tick notices the lease is gone." So every commit re-checks ownership **inside the transaction that's about to write the outcome**, via `lockStepIfOwner`:
```sql
select * from step
where id = $stepId and lease_owner = $workerId and status = 'running'
for update
```
If this returns nothing, `commitOutcome` writes **nothing** and returns — the step is someone else's problem now. Without this re-check, a "zombie" worker (one whose lease actually expired and was reclaimed, but which is still running old JS because it hadn't gotten a heartbeat tick yet) could commit a stale outcome on top of whatever the *new* owner already wrote: two `step.completed` history rows for one step, or a worker's success clobbering a poison-pill failure that already ran `cancelPendingSteps` on the rest of the run. The `for update` inside the same transaction as the write is what makes this a real compare-and-swap, not a check that a slower writer could still race past.

**Release** (`releaseLease`) just clears `lease_owner`/`lease_expires_at` — deliberately decoupled from `completeStep`/`failStep`/`retryStep` so it composes into whichever transaction already has the outcome, rather than a second round trip.

**When a worker dies**, its lease just sits there until it expires — nobody actively notices a crash, the TTL is the only signal. `reclaimExpiredLeases` (`src/queue/lease.ts`), run on a timer by every worker (`reclaimIntervalMs`, default 5s), scans `findExpiredLeases` (`status = 'running' and lease_expires_at < now()`, backed by the `step_lease_idx` partial index from 0002) and for each one either:
- sends it back to `ready` via `reclaimStep` (which **re-checks** `status = 'running' and lease_expires_at < now()` at write time — if a heartbeat rescued it in the gap between scan and write, this returns nothing and the step is left alone), bumping `reclaim_count`; or
- if `reclaim_count + 1` would exceed the **poison-pill ceiling** (`DEFAULT_MAX_RECLAIMS = 3`), fails the step and its run outright via `poisonStep` instead of reclaiming it again.

The ceiling exists because reclaim and retry are answering different questions. A step that *throws* is handled by `shouldRetry`/`maxAttempts` (§3) — that path never touches `reclaim_count`. A step whose **worker crashes outright** (segfault, OOM-killed, `kill -9`) never gets the chance to throw; the only signal is "the lease expired with no outcome." Nothing about `claimNextStep` consults `max_attempts` before re-claiming a reclaimed step, so without a separate ceiling a step that reliably kills whatever worker touches it (a "poison pill") would cycle forever, burning one worker's capacity every TTL. `reclaim_count` — incremented only by `reclaimStep`/`poisonStep`, never by ordinary retries — is what lets the ceiling apply independent of `max_attempts`.

### The proof — `tests/workers-distributed.test.ts`
Mirroring Phase 1's real-`kill -9` crash-recovery test: a worker is spawned as its own `bun` subprocess (`tests/fixtures/worker-hang.ts`) with a 500ms lease TTL, claims a single step, writes `"started\n"` to a signal file (the durable, bounded-poll proof it actually owns the lease), then hangs forever. The test polls for that signal, asserts the step row is `running` with `lease_owner` equal to the subprocess's worker id, then sends a real `SIGKILL` and asserts `exitCode === null && signalCode === 'SIGKILL'`. Only *then* does an in-process "surviving" worker — registered under the same workflow name but a different step implementation — start polling. Its own reclaim sweep (`reclaimIntervalMs: 50`) picks the expired lease back up once the 500ms TTL elapses, claims it, and completes the run. Measured across repeated runs: **elapsed between kill and run-`completed` was ~500–600ms** against a 500ms TTL — essentially "TTL plus one reclaim tick," nowhere near the `leaseTtlMs * 10` bound the test asserts. The completing step's own read of its `lease_owner` (written into the signal file as `resumed:surviving-worker`) is the direct proof the lease changed hands, not just that the run eventually finished somehow.

(The survivor is deliberately *not* started until the "started" signal is observed — starting it earlier lets it win the race for the step's very first claim before the subprocess even finishes booting, which would prove nothing about reclaim.)

---

## 3. Retries + backoff

`attempt` is incremented exactly once per real execution attempt, **at claim time**, inside `claimNextStep`'s own update (`attempt = attempt + 1`) — not when the step function starts, not when it fails. That's deliberate: claiming *is* committing to an attempt, whether the step function ever gets to run to completion or the worker dies first. `retryStep` (called when a step fails and is going back to `ready`) never touches `attempt` — the row picks up its next attempt number the next time `claimNextStep` claims it, not when the retry is scheduled.

The backoff policy (`src/engine/retry.ts`) is pure — no I/O, unit-tested without Postgres (`tests/retry.test.ts`):
```ts
ceiling = min(policy.maxMs, policy.baseMs * policy.factor ** (attempt - 1))
delay   = policy.jitter ? ceiling * (0.5 + Math.random() * 0.5) : ceiling
```
Full jitter in `[0.5, 1]` of the ceiling — always `> 0` when the ceiling is `> 0` (a zero-delay retry storm is exactly as bad as no backoff) and always `<= ceiling` (so `maxMs` stays a real ceiling even after jitter). `attempt` here is 1-based and means "attempts already used" — `backoffMs(1, ...)` is the delay before the *second* attempt, not the first (the first attempt never backs off, it just runs).

The decision lives in `worker.ts`'s `commitOutcome`: on failure, if `shouldRetry(owned.attempt, owned.max_attempts)`, compute `nextRunAfter` and call `retryStep(tx, step.id, error, runAfter)` — which sets `status = 'ready'`, clears the lease, and sets `run_after = runAfter`. That last part is the entire point: **a retry-scheduled step is `ready`, not `running`.** It sits in the queue exactly like any other not-yet-due step, filtered out of `claimNextStep`'s candidates by the same `run_after <= now()` clause every step is subject to. No worker is blocked waiting on it, no timer thread owns it, nothing is polling it in a busy loop — the backoff delay costs the system nothing but a due-time in a row until some worker's normal poll cycle finds it due again. `tests/workers-distributed.test.ts`'s retry case makes this observable directly: it samples the step row mid-flight and asserts it was seen `ready` with a `run_after` still in the future (not `running`, not claimed) — proof the delay actually released the worker instead of pinning it — and separately asserts the step's final `run_after` (set by the second `retryStep` call) is strictly later than its creation-time value.

---

## 4. Two run modes: `startRun` (inline) vs `enqueueRun` (durable)

Both now share `registerAndCreateRun` — register the workflow, idempotently create the `run` row, and (only on first creation) materialize step rows. Neither runs anything; that split is what makes the two public entry points trivial:

- **`startRun`** (Phase 1, unchanged in spirit): calls `registerAndCreateRun` then immediately `executeRun` — drives the run to completion or first failure **on the calling process** before returning. No queue involved; there is nothing for another worker to pick up because this call runs every step itself, synchronously, right here. Still the right choice for a script, a test, a single-process deployment, or anywhere the caller wants to block until the run is done.
- **`enqueueRun`** (new in Phase 2): calls `registerAndCreateRun` and returns **immediately** — `{ runId, created }` — without executing a single step. The step rows it just inserted (all `ready`, since a workflow's initial steps have no deps) *are* the queue from that point on; any worker polling this run's `namespace` picks them up. This is the mode that actually needs Phase 2's machinery: nothing about `enqueueRun` itself claims, leases, retries, or advances anything — that's entirely the worker's job.

Both exist because they answer different questions: "run this and tell me the result" vs. "this needs to happen, eventually, on whichever workers are up." `tests/worker.test.ts`'s first case pins down `enqueueRun`'s contract directly — the run is `queued`, its step is `ready`, and nothing has executed.

---

## 5. The step state machine, updated

Phase 1's diagram was single-process and had no way for a `running` step to become anything but `completed` or `failed` (a crash just reset it straight back to `ready`, since there was no leasing to reclaim from). Phase 2 adds claim/lease/retry/reclaim:

```
                     (deps completed)
        pending ─────────────────────────▶ ready
                                              │
                              claim (FOR UPDATE SKIP LOCKED,
                              attempt += 1, lease granted)
                                              │
                                              ▼
                                          running ── heartbeat extends
                                              │        lease_expires_at
              ┌───────────────┬──────────────┼───────────────────┐
              │ fn ok,         │ fn throws,    │ fn throws,        │ lease expires,
              │ fencing holds  │ retries left   │ retries exhausted │ nobody heartbeats
              ▼                ▼ (retryStep:    │ or unretryable    ▼
          completed        ready ◀── run_after  │                reclaim sweep:
                            (attempt unchanged;  ▼                reclaim_count += 1
                             next claim bumps    failed                │
                             it)                 ⇒ run failed,         │
                                                  cancelPendingSteps    │
                                                                        │
                                        reclaim_count ≤ ceiling ────────┤
                                                 │                      │
                                                 ▼                      │
                                              ready ◀────────────────────┘
                                       (lease cleared, run_after untouched
                                        — immediately claimable again)
                                                 │
                                  reclaim_count > ceiling (poison-pill)
                                                 ▼
                                              failed (poisoned)
                                       ⇒ run failed, cancelPendingSteps
```

Two structurally different ways a step returns to `ready` from `running`, easy to conflate but tracked by different counters with different due-time semantics:
- **Retry** (the step ran and threw): `attempt` was already bumped at claim time; `run_after` moves into the future per the backoff policy; `reclaim_count` is untouched.
- **Reclaim** (the step's worker never got the chance to report anything): `reclaim_count` bumps; `run_after` is left alone, so the step is immediately claimable the instant it's back to `ready` — no backoff, because nothing about the step itself is known to be broken, only that its previous worker vanished.

`cancelPendingSteps` (unchanged from what it was reserved for in Phase 1's schema) is what stops other workers from picking up the rest of a run that's already decided: once a step is `failed` (retries exhausted, or poisoned), every sibling still `pending`/`ready` in that run flips to `cancelled` in the same transaction — `running` steps are left for their own worker or the reclaim sweep to resolve, not yanked out from under someone.

---

## 6. What changed in `executeRun`, and why it's behavior-preserving

Two extractions, both already necessary once a second execution path (the worker) needed the same logic Phase 1's inline loop had:

- **`registerAndCreateRun`** — the "register workflow, idempotently create run, materialize steps" block that used to live only inside `startRun` is now a private helper both `startRun` and `enqueueRun` call. `startRun`'s observable behavior is unchanged: idempotent-key semantics, step materialization only on first creation, everything Phase 1's tests already pinned down still holds — the extraction just gives `enqueueRun` a way to stop *before* the "now execute it" step that used to be unconditional.
- **`advanceRun`** — the "flip newly-ready steps, finalize the run if everything's `completed`" logic that used to be inline in `executeRun`'s `while` loop is now a standalone function, called both by `executeRun` (with a bare, non-transactional `db` — same as Phase 1's original behavior, since a single process has nothing else to race) and by `worker.ts`'s `commitOutcome` (with the open transaction that just wrote a step's outcome, so the outcome and the resulting DAG advancement commit atomically). The readiness rule itself — "a `pending` step is ready once every named dep is `completed`" — was pulled further out into `src/engine/scheduler.ts` (`dependenciesSatisfied`, `newlyReadySteps`, `isRunComplete`, `isRunBlocked`), so there is exactly **one** definition of "ready" that both the inline executor and the worker consult, not two that could quietly drift apart.

**A concurrency bug this testing surfaced, and the fix:** `advanceRun`'s readiness check reads the run's steps and decides what's newly ready based on what it sees as `completed`. Under real worker concurrency, two sibling fan-in steps (e.g. both deps of a `finalize` step) can complete in **overlapping transactions** — and each transaction's read of the other's completion, if the other hasn't committed yet, sees the pre-completion value. Neither transaction then observes *both* deps satisfied, so neither flips `finalize` to `ready` — and since nothing else ever re-triggers the check for that step, it's stranded `pending` forever. `tests/workers-distributed.test.ts`'s "3 workers" case reproduced this directly (a 4-step diamond DAG, 10 concurrent runs) before the fix: some runs' `finalize` step simply never advanced.

The fix is `lockRun` (`src/store/repositories.ts`) — `select * from run where id = $runId for update` — acquired as the **first** run-referencing statement in the transaction that will write a step's outcome (`worker.ts`'s `commitOutcome`, right after the lease-ownership fencing check succeeds; `advanceRun` itself also takes the lock, for correctness when called directly). The naive version of this fix — locking only inside `advanceRun`, *after* `completeStep`/`insertHistory` had already run — actually made things worse: `insertHistory`'s `INSERT` (referencing `run_id` via foreign key) takes an implicit `FOR KEY SHARE` lock on the parent `run` row, and two transactions that each hold that weaker lock and then both request the stronger `FOR UPDATE` deadlock on each other (Postgres correctly detected and aborted one with `deadlock detected`). Taking the exclusive lock **first**, before any statement that would otherwise acquire the weaker one, avoids the upgrade entirely — the second transaction to reach that line just waits for the first to commit, and by the time it re-reads the steps, both completions are visible. This is behavior-preserving for the single-worker and no-contention cases (the lock is uncontended, essentially free) and is what actually makes `advanceRun` correct, not just usually-correct, under concurrent fan-in. See `src/engine/executor.ts`'s `advanceRun` doc comment and `src/worker/worker.ts`'s `commitOutcome` for the full reasoning in place.

---

## 7. Migration `0002_queue.sql` — additive, as required

Two changes, neither touching 0001:
- `alter table step add column reclaim_count integer not null default 0` — safe against existing rows (`not null default 0` needs no backfill).
- `create index step_lease_idx on step (lease_expires_at) where status = 'running'` — a second partial index alongside 0001's `step_claim_idx` (which only covers `status = 'ready'` and doesn't help the reclaim sweep's `status = 'running'` scan). No change to `step_claim_idx` itself: it already orders by `(priority desc, run_after)` under `status = 'ready'`, exactly what `claimNextStep`'s `ORDER BY` needs — the extra `run_after`/`lease_expires_at` predicates in that query's `WHERE` clause are cheap re-checks on the small row set the index already narrows to, and `FOR UPDATE SKIP LOCKED LIMIT 1` stops the scan at the first winning row regardless.

---

## 8. What landed, file by file

**New:**
- `src/queue/claim.ts` — `claimStep`, a typed, minimal wrapper over `repositories.claimNextStep` so `worker.ts` doesn't depend on `repositories.ts` directly.
- `src/queue/lease.ts` — `heartbeatLease`, `releaseLease`, `reclaimExpiredLeases` (the reclaim sweep + poison-pill ceiling).
- `src/engine/retry.ts` — pure backoff policy: `backoffMs`, `shouldRetry`, `nextRunAfter`, `DEFAULT_RETRY_POLICY`. No I/O.
- `src/engine/scheduler.ts` — the single definition of DAG readiness, extracted from Phase 1's inline loop: `dependenciesSatisfied`, `newlyReadySteps`, `isRunComplete`, `isRunBlocked`.
- `src/worker/worker.ts` — `createWorker`: the claim/run/commit loop, heartbeat timer, periodic reclaim sweep, graceful `stop()`.
- `src/store/migrations/0002_queue.sql` — `reclaim_count` column + `step_lease_idx`.
- `tests/queue.test.ts` — concurrent claim (no double-claim), heartbeat ownership/fencing, reclaim of an expired lease, the poison-pill ceiling end to end.
- `tests/worker.test.ts` — `enqueueRun` returns before executing anything; a single worker drains a multi-step run including a retry.
- `tests/retry.test.ts`, `tests/scheduler.test.ts` — unit coverage for the pure policy/readiness modules, no Postgres required.
- `tests/workers-distributed.test.ts` + `tests/fixtures/worker-hang.ts` — the Phase 2 "ships" proof: 3 real concurrent workers draining 10 runs of a 4-step DAG with no step double-run and work genuinely spread across workers; a real `kill -9` mid-step reclaimed within the lease TTL by a surviving worker; a step that fails twice and self-heals on the third attempt with `run_after` genuinely parked in the future between attempts.
- `examples/workers.ts` — runnable demo: 6 runs, 3 workers, one step that fails its first attempt so the retry path is visible in the output.

**Changed:**
- `src/store/repositories.ts` — added the queue/lease/retry query surface: `claimNextStep`, `heartbeatStep`, `releaseStep`, `findExpiredLeases`, `reclaimStep`, `poisonStep`, `lockStepIfOwner`, `retryStep`, `cancelPendingSteps`, `countStepsByStatus`, `markRunStarted`, and `lockRun` (added during this work — see §6). All still typed functions behind the storage boundary; no SQL leaked into `engine/`, `queue/`, or `worker/`.
- `src/engine/executor.ts` — `registerAndCreateRun` extracted (shared by `startRun`/`enqueueRun`); `enqueueRun` + `EnqueueRunResult` added; `advanceRun` extracted from `executeRun`'s loop (shared with `worker.ts`) and now locks the run row first (§6).
- `src/config.ts` — `ORQ_LEASE_TTL_MS`, `ORQ_POLL_INTERVAL_MS`, `ORQ_WORKER_CONCURRENCY` env vars, each fail-fast on malformed input like the rest of `loadConfig`.
- `src/index.ts` — exports `enqueueRun`, `advanceRun`, `createWorker`, and their types.

---

## 9. Verification

| Check | Result |
|---|---|
| `bunx tsc --noEmit` (strict) | ✅ zero errors |
| `bun test` | ✅ 44 pass / 0 fail across 10 files |
| `bun test tests/workers-distributed.test.ts` (run repeatedly) | ✅ 3/3 pass, consistently, ~1.1s total |
| Observed lease reclaim timing (500ms TTL) | ✅ ~500–600ms elapsed from `kill -9` to run `completed` (across 5+ runs) — bounded well under the `leaseTtlMs * 10` assertion |
| `bun run examples/workers.ts` | ✅ 6/6 runs complete; `transform` step shows `attempts: 2` on every run (fails once, retries, succeeds) |

Tests run against the Dockerized Postgres 16 from Phase 0 (port 5433), each test file/case scoped to its own random `namespace` since the queue is global.

---

## 10. What Phase 2 deliberately does NOT do

No `sleep`, no step timeouts, no cancellation (Phase 3) — a step that never returns just holds its lease until the TTL, there's no way to time it out early. No fan-out/fan-in beyond what a plain DAG already expresses, and no `waitForEvent` (Phase 5). No rate limiting or concurrency caps per external resource (Phase 6). No saga/compensation semantics (Phase 7) — a failed step still fails the whole run and cancels its siblings, same fail-fast shape as Phase 1, just now with retries in front of it. No observability UI (Phase 8) — the only place to see what happened is the `history` table and step rows directly. `reclaimExpiredLeases` is intentionally **global**, not namespace-scoped (unlike `claimNextStep`) — any worker's sweep can reclaim any other namespace's expired lease, which is correct (a lease is a lease regardless of who's asking) but means a busy shared dev Postgres can show reclaim activity that has nothing to do with the namespace you're currently testing; that's expected, not a bug.

---

## Next: Phase 3 — Execution control
Steps get the ability to sleep for hours without pinning a worker (just `run_after` pushed into the future — the same mechanism retries already use, now exposed as `ctx.sleep()`), exceed a timeout and be marked/killed, and whole runs or single executions can be cancelled cooperatively. Features: #9 sleep/delayed steps, #10 step timeouts, #11 cancellation.
