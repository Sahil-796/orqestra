# Phase 7 — Failure handling

> Status: **done & verified** · Branch: `phase-7-failure-handling`
> Ships (per build plan): *runs that exhaust their retries land in a dead-letter queue instead of vanishing; operators manually retry from there; per-workflow policies decide fail-fast vs continue-on-error; and compensation steps unwind side effects (refund the charge if provisioning fails).* ✅

Through Phase 6 a failing run had exactly one ending: its step blew its retry budget, the run flipped to `failed`, and that was it — the failure was recorded but *inert*. Nothing caught it, nothing could re-drive it, and a workflow that had already charged a card before a later step failed had no way to give the money back. Phase 7 is about what happens when work breaks *for real*. Four features land: **#26 dead-letter queue, #27 manual retry, #28 failure policies, #29 compensation / rollback.**

The organizing idea: `failed` was a dead end; Phase 7 replaces it with a **parking lot** (`dead_letter`) an operator can inspect and re-drive, a **policy knob** that decides whether one bad step sinks the whole run, and a **saga** that runs registered rollbacks in reverse when a run dies.

No observability dashboard yet (Phase 8) — this phase builds the *mechanics* of failure handling; surfacing them in a UI (and driving #27 from a button) is Phase 8's job.

---

## 1. Two new terminal statuses, and why `failed` is now (almost) vestigial

Phase 7 adds two run statuses to the `RunStatus` union + the `run_status_check` CHECK (migration 0008), mirrored in `src/types.ts`:

- **`dead_letter`** — a run that exhausted its retry budget under the default `fail_fast` policy. The parked form of the old bare `failed`.
- **`completed_with_errors`** — a run that finished under `continue_on_error` with at least one failed step. A *policy-chosen success*: it ran to the end, just not cleanly.

The consequence worth stating plainly: after Phase 7, a `fail_fast` run that exhausts its retries **no longer ends `failed` — it ends `dead_letter`**, on every execution path (inline, worker, and the lease-reclaim poison-pill path). `failed` survives as a run status only for paths that don't route through the terminal-failure decision; the tests that used to assert `run.status === 'failed'` on retry exhaustion were updated to `dead_letter` because the behavior genuinely changed, not because the test was wrong before.

Both new statuses are **terminal**, which matters for child workflows — see §6.

---

## 2. The storage contract (migration 0008)

One additive migration lays down everything the engine and control layers build on. Per the storage-boundary rule, `engine/`, `worker/`, and `control/` never touch this SQL directly — they go through typed functions in `src/store/repositories.ts`.

- **Run statuses** — `dead_letter` and `completed_with_errors` added to `run_status_check`.
- **Dead-letter metadata on `run`** — `dead_lettered_at timestamptz`, `dead_letter_reason text`, and a partial index `run_dead_letter_idx (… where status = 'dead_letter')` so listing the DLQ stays cheap as the run table grows.
- **`failure_policy text` on `run`** — nullable, default `'fail_fast'`, with a `run_failure_policy_check` CHECK. The authoritative policy lives on the workflow *definition* (code); this column is a persisted-for-observability copy the executor stamps at run start.
- **`compensation` table** — `(id, run_id, step_id, step_name, status ('executed'|'failed'), result jsonb, error jsonb, created_at)`, with `UNIQUE (run_id, step_name)`. This is the durability + idempotency ledger for the saga (§5).

Repository surface (the contract three feature layers code against):

| Function | Purpose |
| --- | --- |
| `deadLetterRun(sql, runId, reason)` | Transition an exhausted/`failed`/`running` run to `dead_letter` with metadata. Matches `status in ('queued','running','failed')`. |
| `listDeadLetterRuns` / `getDeadLetterRun` | The operator DLQ read side. |
| `resetRunForRetry(sql, runId)` | Re-drive a dead-lettered run (§4). |
| `setRunFailurePolicy` / `getRunFailurePolicy` | Persist/read the run's policy (null coerced to `fail_fast`). |
| `recordCompensation` / `getExecutedCompensations` / `hasCompensationRun` | The saga idempotency ledger (§5). |

---

## 3. #26 Dead-letter queue — one decision, three call sites

The heart of Phase 7 is a single question asked at exactly one moment: *a step has exhausted its retries — now what happens to the run?* The answer is policy-dependent (§4), and under the default `fail_fast` it is "route the run to the DLQ." That decision had to be wired at every place a run can reach terminal failure — and there are three:

1. **The inline driver** (`executeRun`, `src/engine/executor.ts`). The terminal-failure decision was deliberately lifted *out* of `runStep` and up into `executeRun`: `runStep` now returns a `StepOutcome` and owns only step-level persistence + the retry loop, while `executeRun` owns the run-level, policy-dependent transition. On `fail_fast` exhaustion it runs compensations, then calls the shared `deadLetterRunAndWake` helper.
2. **The durable worker path** (`commitOutcome`, `src/worker/worker.ts`). This is the primary execution path — how every enqueued run and every child run actually fails. It used to hard-code `updateRunStatus(tx, runId, 'failed', …)`; it now makes the *same* policy decision as the inline path, sharing `deadLetterRunAndWake` / `finalizeWithErrors` from `executor.ts` so the two paths can never drift on what "route to the DLQ" means. (Wiring this was a late catch — the inline path alone would have left #26/#28 dark for real, worker-driven runs.)
3. **The lease-reclaim poison-pill path** (`reclaimExpiredLeases`, `src/queue/lease.ts`). A step whose lease keeps expiring without ever completing (the crash-loop case) hits the poison-pill ceiling. That path already *named* its outcome `deadLettered`, but had been marking the run `failed`; it now genuinely routes through `deadLetterRun`, so a crash-looping run is visible in the DLQ like any other exhausted run.

`deadLetterRunAndWake` is the shared write: flip the run to `dead_letter`, log a `run.dead_lettered` history event, and wake any parent step blocked on this run as a child (§6). It takes a `sql` handle that may be a bare `db` (inline) or an open transaction (the worker/reclaim paths, committing the failing step's outcome and the dead-letter transition atomically). It deliberately does **not** cancel the run's still-pending steps — the inline driver has no concurrent claimers so it needn't, and the worker calls `cancelPendingSteps` itself right after (its siblings can be claimed by other workers, so stopping them is worker-path-specific).

---

## 4. #28 Failure policies — and #27 Manual retry

### The policy

A workflow opts in via `defineWorkflow(name, builder, { failurePolicy })`, defaulting to `'fail_fast'`. It's surfaced as `handle.failurePolicy` (always concrete) and read from the in-memory registered definition at execution time — authoritative even after a crash, since definitions are re-registered in-process. The two behaviors, applied identically on the inline and worker paths:

- **`fail_fast`** (default) — the first terminal step failure ends the run: run compensations (§5), route to the DLQ, cancel the rest. This is the classic "one step died, the run is done" semantics, just parked instead of failed.
- **`continue_on_error`** — a terminally-failed step is marked `failed` but does **not** stop the run. Steps that don't depend on it keep progressing (a failed step never satisfies a dependent, so its dependents stay `pending` forever — that's correct, not a leak). When no runnable work remains and at least one step failed, the run finalizes as `completed_with_errors` — no rollback, no DLQ. `finalizeWithErrors` builds the output exactly like a clean completion, except failed steps contribute `undefined` (same as a skipped step).

On the worker path, "no runnable work remains" is detected by `finalizeIfDrainedWithErrors`: a run under this policy is finished only once no step is `ready`/`running`/`blocked` **and** no `pending` step is still satisfiable (all deps completed/skipped). Because each step commits independently on the worker, this check runs after *both* a successful commit (which may have drained the last runnable work) and a failed one.

### Manual retry (#27)

`retryDeadLetterRun(db, runId)` (`src/control/retry.ts`) is the operator action. It validates the run is actually in `dead_letter` (via `getDeadLetterRun`); if not, it returns `{ retried: false, reason: 'not_dead_letter' }` and mutates nothing. On a match it calls `resetRunForRetry`, which transitions the run back to `queued` (the status the claim path requires), clears the dead-letter metadata + `finished_at`/`started_at`, revives `failed` steps → `ready` (attempts reset, error/lease cleared), and preserves already-`completed` steps. A worker's normal claim loop then picks it up and re-runs only the unfinished work.

Exposed three ways: the control function, `Orquestra.retry(runId)` (mirroring `.cancel()`), and operator HTTP routes — `GET /dead-letter` (list, `?limit=`) and `POST /dead-letter/:id/retry` — composed into the server handler ahead of the existing trigger routes. The load-bearing test drives a *real* `createWorker` loop: dead-letter a run, retry it, and assert the revived step actually re-executes to completion.

---

## 5. #29 Compensation / rollback — the saga

A step registers a rollback from inside its body: `ctx.compensate(() => refund(chargeId))`. Registrations accumulate per-context in call order; after a step commits successfully, `runStep` captures them via `getCompensations(ctx)` and `executeRun` accumulates them in completion order. On a `fail_fast` terminal failure, `runCompensations` replays the completed steps' rollbacks in **reverse** order — the phase's shipping example: `charge → provision`; `provision` fails → the `charge`'s refund runs automatically.

### Exactly-once, durably

Rollbacks must never double-fire (refunding twice is worse than not at all). The `compensation` table is the ledger: `recordCompensation` does `INSERT … ON CONFLICT (run_id, step_name) DO NOTHING`, so the first caller gets `created: true` (perform the real side effect) and every replay gets `created: false` (skip). Combined with `hasCompensationRun`, a re-driven run never re-runs a compensation it already executed. This is the "did I already do this?" idempotency the build plan flagged as the hardest real-world property, made durable for the sequential drive/re-drive path.

### The honest limitation: worker-path compensation is not wired

This is the one place Phase 7 stops short, and it's a deliberate scoping call, not an oversight. `ctx.compensate(fn)` registers an **in-memory closure**. On the inline path that's fine — every step runs in one process, so the closures are all in hand when a later step fails. On the **worker path**, each step is driven in isolation on a possibly-different worker, and the worker does **not** replay the whole definition — so a completed step's compensation closure is simply not present in the process where a later step fails. You cannot re-run the completed step's body to re-collect the closure without also re-running its side effect.

So on the worker path, a `fail_fast` run still correctly dead-letters — it just doesn't auto-run rollbacks. Making the saga work durably across workers needs a **serializable compensation descriptor** (compensation-as-a-persisted-step the engine can enqueue and any worker can run), which is a genuine redesign of the `ctx.compensate` API rather than a bolt-on. It's left as a tracked follow-up. The `recordCompensation`/`hasCompensationRun` ledger already in place is exactly what that future durable saga will build on.

---

## 6. Child workflows: a dead-lettered child must wake its parent

Phase 4's child-await wakes a blocked parent step when the child run reaches a *terminal* status. That terminal set (`isTerminalRunStatus` in `src/engine/child.ts`, mirrored by the SQL guard in `resolveBlockedStepForChildRun`) predated Phase 7 and listed only `completed`/`failed`/`cancelled`. Left alone, a child that dead-lettered would never release its parent — **the parent would hang forever.** Since a failing child now ends `dead_letter` (not `failed`), this would have silently broken the main child-failure path.

The fix adds both new statuses to the terminal set and the SQL guard, and teaches `classifyChildRun` how each propagates:

- **`dead_letter`** → propagate as a **failure** (`{ ok: false, status: 'failed' }`). Before Phase 7 the same exhausted child ended `failed` and propagated as a failure; this preserves that.
- **`completed_with_errors`** → propagate as a **success** carrying the child's (possibly partial) output — the child chose continue-on-error deliberately, so the parent gets what it produced.

The two definitions of "terminal" (the TS set and the SQL `status in (…)`) are kept in sync by comment, since they must agree for the wake to fire.

---

## 7. Inline vs. worker: two drivers, one decision

Phase 7 touches the one place the two execution drivers most easily diverge — the failure path — so the guiding rule was **share the decision, not duplicate it**. `executeRun` (inline) and `commitOutcome` (worker) both read the same policy, and both call the same `deadLetterRunAndWake` / `finalizeWithErrors` helpers exported from `executor.ts`. The differences that remain are intrinsic to the path, not accidental:

| | Inline (`executeRun`) | Worker (`commitOutcome`) |
| --- | --- | --- |
| Concurrency | single process, one loop | N workers, one step per claim/tx |
| Cancel-the-rest on `fail_fast` | not needed (no other claimers) | `cancelPendingSteps` (siblings are claimable elsewhere) |
| `continue_on_error` finalize | after the loop drains | `finalizeIfDrainedWithErrors` after each commit |
| Compensation (#29) | ✅ closures in-process | ❌ closures not recoverable (§5) |

The worker path is the shipping-bar path: it's what proves #26/#28 survive real distribution and crashes. The inline path is the synchronous convenience driver (and where the saga fully works today).

---

## Test coverage

`bun test` is green across the phase (one pre-existing, unrelated cross-test timing flake in a Phase-5 schedule test that passes in isolation). The load-bearing proofs:

- `tests/failure-store.test.ts` — the repository contract directly: dead-letter transition, reset-for-retry round-trip, compensation idempotency (`ON CONFLICT` → `created` flips to false on replay).
- `tests/dead-letter.test.ts` / `tests/failure-policy.test.ts` / `tests/compensation.test.ts` — the **inline** path: exhaustion → `dead_letter`; `fail_fast` vs `continue_on_error` (→ `completed_with_errors`); and the saga proving the refund runs exactly once across a drive + re-drive.
- `tests/worker-failure-handling.test.ts` — the **worker** path via a real `createWorker` loop: a worker-driven run that exhausts its retries lands in `dead_letter` (not `failed`), and `continue_on_error` finishes as `completed_with_errors` with independent steps still run.
- `tests/manual-retry.test.ts` — #27 end to end: a retried run is reset and drained to `completed` by a real worker claim loop; a non-dead-lettered id returns `not_dead_letter` / 404 without mutation.
- `tests/child-workflows.test.ts` (updated) — a failed child now dead-letters and still wakes its parent, propagating as a `ChildWorkflowError`.
- `tests/queue.test.ts` / `tests/execution-control.test.ts` (updated) — the poison-pill and timeout-exhaustion paths now route to `dead_letter` with a `run.dead_lettered` history event.

---

## What's deferred (and why)

- **Durable worker-path compensation (#29).** The closure-based `ctx.compensate` is inline-only; the distributed saga needs a serializable compensation descriptor. Tracked follow-up — the idempotency ledger is already in place for it (§5).
- **A vestigial legacy `dead_letter` *table*** from `0001_init.sql` is left untouched — Phase 7 keys dead-letter state on the `run` row + status instead. No clash (a table named `dead_letter` vs a status value `'dead_letter'` live in different namespaces); dropping the unused table is a housekeeping migration for later, not a correctness concern.
- **The UI to drive all of this** (click a dead-lettered run, read its trace, hit Retry) is Phase 8.
