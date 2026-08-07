# Phase 1 — Durable core (single process)

> Status: **done & verified** · Branch: `phase-1-durable-core`
> Ships (per build plan): *a workflow that `kill -9`s halfway and resumes without re-running completed steps.* ✅

Phase 0 gave us a schema and a `defineWorkflow()` that only *registered* a DAG — nothing ran. Phase 1 is where the engine first **executes** a workflow, and does it durably: every step's status and result is committed to Postgres as it runs, so the process can be killed mid-run and resumed exactly where it stopped. Features delivered: **#1 workflow definitions (now executable), #2 durable step execution, #3 crash recovery, #8 idempotency.**

No queue, no workers, no leasing, no retries yet — that's Phase 2. Phase 1 is a single-process, **fail-fast** executor and the durable step log.

---

## 1. The core idea: the `step` table *is* the state machine

Unlike Temporal-style engines that replay workflow *code* on recovery, orqestra is **data-driven**. When a run starts, we materialize one `step` row per DAG step. Execution is then just a loop over those rows:

1. Flip any `pending` step whose dependencies are all `completed` → `ready`.
2. If every step is `completed` → finalize the run.
3. Run each `ready` step; persist its outcome; repeat.

There is no in-memory orchestration state to lose. **Recovery is not a special path — it's the same loop re-reading the same rows.** A restarted process reloads the step rows and continues; `completed` steps are simply skipped.

```
pending ──(deps completed)──▶ ready ──(claimed)──▶ running ──(fn ok)──▶ completed
                                                         └──(fn throws)──▶ failed ⇒ run failed (fail-fast)
```

---

## 2. Durable step execution (#2) — the atomicity boundary

The invariant from the build plan: *a step isn't "done" until its result is committed to Postgres.* The tricky part is that a step's function is arbitrary user code (possibly slow, possibly calling the outside world), so we **cannot** hold a transaction open across it. The resolution:

- **Run the user function *outside* any transaction.**
- **Persist the outcome inside *one* transaction:**
  - success → `completeStep(result)` + `insertHistory('step.completed')`
  - failure → `failStep(error)` + `insertHistory('step.failed')` + `updateRunStatus('failed')`

So the atomic unit is the *commit of the outcome*, not the work itself. If the process dies after the function runs but before that commit, the step never reached `completed` — on resume it simply **re-runs** (at-least-once delivery). A `completed` row is **never** re-executed; its recorded result is the truth. That memoization is what turns at-least-once into *effectively* exactly-once.

Step `result` is persisted as the encoded `Result<T>` codec (`{ ok, value }`) and `error` as a `serializeError` shape, both in `jsonb` — the same round-trip-safe encoding Phase 0 set up.

---

## 3. Crash recovery (#3) — `executeRun` is the recovery path

`executeRun(db, handle, runId)` is deliberately **the same function** for first execution and for resume. Two details make re-entry safe:

- **Terminal short-circuit:** if the run is already `completed`/`failed`/`cancelled`, it returns immediately.
- **Interrupted steps reset:** at entry, any step left in `running` is flipped back to `ready` (`resetRunningSteps`). In a single-process world with no leasing yet, a `running` step can only mean "a crash interrupted it" — so it's re-runnable.

Recovery across a whole process restart is `resumeAll(db, handles)`: it reads every run still `queued`/`running` via `getIncompleteRuns`, resolves each to a registered workflow handle by name, and calls `executeRun`. Runs whose workflow isn't registered in this process are skipped with a warning — a single-process deployment can only resume workflows it actually knows about.

### The proof — `tests/crash-recovery.test.ts`
This is the "ships" test, and it uses a **real `kill -9`**, not a simulated failure:

1. A fixture (`tests/fixtures/crash-workflow.ts`) is spawned as its **own `bun` subprocess**. Its workflow has step `a` (appends `"A"` to a file — a durable, count-once side effect) → step `b` (`dependsOn: ['a']`).
2. On this pass, step `b` calls `process.kill(process.pid, 'SIGKILL')` the instant it starts — *after* `a` has committed (the loop always finishes `a`, transaction and all, before `b` becomes `ready`) but *before* `b` writes or commits anything.
3. The test asserts the subprocess died by signal (`exitCode === null`, `signalCode === 'SIGKILL'`) and that the file contains exactly `"A\n"` — `a` committed, `b` did nothing.
4. It then **resumes in-process** via `resumeAll` (re-registering the same workflow so step fns are found) and asserts: the file has **exactly one** `A` (step `a` was memoized, *not* re-run) and **exactly one** `B` (step `b` completed), and both steps + the run are `completed`.

If recovery re-ran completed steps, the file would read `A / A / B`. It reads `A / B`. That's the whole phase in one assertion.

---

## 4. Idempotency (#8)

Two layers:

- **Idempotent starts.** `createRun` now uses `insert … on conflict (idempotency_key) do nothing returning *`, falling back to selecting the existing row on conflict, and returns `{ run, created }`. `startRun` only materializes steps when `created` is true. A duplicate `startRun` with the same key never creates a second run or a duplicate set of steps — if the original is still in flight it *resumes* it; if it already finished, it returns the recorded result.
- **Stable step identity + memoization.** Steps are keyed by name within a run, and a `completed` step's result is reused rather than recomputed. Duplicate triggers and duplicate execution both collapse to one committed outcome.

---

## 5. What landed, file by file

**New:**
- `src/engine/executor.ts` — the driver: `startRun`, `executeRun`, `resumeRun` (alias for `executeRun`), `resumeAll`, plus `StartRunOptions`/`RunResult`. Imports **no** `postgres` — it talks to storage only through `repositories.ts` and `withTransaction`, honoring the Phase 0 storage boundary.
- `tests/executor.test.ts` — fan-out/fan-in DAG completion, fail-fast (a downstream step never runs after an upstream failure), and idempotent double-`startRun`.
- `tests/crash-recovery.test.ts` + `tests/fixtures/crash-workflow.ts` — the real-`SIGKILL` recovery proof above.
- `examples/durable.ts` — a runnable durable workflow.

**Changed:**
- `src/store/repositories.ts` — added `NewStep` and the typed queries the engine needs: idempotent `createRun` (now `{ run, created }`), `insertSteps`, `getStepsByRun`, `markStepRunning`, `markStepReady`, `completeStep`, `failStep`, `updateRunStatus`, `getIncompleteRuns`, `resetRunningSteps`. All SQL stays behind this boundary.
- `src/define/context.ts` — `now()` → `new Date()` and `random()` → `Math.random()` are now real. `sleep`/`waitForEvent` still throw, now correctly citing Phase 3 / Phase 5.
- `src/index.ts` — exports the engine entrypoints.
- `tests/repositories.test.ts` — updated for `createRun`'s new return shape.

**No new migration** — the Phase 0 schema already had every column Phase 1 needed (`status`, `attempt`, `result`, `error`, `depends_on`, `run_after`, timestamps). Migrations stay append-only and untouched.

### The run's `output`
On completion, `run.output` is set to a plain record `{ [stepName]: resultValue }` of every step's value — deterministic and unambiguous, and enough to drive Phase 4 fan-in later.

---

## 6. A determinism note (deliberately deferred)

`ctx.now()` / `ctx.random()` are real implementations, not recorded-and-replayed. That's **correct** for Phase 1 because the memoization boundary is the *whole step*: a step either `completed` (its result is reused verbatim, so its internal `now()`/`random()` never matter again) or it re-runs from scratch (nothing committed to be inconsistent with). Recording these for cross-step deterministic replay is a future refinement, not a Phase 1 requirement given the data-driven model.

---

## 7. Verification

| Check | Result |
|---|---|
| `bunx tsc --noEmit` (strict) | ✅ zero errors |
| `bun test` | ✅ 11 pass / 0 fail across 5 files |
| `bun test tests/crash-recovery.test.ts` | ✅ real `SIGKILL`, resume re-runs nothing completed |
| `bun run examples/durable.ts` | ✅ runs to completion; re-run with same idempotency key returns same run |

Tests run against the Dockerized Postgres 16 from Phase 0 (port 5433).

---

## 8. What Phase 1 deliberately does NOT do

No queue, no concurrent workers, no leasing, no reclaim (Phase 2). No retries or exponential backoff — Phase 1 is fail-fast, one attempt per step (`max_attempts` exists in the schema but isn't consumed yet; retries are Phase 2). No `sleep`, no timeouts, no cancellation (Phase 3). Execution is in-process and synchronous — `startRun` drives the run to completion or first failure on the calling process.

---

## Next: Phase 2 — Queue, workers & leasing
Turn the durable step log into a claimable queue (`FOR UPDATE SKIP LOCKED`), run N worker processes that pull work with expiring leases, and add configurable retries with exponential backoff. Features: #5 run queue, #6 concurrent workers, #7 leasing + reclaim, #4 retries + backoff.
