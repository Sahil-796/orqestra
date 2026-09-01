# Plan: swappable storage backend (Postgres + Redis)

Goal: `ORQ_STORAGE=pg|redis` chosen once at boot picks a complete backend. The
engine never learns which one it got. Postgres stays the reference
implementation; Redis is added behind the same interface.

## Guiding principle

Not "thin storage vs fat Lua." The cut is:

- **Policy → TypeScript** (pure functions, shared by both backends): retry
  decision, backoff math, failure-policy branching.
- **Structure → the atomic write** (transaction on PG, one Lua script on Redis),
  driven by **counters** so it stays O(1): dependent release, run finalization,
  drained-with-errors detection, cancellation check.

Postgres keeps its existing interactive transactions unchanged — it is strictly
stronger and already deadlock-hardened. Only Redis needs CAS + Lua.

## The safety rule (resolves "reading outside a tx")

Every input to the THINK phase must be exactly one of:
- **(a) guarded** — the write's fence rejects if it changed (`lease_owner`,
  `status`, `attempt`),
- **(b) immutable** — stale read still correct (`max_attempts`, DAG shape,
  per-run `failure_policy`),
- **(c) re-derived inside the write** — never decided in THINK.

Smuggling a (c) value into THINK as if guarded is the only real bug. DAG
finalization / dependent release / drain detection are all (c): they depend on
sibling rows a per-step fence cannot see. (Race they'd cause: two siblings of a
`continue_on_error` run fail concurrently, each sees the other `running`, both
conclude "not drained" → run hangs `running` forever. This is the race
`lockRun` at worker.ts:238-247 already prevents.)

## Fence discipline (prevents fan-in retry storms)

- **Step → full CAS** (owner + status + attempt). Rejection = "lost the step",
  discard (today's `'discarded'` path).
- **Run → predicate check, never version-CAS** (`status='running'`,
  `cancel_requested=false`). Version-CAS on the run makes N-way fan-in O(N²)
  retries. Counter decrements commute, so siblings stop conflicting.

---

## Stages

### Stage 1 — Make the seam real (Postgres only, no behavior change)
All 39 test files must still pass; zero new features.
1. Catalog the ~14 atomic operations from the `withTransaction` sites
   (worker.ts, executor.ts, lease.ts, control/child.ts). **Done** — see table below.
2. Hoist THINK into shared pure functions (`shouldRetry`, `nextRunAfter` already
   are) producing a **verdict** (`{action:'retry', runAfter}` |
   `{action:'fail_terminal', policy}` | ...).
3. Write `src/store/adapter.ts` — the interface at the **operation** level
   (`commitStepOutcome(snapshot, verdict) -> committed | fenced_out`), domain
   types only. No `Db`, no `sql`, no `withTransaction` in signatures.
4. Move today's code into `src/store/postgres/PgAdapter.ts` implementing it; the
   `withTransaction` blocks move out of engine/worker/control into these methods.
5. Inject the adapter; replace `sql: Db` threading + `withTransaction` imports in
   engine/worker/control with the injected `adapter`. `config.ts` gains
   `ORQ_STORAGE` (fail-fast, one boot-time gate).

### Stage 2 — In-memory adapter (the truth test)
`MemoryAdapter` (Maps + a mutex for atomic ops) implementing the same interface.
Run the whole engine suite against it. Green = the seam is honestly neutral, and
you get DB-less unit tests. If in-memory can't satisfy a method, Redis can't —
cheap early failure.

### Stage 3 — Counter refactor (both backends benefit)
Add aggregate keys/columns updated in the same atomic write as the step:
per-step `deps_remaining`, per-run `remaining_incomplete`, `failed_count`.
Dependent release = DECR-to-zero; drain detection = O(1). Port back to Postgres,
deleting its sibling scans. Build a **recount/audit repair** op (no CHECK
constraint exists on Redis to catch counter drift).

### Stage 4 — Redis adapter
`RedisAdapter` implementing the same interface. Day-one, unretrofittable
decisions:
- **Hash-tag every key of a run** (`{run:abc}:step:x`, `{run:abc}:pending`,
  `{run:abc}:history`) so one Lua script's keys share a cluster slot. Pins a run
  to one shard (caps a single run's throughput at one shard — fine for workflows).
- **Lease sweep** = ZSET scored by `lease_expires_at`, popped + re-fenced in a
  script. **Sleep/delay** = ZSET scored by wake time (mirrors `run_after`).
- Every mutation goes through a script (counters are load-bearing invariants).
- Durability tier decided explicitly (AOF `appendfsync always` vs weaker) — this
  is a real decision for a "crash-proof" engine, not a default.

### Stage 5 — Config gate + CI matrix
`makeAdapter()` switches on `ORQ_STORAGE`. CI runs the suite three times: memory,
pg, redis. Green on all three = done.

---

## Atomic-operation catalog (Stage 1 output)

Per-op backend treatment: **PG = keep existing transaction**; Redis column is the
plan.

| # | Operation | Site | Redis treatment |
|---|-----------|------|-----------------|
| 1 | commitStepOutcome | worker.ts:228 | hybrid: verdict-in + Lua (step CAS, run predicate, counter DAG) |
| 2 | commitSleep | worker.ts:444 | thin CAS |
| 3 | commitChildBlock | worker.ts:501 | thin CAS + counter |
| 4 | commitEventWait | worker.ts:587 | hybrid (block + backstop find) in one script |
| 5 | commitCancelRunningStep | worker.ts:650 | hybrid: step CAS + run finalize via counters |
| 6 | markRunStartedOnce | worker.ts:773 | thin CAS |
| 7 | materializeRun | executor.ts:142 | script (insert steps + counters + history) |
| 8 | finalizeRunStatus | executor.ts:204 | script (predicate + counters) |
| 9 | inlineCommitOutcome | executor.ts:469 | same as #1 (unify the two commit paths) |
| 10 | inlineCommitAdvance | executor.ts:500 | same as #1/#8 |
| 11 | inlineEventWait | executor.ts:538 | same as #4 |
| 12 | inlineFail | executor.ts:590 | same as #1 fail branch |
| 13 | reclaimOrPoison | lease.ts:88 | ZSET pop + re-fence script + counters |
| 14 | spawnChildRun | child.ts:137 | script (createRun + insertSteps + counters) |
| — | claimNextStep | repositories.ts:668 | Lua (BullMQ shape: candidate + advisory-equivalent recount + rate/concurrency counters) |
| — | ~30 simple reads/writes | — | plain Redis commands, no Lua |

Note: #9-12 (executor inline single-process path) re-implement #1/#4/#8. Confirm
behavioral equivalence, then route both through the same adapter methods so each
Lua script is written once.

## Recommendation

Do **Stage 1 + Stage 2** first and stop to evaluate. They pay for themselves
(proven seam + fast DB-less tests) regardless of whether Redis ships. Stage 3-4
(Redis) become their own phase once the interface is proven by two backends —
respecting the repo's phase discipline.
