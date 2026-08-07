# Phase 0 — Foundation & the Pivot

> Status: **done & verified** · Branch: `phase-1-durable-execution`
> Ships (per build plan): *a migration that creates the schema, and a typed `defineWorkflow()` that registers a DAG.* ✅

Phase 0 doesn't add a single user-facing feature. It sets the ground everything else stands on: **where state lives, what the tables look like, and the folder boundaries** later phases must not cross. Getting this right now is what makes Phase 1–8 additions instead of rewrites.

---

## 1. The pivot: Redis → Postgres

The old skeleton kept workflow state in **Redis**. Phase 0 makes **Postgres the single source of truth**. Why this matters:

- **Transactions give exactly-once step commits for free.** Claim a step, run it, persist its result — all in one atomic unit. If the process dies mid-step, the transaction never committed, so there's no half-done state.
- **`FOR UPDATE SKIP LOCKED` is a correct, contention-free queue** with no extra infrastructure. Many workers can pull from the same `step` table without stepping on each other.
- **Crash recovery is just "read the rows."** State was never only in memory, so recovery needs no special replay-of-Redis logic.

Redis isn't gone forever — it comes back in **Phase 6** as an *optional accelerator* (rate-limit counters, pub/sub wakeups). The rule: a Redis flush must only ever cost latency, never lose work.

**Removed:** `src/core/redis.ts`, `src/core/workflow.ts`, `src/storage/workflow-storage.ts`, and the `ioredis` / `uuid` dependencies.
**Added:** the `postgres` (porsager) driver and a Dockerized Postgres 16.

---

## 2. The schema — `src/store/migrations/0001_init.sql`

The whole engine falls out of **eight tables**. This is the most important artifact in Phase 0.

| Table | What it is | Key columns |
|---|---|---|
| `workflow` | A registered definition **+ version** | `name`, `version`, `dag jsonb`, `unique(name, version)` |
| `run` | One execution of a workflow | `status`, `input/output jsonb`, `idempotency_key unique`, `namespace` |
| `step` | **The queue AND the durable step log, unified** | `status`, `attempt/max_attempts`, `result/error jsonb`, `depends_on text[]`, `run_after`, `lease_owner/lease_expires_at`, `priority` |
| `signal_wait` | What a run is blocked on (an external event) | `event_key`, `satisfied_at` |
| `event` | Events that have arrived | `key`, `payload jsonb` |
| `history` | Append-only observability spine (timeline + logs) | `run_id`, `step_id`, `type`, `data jsonb`, `at` |
| `dead_letter` | Runs that exhausted retries | `reason`, `snapshot jsonb` |
| `schema_migrations` | Which migrations have run | `version`, `applied_at` |

### The one query that is the whole engine
Every worker will run this (Phase 2) to grab work:

```sql
SELECT * FROM step
WHERE status = 'ready' AND run_after <= now()
  AND (lease_expires_at IS NULL OR lease_expires_at < now())
ORDER BY priority DESC, run_after
FOR UPDATE SKIP LOCKED LIMIT 1;
```

The `step` table's columns are shaped **specifically so that this one query** delivers the queue (#5), workers (#6), leasing (#7), priorities (#14), sleep/delay (#9 via `run_after`), and retry backoff (#4) — each is just a predicate or column on it. Phase 0 also pre-builds the partial index that makes it fast:

```sql
create index step_claim_idx on step (priority desc, run_after) where status = 'ready';
```

### Design choices baked in early (cheap now, painful later)
- **`namespace` on `run`** — multi-tenancy. A column now; impossible to bolt on cleanly once data is mixed.
- **`workflow.version`** — a run started on v1 must *finish* on v1's logic, even after v2 deploys. Long-running/sleeping workflows would break on every deploy otherwise.
- **Statuses as `text` + `CHECK`** rather than native Postgres `ENUM` — new statuses can be added without an `ALTER TYPE`.
- **`jsonb`** for every "shape varies" column (`dag`, `input`, `output`, `result`, `error`, `payload`, `data`, `snapshot`).
- **`gen_random_uuid()`** (via the `pgcrypto` extension) — the DB generates ids, so no client-side id library is needed.

---

## 3. The migration runner — `src/store/migrate.ts`

Deliberately lean, no framework. It:
1. Ensures a `schema_migrations` table exists.
2. Reads numbered `.sql` files from `migrations/`, sorted.
3. Skips any version already in `schema_migrations`.
4. Runs each remaining file **inside its own transaction**, then records its version — so a failed migration rolls back cleanly and is retried next run.

Running it twice is a **no-op** (verified). Invoked via `bun run migrate` or imported as `migrate(sql)` in tests.

---

## 4. The folder structure — boundaries that matter

```
src/
  define/          # PUBLIC API — defineWorkflow, WorkflowContext
  engine/          # the durable brain (Phase 1+): executor, scheduler, retry
  queue/           # claim / lease (Phase 2)
  worker/          # the long-running process loop (Phase 2)
  store/           # POSTGRES ONLY — every SQL query lives here
    migrations/
    client.ts        # the connection (only file that imports `postgres`)
    migrate.ts
    repositories.ts  # typed query functions
  triggers/        # api · cron · webhook · event (Phase 5)
  control/         # concurrency · rate-limit · priority (Phase 6)
  observability/   # logger, metrics
  types.ts         # core types + Result codec
  config.ts        # env loading
  index.ts         # public exports
tests/             # crash-recovery + concurrency tests are first-class
examples/          # hello.ts
```

**The rule that keeps correctness testable:** `engine/` knows *nothing* about Postgres. It talks to `store/` through typed functions in `repositories.ts`. Only `client.ts`, `migrate.ts`, and `repositories.ts` are allowed to import the `postgres` package. This keeps the correctness-critical logic unit-testable without a database and leaves the door open to swap storage later.

---

## 5. The building blocks Phase 0 delivers

- **`config.ts`** — typed loader for `DATABASE_URL`, `ORQ_POOL_SIZE`, `ORQ_LOG_LEVEL`. Fails fast with a clear error on bad input. Defaults to the local docker db.
- **`observability/logger.ts`** — small, dependency-free, leveled (debug/info/warn/error) structured JSON logger.
- **`types.ts` + the Result codec** — `Result<T>` is `{ok: true, value}` or `{ok: false, error}`. Because a step's result/error is stored in a `jsonb` column, and JS `Error` objects aren't JSON-safe (message/stack are non-enumerable), `serializeError` / `deserializeError` convert them to/from a plain `{name, message, stack, cause}` shape that survives a round-trip through the database. This matters the moment a step fails and we need to persist *why*.
- **`store/client.ts`** — the connection: `getDb()` (shared singleton) / `createDb()` (fresh, for tests & migrations) and a `withTransaction` helper.
- **`store/repositories.ts`** — typed query functions with real (but basic) inserts/selects: `insertWorkflow`, `getWorkflowByName`, `createRun`, `getRun`, `insertHistory`. **No execution logic yet** — that's Phase 1.

---

## 6. The public API — `defineWorkflow`

```ts
export const helloWorkflow = defineWorkflow('hello', (wf) => {
  wf.step('greet', async (ctx) => `hello, ${JSON.stringify(ctx.input)}`)
  wf.step('shout', async () => 'HELLO!', { dependsOn: ['greet'] })
})

const orq = orquestra()
await helloWorkflow.register(orq.db)   // upserts into `workflow`, versions on change
```

`defineWorkflow` separates **two things** on purpose:
- **The DAG definition** (`{name, version, steps: [...]}`) — pure data, serialized into `workflow.dag`. Carries *no functions*.
- **The step functions** — kept in-process, keyed by step name (`stepFns: Map`), so the Phase 1 executor can look them up when it replays a run.

This split is the foundation of **determinism**: the DAG in the database is the *plan*; the function is just an implementation the executor calls. `register()` compares the new DAG structurally against the latest stored version and only writes a new version if it actually changed.

### The `WorkflowContext` stubs
`context.ts` declares the object steps will receive — `input`, `runId`, and typed stubs `now()`, `random()`, `sleep()`, `waitForEvent()` that **throw "not implemented in Phase 0."** They exist now so the *shape* is fixed and steps are written against the deterministic API from day one:
- `now()` / `random()` — steps must use these instead of `Date.now()` / `Math.random()` so that **replay produces the same result** as the original run (determinism guard, filled in Phase 1+).
- `sleep()` — Phase 3. `waitForEvent()` — Phase 5.

---

## 7. Verification

| Check | Result |
|---|---|
| `bun install` | ✅ clean |
| `bunx tsc --noEmit` (strict) | ✅ zero errors |
| `bun run migrate` | ✅ applies `0001_init`; second run is a no-op |
| `bun test` | ✅ 7 pass / 0 fail (migration idempotency, `defineWorkflow` DAG + register, repository round-trip) |
| `bun run examples/hello.ts` | ✅ registers the `hello` workflow |

Tests run against a real Dockerized Postgres 16 (`docker compose up -d`, port 5433).

---

## 8. What Phase 0 deliberately does NOT do

No execution, no queue, no workers, no retries, no resumability. `ctx.sleep`/`waitForEvent` throw. Those are Phase 1+. Phase 0 is purely the foundation: **the pivot, the schema, the folder boundaries, and a typed `defineWorkflow` that registers a DAG.**

---

## Next: Phase 1 — Durable core (single process)
Execute a workflow as a sequence of steps, persisting every step's status and result, and prove you can `kill -9` the process mid-run and resume exactly where it stopped. Features: #1 durable definitions, #2 durable step execution, #3 crash recovery, #8 idempotency.
