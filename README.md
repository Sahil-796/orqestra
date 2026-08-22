# orqestra

A durable, crash-proof workflow engine built on Bun, TypeScript and Postgres.

orqestra runs multi-step workflows that survive process crashes, retry themselves, sleep for
days without holding a worker, and fan out across a pool of concurrent workers — the
Temporal / Inngest idea, built lean enough to read in an afternoon.

Postgres is the single source of truth. Every durable fact — run state, step results, queue
rows, leases, timers — lives in Postgres and is mutated inside transactions. There is no
separate broker, no control plane, and no vendor. A worker is a process you start.

---

## Why Postgres is the whole engine

The design collapses into one table and one query. `step` is simultaneously the durable step
log and the work queue, and every worker claims work with:

```sql
SELECT * FROM step
WHERE status = 'ready' AND run_after <= now()
  AND (lease_expires_at IS NULL OR lease_expires_at < now())
ORDER BY priority DESC, run_after
FOR UPDATE SKIP LOCKED
LIMIT 1;
```

`FOR UPDATE SKIP LOCKED` is a correct, contention-free queue with no extra infrastructure.
Transactions give exactly-once step commits for free: claim, run, persist result in one
atomic unit. And crash recovery is just "read the rows" — state was never only in memory.

Queueing, concurrent workers, leasing, priorities, sleep, and retry backoff are all
predicates or columns on that one query rather than separate subsystems.

---

## Invariants

Five properties hold from the first phase. Everything else is built on top of them.

**Durable by default.** Nothing exists only in a worker's memory. A step is not "done" until
its result is committed to Postgres in the same transaction that advances the run.

**Determinism.** Workflow code is replayed on recovery. A completed step returns its
recorded result rather than re-running. The function is a plan; the log is the truth.

**Idempotency.** Every start carries an idempotency key and every step has a stable name.
Duplicate triggers and duplicate side effects collapse to one committed outcome.

**At-least-once delivery.** Workers can die mid-step, so a step can run more than once.
Idempotency plus result memoization turn that into effectively exactly-once. Design external
calls around an idempotency key.

**Leases, not locks.** A worker claims work with an expiring lease. If it crashes, the lease
expires and another worker reclaims the step. No job is ever permanently stuck behind a dead
worker.

---

## Requirements

- Bun 1.1+
- Postgres 16 (a Docker Compose file is included)

---

## Getting started

```bash
bun install
bun run db:up     # Postgres 16 on port 5433
bun run migrate   # apply migrations; idempotent
```

Define a workflow as a DAG of named steps:

```ts
import { defineWorkflow } from 'orqestra'

export const orderWorkflow = defineWorkflow('order-fulfillment', (wf) => {
  wf.step('validateOrder', async (ctx) => {
    const { itemCount } = ctx.input as { itemCount: number }
    if (itemCount <= 0) throw new Error('order must have at least one item')
    return { itemCount }
  })

  wf.step(
    'chargePayment',
    async (ctx) => {
      const { itemCount } = ctx.input as { itemCount: number }
      return { amount: itemCount * 25 }
    },
    { dependsOn: ['validateOrder'], maxAttempts: 3, timeoutMs: 10_000 }
  )

  wf.step('shipOrder', async () => ({ shipped: true }), {
    dependsOn: ['chargePayment'],
  })
})
```

### Two run modes, on purpose

**Inline** — executes the whole run in this process and resolves with its output. Intended
for tests and local development.

```ts
import { orquestra, startRun } from 'orqestra'

const orq = orquestra()
const result = await startRun(orq.db, orderWorkflow, {
  input: { itemCount: 3 },
  idempotencyKey: 'order-1042',
})
console.log(result.status, result.output)
```

**Durable** — writes the run and its ready steps to the queue and returns immediately.
Nothing executes on this call; workers pick the steps up.

```ts
import { orquestra, enqueueRun, createWorker } from 'orqestra'

const orq = orquestra()

const { runId } = await enqueueRun(orq.db, orderWorkflow, {
  input: { itemCount: 3 },
  idempotencyKey: 'order-1042',
})

const worker = createWorker({
  db: orq.db,
  handles: [orderWorkflow],
  concurrency: 4,
})
worker.start()
```

Both modes share one workflow definition and one execution path. The difference is who
drives the loop.

---

## Execution control

### Sleep

```ts
await ctx.sleep('24h')
```

Sleeping does not block. `ctx.sleep` throws a control-flow signal the worker catches, writes
the step back to the queue with a future `run_after`, and then goes looking for other work.
No worker slot, no database connection, and not even the process is held for the duration —
kill it and the step still wakes on schedule, because the wake time is a column.

A sleeping step re-runs **from the top** when it wakes; there is no saved stack. The engine
counts sleeps already served on the row, so an already-served `ctx.sleep` returns
immediately instead of suspending again. Write step bodies so re-running the part before a
sleep is harmless, and derive values that must survive a sleep from `ctx.input` rather than
from `ctx.now()` or `ctx.random()`.

Suspending is not failing: a sleep costs no retry attempt.

### Timeouts

```ts
wf.step('callVendor', fn, { timeoutMs: 30_000 })
```

A step that exceeds its timeout is aborted and recorded as failed. It is an ordinary failure
as far as retry and backoff are concerned — only the history label differs.

### Cancellation

```ts
const result = await orq.cancel(runId)
```

Cancellation is cooperative. If no step is currently running, the run is finalized to
`cancelled` immediately. If a step is in flight, the request is recorded and the worker that
holds the lease observes it on its next heartbeat and finalizes at a safe point — stomping a
running step from outside would race that worker's fenced commit and could produce two
outcomes for one step. The returned `CancelResult` distinguishes the two cases.

Steps receive `ctx.signal`, an `AbortSignal` fired on timeout or cancellation. Pass it to
`fetch` and other child work so an abandoned step stops burning resources.

### Retries

Failures retry with exponential backoff up to the step's `maxAttempts` (default 1). Backoff
is expressed as a future `run_after`, so a waiting retry occupies no worker.

---

## Orchestration

### Dependencies, fan-out and fan-in

```ts
const shards = builder.fanOut('shard', 10, async (i) => process(i), { dependsOn: ['seed'] })
builder.step('combine', combineFn, { dependsOn: shards })
```

A step becomes claimable exactly when every step it names in `dependsOn` has resolved. Fan-out
and fan-in need no special primitive: N steps naming the same dependency all become ready
together and N workers claim them in parallel, and a join step naming all N runs once, after
the last of them commits. The release is a single row update per completion, so ten workers
finishing ten siblings at the same instant release the join exactly once.

### Conditional branching

```ts
builder.step('decide', async (ctx) => {
  if (!needsReview) ctx.skip('review')
  return 'decided'
})
builder.step('review',  reviewFn,  { dependsOn: ['decide'] })
builder.step('publish', publishFn, { dependsOn: ['decide', 'review'] })
```

`ctx.skip` names sibling steps that should not run. They are recorded as `skipped` — a
terminal state, not an error — and a skipped dependency satisfies a downstream `dependsOn`
exactly like a completed one, so `publish` still runs. Without that rule, an untaken branch
would strand every join behind it forever.

A skip only takes effect if the deciding step itself commits: calling `ctx.skip` and then
throwing skips nothing. A step whose dependencies *all* resolved by being skipped is skipped
too, rather than run against no real input, so an untaken branch's whole downstream chain
resolves in one pass.

### Child workflows

```ts
const result = await runChildWorkflow(db, ctx, childWorkflow, { input })
```

A step can start another workflow and wait for its result. Waiting is durable, the same way
sleeping is: the parent step is written to `blocked`, its lease released and its worker freed,
and it is woken by the child's terminal transition — not by a timer, and not by the process
that spawned it. Kill that process and the parent still resumes when the child finishes.

A failed or cancelled child throws `ChildWorkflowError` in the parent, which fails the parent
step like any other error. Use `runChildWorkflowResult` to inspect the outcome instead of
throwing. Awaiting a child costs no retry attempt, and cancelling a parent cancels its blocked
step — though not, for now, the child run itself.

---

## Signals & triggers

Phase 5 gives a run ways to *wait for the outside world* and five ways to *be started by it*. The
engine stops being only a library you call and becomes a service that reacts.

### Waiting for an event

```ts
const payment = await ctx.waitForEvent('payment.confirmed')
```

`ctx.waitForEvent` suspends the step durably — exactly like sleep and child-await: the step is
written to `blocked`, its lease released and its worker freed, and it is woken only when a matching
event is published. The wait lives on the `step` row (`waiting_event_name` + an optional
correlation key), so a killed process still resumes when the event arrives. Delivery is
replay-safe and exactly-once: an `event_seq` counter and stored `event_payloads` mean a resumed
step returns the same payload it would have the first time, and a given publish wakes each blocked
step at most once. There is no backlog — a waiter is woken only by events published at or after its
wait began, and the throw→block race is closed against the step's own DB claim time, so no clock
skew can lose or double-deliver a signal.

Publish an event from anywhere with `publishSignal(db, { name, correlationKey?, payload? })`.

### Starting a run five ways

An HTTP server (`startServer()`, or `bun run src/server.ts`) is the front door:

| Way in            | How                                                                                  |
| ----------------- | ------------------------------------------------------------------------------------ |
| **API trigger**   | `POST /runs` (or `/workflows/:name/runs`) with a JSON `input` — starts a run now      |
| **Delayed start** | the same call with `runAt` (a timestamp) or `delayMs` — records a one-shot schedule   |
| **Webhook**       | `POST /webhooks/:name` — durably publishes an event, resuming any `waitForEvent`      |
| **Signal**        | `POST /signals` — a direct `publishSignal` over HTTP                                  |
| **Cron / event**  | declared on the workflow, fired by the trigger daemon (below)                         |

All the start and publish paths take an idempotency key (the `Idempotency-Key` header, or a
webhook delivery id) so a retried request collapses to one run or one event — the same
exactly-once guarantee the rest of the engine leans on.

Cron and internal-event triggers are declared on the definition:

```ts
defineWorkflow('nightly-rollup', build, { triggers: [{ type: 'cron', cron: '0 3 * * *' }] })
defineWorkflow('on-signup', build, { triggers: [{ type: 'event', event: 'user.created' }] })
```

The **trigger daemon** (`startTriggerRunner({ db, workflows })`) is a poll loop, not a hot loop —
between ticks it sleeps and releases, the same discipline as the worker. Each tick it claims due
schedules (`FOR UPDATE SKIP LOCKED`, with a guard bump so two daemons never double-fire) and starts
their runs — advancing a cron to its next occurrence, disabling a one-shot — and routes freshly
published events to the workflows that subscribe to them. Cron registration is idempotent across a
process *restart*, not just within one process, so a redeploy never duplicates a schedule.

---

## Configuration

Configuration is read from the environment by `src/config.ts`, which fails fast with a clear
error on malformed input.

| Variable                 | Default                                              | Meaning                                        |
| ------------------------ | ---------------------------------------------------- | ---------------------------------------------- |
| `DATABASE_URL`           | `postgres://orqestra:orqestra@localhost:5433/orqestra` | Postgres connection string                   |
| `ORQ_POOL_SIZE`          | `10`                                                 | Connection pool size                           |
| `ORQ_LOG_LEVEL`          | `info`                                               | `debug` · `info` · `warn` · `error`            |
| `ORQ_LEASE_TTL_MS`       | `30000`                                              | How long a claim holds before it is reclaimable |
| `ORQ_POLL_INTERVAL_MS`   | `200`                                                | Worker sleep after finding the queue empty      |
| `ORQ_WORKER_CONCURRENCY` | `1`                                                  | Max steps one worker runs at once               |
| `ORQ_HTTP_HOST`          | `0.0.0.0`                                             | Trigger server bind host                        |
| `ORQ_HTTP_PORT`          | `3000`                                               | Trigger server bind port                        |

Lease TTL is the tuning knob that matters: too short and healthy long steps get reclaimed
and double-run; too long and a crashed worker's job stalls. In-flight steps heartbeat to
extend their lease, which decouples the TTL from step duration.

---

## Architecture

```
src/
  define/          public API — defineWorkflow, WorkflowContext
  engine/          the durable brain — executor, scheduler, retry, sleep, timeout, dag, child
  queue/           claim (FOR UPDATE SKIP LOCKED) and lease
  worker/          the long-running process loop
  store/           Postgres only — every query lives here
    migrations/    append-only numbered SQL
    client.ts      the connection
    repositories.ts typed query functions
  control/         cancellation and child runs; concurrency, rate limits and priority later
  triggers/        api · cron · webhook · event
  observability/   structured logger, metrics
  types.ts         core types + Result codec
```

**The boundary that keeps correctness testable:** `engine/` knows nothing about Postgres. It
talks to storage only through typed functions in `store/repositories.ts`, and only
`store/{client,migrate,repositories}.ts` may import the `postgres` package. Correctness-
critical logic stays unit-testable without a database, and storage stays swappable.

### The data model

The core tables carry the whole engine.

| Table               | Purpose                                                  |
| ------------------- | -------------------------------------------------------- |
| `workflow`          | A registered definition and its version                   |
| `run`               | One execution of a workflow                               |
| `step`              | The queue and the durable step log, unified — plus what a blocked step is waiting for |
| `events`            | Published signals/events — an append-only log             |
| `schedules`         | Cron, delayed and one-shot run starts                     |
| `history`           | Append-only observability spine                           |
| `dead_letter`       | Runs that exhausted their retries                         |
| `schema_migrations` | Which migrations have been applied                        |

(The original `signal_wait` and `event` placeholder tables from migration 0001 are superseded —
a blocked step's event-wait now lives on `step` columns, and the durable event log is `events`.)

Several choices are cheap now and painful to retrofit, so they are in from the start:
`workflow.version`, so a run started on v1 finishes on v1's logic even after v2 deploys;
`run.namespace`, for per-tenant isolation of queues and quotas; statuses as `text` with
`CHECK` constraints rather than native enums, so new values need no `ALTER TYPE`; and
`jsonb` for every column whose shape varies.

---

## Examples

Runnable against a local Postgres:

```bash
bun run examples/durable.ts            # a DAG that fans out and back in
bun run examples/workers.ts            # 3 workers draining one queue, with a retry
bun run examples/sleeping-workflow.ts  # a step that sleeps and releases its worker
```

---

## Development

```bash
bun run db:up                        # start Postgres
bun run migrate                      # apply migrations
bun test                             # full suite against a real database
bun test tests/crash-recovery.test.ts # a single file
bunx tsc --noEmit                    # strict typecheck; zero errors is the bar
bun run db:down                      # stop Postgres
```

Crash-recovery and concurrency tests are first-class: the suite kills real worker processes
mid-step and asserts that leases are reclaimed and completed steps are never re-run.

---

## Status

orqestra is under active development and the public API is not stable.

Built depth-first over nine phases (0–8), 35 features; the authoritative plan is
[`docs/build-plan.html`](docs/build-plan.html), with per-phase notes in `docs/phase-N.md`.

**Phases 0–5 are done:** the Postgres foundation, durable execution with crash recovery, the
claim queue with expiring leases and concurrent workers, execution control (sleep, timeouts,
cancellation), orchestration (dependencies, fan-out/fan-in, conditional branching, child
workflows), and signals & triggers (`ctx.waitForEvent`, plus API, event, cron, delayed and
webhook starts).

**Next is Phase 6 — flow control at scale:** concurrency limits, rate limiting and priorities,
where Redis first earns a place. Phases 7–8 cover failure handling and observability.
