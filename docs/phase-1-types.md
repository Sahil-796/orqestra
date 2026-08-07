# Phase 1 — type map

A reading companion to `docs/phase-1.md`. That doc explains *what the engine does*;
this one answers "what is this thing and where does it live" so you don't have to
jump-to-definition every third line.

## The one sentence

Phase 1 has **three data shapes** (`RunRow`, `StepRow`, `WorkflowRow` — rows in
Postgres), **one in-process shape** (`WorkflowHandle` — the DAG + the actual
functions), and **one glue type** (`Db`). Everything else is a helper built out of
those.

## Where each type is declared

| Type | File | What it is |
|---|---|---|
| `RunStatus`, `StepStatus` | `src/types.ts` | string unions, mirrored by SQL `CHECK` constraints |
| `StepDefinition`, `WorkflowDefinition` | `src/types.ts` | the **serializable** DAG (no functions) — persisted as `workflow.dag` jsonb |
| `Result<T>`, `SerializedError` | `src/types.ts` | jsonb-safe wrapper for step results/errors |
| `Db` | `src/store/client.ts` | alias for `postgres.Sql`. A connection **or** an open transaction |
| `WorkflowRow`, `RunRow`, `StepRow`, `HistoryRow`, `NewStep` | `src/store/repositories.ts` | 1:1 with the tables in `0001_init.sql` |
| `WorkflowHandle`, `StepFn`, `StepOptions`, `WorkflowBuilder` | `src/define/workflow.ts` | the in-process side: DAG + step functions |
| `WorkflowContext` | `src/define/context.ts` | what a step function receives |
| `StepLike` | `src/engine/scheduler.ts` | structural subset of `StepRow` so scheduler is DB-free |
| `StartRunOptions`, `RunResult`, `EnqueueRunResult`, `AdvanceResult` | `src/engine/executor.ts` | executor call/return shapes |
| `RetryPolicy` | `src/engine/retry.ts` | Phase 2 policy numbers (pure math, no I/O) |

Rule of thumb: **`snake_case` fields = a DB row** (`depends_on`, `run_id`,
`max_attempts`). **`camelCase` fields = an in-process object** (`dependsOn`,
`runId`, `maxAttempts`). If you see `step.depends_on` you're looking at a
`StepRow`; `step.dependsOn` is a `StepDefinition`. That naming split is the
fastest way to know which side of the storage boundary you're on.

## The two representations of "a step"

This is the thing that trips people up. A step exists **twice**:

```
StepDefinition          (src/types.ts)          StepRow           (repositories.ts)
  name                                            id, run_id, name
  dependsOn: string[]      ──materialized──▶      depends_on: string[]
  maxAttempts                 at run creation     status, attempt, max_attempts
  timeoutMs?                                      result, error
  priority                                        run_after, lease_owner, ...
```

- `StepDefinition` is the **template**: what the DAG says, stored once per
  workflow version in `workflow.dag`.
- `StepRow` is the **instance**: one row per step *per run*, carrying live state.

The conversion happens in exactly one place — `registerAndCreateRun` in
`src/engine/executor.ts`, which maps `handle.definition.steps` → `NewStep[]` and
sets the initial status (`ready` if no deps, else `pending`).

`NewStep` is just "a `StepRow` before it has an id" — the insert payload.

## The two representations of "a workflow"

Same pattern:

- `WorkflowDefinition` — pure data, serializable, goes into Postgres.
- `WorkflowHandle` — `{ name, definition, stepFns: Map<string, StepFn>, register() }`.
  It holds the **actual JavaScript functions**, which can never be persisted.

This split is *why* the engine is data-driven: the DB knows the shape of the DAG,
the process knows the code. `runStep` bridges them with
`handle.stepFns.get(step.name)` — a name lookup. That single line is the whole
reason step names must be unique and stable.

## `Db` — the type you'll see most

```ts
export type Db = Sql   // from the `postgres` package
```

Every repository function takes `sql: Db` as its first arg. The important part:
**`Db` is either a pool or a transaction**, and callers pass whichever they want:

```ts
await withTransaction(db, async (tx) => {
  await completeStep(tx, step.id, value)   // tx is also a Db
  await insertHistory(tx, { ... })
})
```

So when you read `advanceRun(sql, runId)` and wonder "is this in a transaction?" —
the answer is *it depends on the caller*, and the docblock on `advanceRun` says so
explicitly (`executeRun` passes a bare `db`; the worker passes its `tx`).

## Reading order for the Phase 1 code

Follow the data, not the files. Start at `src/engine/executor.ts` and read in this
order:

1. **`registerAndCreateRun`** — DAG → rows. After this, the DB fully describes the
   run and the process holds nothing important.
2. **`executeRun`** — the loop. Note it takes a `runId`, not state: everything it
   needs it re-reads. That's the crash-recovery property, visible in the signature.
3. **`runStep`** — one step. The key structural detail: `await fn(ctx)` happens
   *outside* any transaction; only the outcome write is atomic.
4. **`advanceRun`** — the "what's ready now" sweep, delegating the actual rule to
   `scheduler.ts`.
5. **`src/engine/scheduler.ts`** — pure functions, ~50 lines, no DB. Read it whole;
   this is where the DAG semantics actually live.
6. **`resumeAll`** — recovery across a process restart.

`retry.ts` is Phase 2 policy that landed early; it has no callers in the Phase 1
path. Skip it while reading Phase 1.

## The state machine, in one table

`step.status` transitions and who performs them:

| From | To | Where |
|---|---|---|
| — | `pending` / `ready` | `registerAndCreateRun` (deps or not) |
| `pending` | `ready` | `advanceRun` ← `newlyReadySteps` |
| `ready` | `running` | `markStepRunning` |
| `running` | `completed` | `completeStep` |
| `running` | `failed` | `failStep` |
| `running` | `ready` | `resetRunningSteps` — **crash recovery**: a `running` row with no live process was interrupted, not in progress |

And `run.status`: `queued` → `running` (top of `executeRun`) →
`completed` (in `advanceRun`, when `isRunComplete`) or `failed` (in `runStep`,
fail-fast on first step failure).

## Result / error codec — why it exists

`Error` objects don't survive `JSON.stringify` (message/stack are non-enumerable),
and step results land in `jsonb` columns. So:

- write path: `serializeError(e)` → `SerializedError` → `failStep`
- read path: `decodeResult<T>(step.result)` → `Result<T>` → `.ok ? .value : undefined`

You see the read path once, in `advanceRun`, building the run's `output` object as
`{ [stepName]: value }`.

## Cheatsheet: when you're lost

- Field is `snake_case` → it's a DB row → definition is in `repositories.ts`.
- Something named `*Row` → `repositories.ts`, mirrors a table in
  `src/store/migrations/0001_init.sql`.
- Something named `*Definition` → `src/types.ts`, serializable, no functions.
- A `sql`/`db`/`tx` parameter → always `Db` from `client.ts`.
- A `handle` parameter → always `WorkflowHandle` from `define/workflow.ts`.
- `ctx` → `WorkflowContext` from `define/context.ts`.
- Anything with no `Db` param at all → a pure module (`scheduler.ts`, `retry.ts`,
  the codecs in `types.ts`). These are the safe places to start reading.
