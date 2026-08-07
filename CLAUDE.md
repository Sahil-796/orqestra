# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What orqestra is

A durable, crash-proof workflow engine (the Temporal / Inngest idea) built lean on **Bun + TypeScript + Postgres**. Workflows run multi-step, survive process crashes, retry, sleep without holding a worker, and fan out across concurrent workers. It is being built **depth-first over 9 phases (0–8), 35 features** — the authoritative design is [`docs/build-plan.html`](docs/build-plan.html); per-phase implementation notes land in `docs/phase-N.md`. Read the build plan before making architectural decisions (But only when needed).

## Commands

```bash
bun run db:up          # start dockerized Postgres 16 (port 5433) — required for migrate/tests
bun run migrate        # apply pending SQL migrations (idempotent; safe to re-run)
bun test               # run all tests (bun:test) — needs Postgres up + migrated
bun test tests/repositories.test.ts   # run a single test file
bunx tsc --noEmit      # typecheck (strict mode) — treat zero errors as the bar
bun run db:down        # stop Postgres
```

Default DB: `postgres://orqestra:orqestra@localhost:5433/orqestra` (override via `DATABASE_URL`). Other env: `ORQ_POOL_SIZE`, `ORQ_LOG_LEVEL`, and the worker defaults `ORQ_LEASE_TTL_MS`, `ORQ_POLL_INTERVAL_MS`, `ORQ_WORKER_CONCURRENCY`. All parsed in `src/config.ts`, which fails fast on bad input.

## The storage boundary (do not cross it)

`engine/` (and everything outside `store/`) must know **nothing** about Postgres — it talks to storage only through typed functions in `src/store/repositories.ts`. Only `src/store/{client,migrate,repositories}.ts` may import the `postgres` package. This keeps correctness-critical logic unit-testable without a DB and leaves storage swappable. When adding queries, add a typed repository function; don't inline SQL elsewhere.

## Conventions

- Code style: 2-space indent, **no semicolons**, single quotes. Match surrounding files.
- TS is strict with `allowImportingTsExtensions` + `verbatimModuleSyntax` — **imports must include the `.ts` extension** (e.g. `from './client.ts'`) and use `import type` for type-only imports. `noUncheckedIndexedAccess` is on, so array/index access is `T | undefined` — handle it.
- Statuses are `text` + `CHECK` constraints (not native enums) so values can be added without `ALTER TYPE`; mirror the union types in `src/types.ts` (`RunStatus`, `StepStatus`).
- Results/errors persist as `jsonb`; use the `Result<T>` codec + `serializeError`/`deserializeError` in `src/types.ts` (raw `Error` objects are not JSON-safe).
- Migrations are append-only numbered `.sql` files in `src/store/migrations/`, applied in one transaction each by the custom runner in `migrate.ts`.
- Directories under `src/` (`engine/`, `queue/`, `worker/`, `triggers/`, `control/`) are `.gitkeep` placeholders reserved for their named phase — put new code in the folder that owns that concern rather than growing `store/` or `define/`.
- For explainations and stuff use simple yet technical enough language

## Phase discipline

Build one thin end-to-end slice per phase; don't pull features forward. 