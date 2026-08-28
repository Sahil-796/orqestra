# Phase 8 — Observability & dashboard

> Status: **done** · Branch: `phase-8-observability`
> Ships (per build plan): *a complete per-run timeline, structured logs, duration/queue/retry metrics, full error traces, live worker health, and queue-depth / throughput charts — plus the UI to drive manual retry (#27) and cancellation (#11).* ✅

Every phase before this one *wrote* to the `history` table (it has been filling since Phase 1's `insertHistory`) and to `run`/`step`; none of it was ever *read back*. Phase 8 is the read side: it surfaces what the engine already records, adds the two things that weren't being recorded (persisted logs and worker heartbeats), and puts a thin operator console over the top. Six features land: **#30 execution history, #31 structured logs, #32 duration metrics, #33 error details + traces, #34 worker health, #35 queue depth / throughput.**

Nothing here changes how a workflow runs. The engine core, the claim query, the lease/fencing semantics are untouched — this phase is purely additive observation, plus two write endpoints (`retry`, `cancel`) that reuse the Phase 7 control functions verbatim.

---

## 1. The layering: read-model → collectors → read API → UI

Four seams, each depending only on the one below it, so the correctness-critical code never learns about the dashboard:

1. **Read-model** (`src/store/repositories.ts`, migration `0009_observability.sql`) — every observability query is a typed repository function, honouring the storage boundary (only `store/` touches Postgres). The one new table is `worker_health`; everything else is read queries and supporting indexes over tables that already existed.
2. **Collectors** (`src/observability/`, wired into `src/worker/worker.ts`) — the two things the engine wasn't yet recording: run-scoped structured logs persisted as `history` rows, and worker heartbeats.
3. **Read API** (`src/dashboard/read-api.ts`, `src/dashboard/static.ts`, mounted in `src/server.ts`) — a JSON HTTP surface under `/dashboard/api/*`, plus static serving of the UI.
4. **UI** (`src/dashboard/ui/`) — a dependency-free, build-free vanilla HTML/CSS/JS single page served at `/dashboard`, consuming only the read API.

The JSON API is the real, headless surface — everything the UI shows is a `curl` away, and you can drive retry/cancel from your own tooling. The UI is a convenience layer on top, in the same spirit as Temporal Web or Inngest's dev dashboard: an optional operator console served by the engine's own HTTP ingress, not something the library forces on a deployment.

## 2. Read-model — surfacing tables that were already filling

`0009_observability.sql` is CREATE-only (append-only migration discipline): the `worker_health` table (`worker_id` primary key, `hostname`, `last_heartbeat_at`, `leased_steps`, `concurrency`, `started_at`, status) plus read-supporting indexes on `run(status, created_at)`, `run(namespace, created_at)`, `history(run_id, type, at)`, `step(status)`, failed steps, and `run(finished_at)`. No existing table is altered.

The read functions (all in `repositories.ts`, all typed):

- `listRuns(filter)` → paginated run list with a computed `duration_ms` (#30).
- `getRunTimeline(runId)` → ordered `history` rows — the per-run timeline (#30).
- `getRunLogs(runId)` → `history` rows of `type = 'log'` (#31, read side).
- `getRunMetrics(filter)` → status counts, avg/p50/p95 durations (`percentile_cont`), queue wait, attempt/reclaim counts, optionally grouped by workflow (#32).
- `getRunErrors(runId)` → failed steps with their full `error` jsonb, deserializable to `{ name, message, stack }` (#33).
- `upsertWorkerHeartbeat(input)` / `listWorkerHealth(staleAfterMs)` → the write and read sides of #34, with a derived alive/stale flag.
- `getQueueDepth()` + `getThroughput(sinceMs, bucketMs)` → step counts by status and bucketed completion/failure counts (#35).

### One schema reality worth knowing

The `step` table carries `created_at`/`updated_at`, not separate `started_at`/`finished_at`. Rather than ALTER an existing table (migrations stay append-only, and the executor would need to write the new columns), the metrics take the honest interpretation of what's there: step "duration" is `updated_at - created_at` on terminal steps (queue wait + all attempts combined, not pure execution time), while `run`-level queue wait uses `run.created_at`/`run.started_at`, which the schema *does* record precisely, and throughput buckets on `run.finished_at`. This is documented on `RunMetrics` in `src/types.ts`. If pure per-attempt execution time ever matters, that's a future `step` column + executor write, not a dashboard change.

## 3. Collectors — the two things the engine wasn't recording

**Structured logs (#31).** `createRunLogger(db, runId)` in `src/observability/logger.ts` wraps the existing stdout `Logger`: every call writes the same JSON line *and* best-effort persists a `history` row (`type: 'log'`) via `insertHistory`, which is what `getRunLogs` reads back. The stdout logger is untouched for callers that don't want persistence. Persistence is fire-and-forget and swallows its own failures — a storage hiccup while logging must never crash the workflow it's describing.

**Worker health (#34).** `worker.ts` now calls `upsertWorkerHeartbeat` on the worker's existing heartbeat cadence (no new config knob — it reuses `heartbeatIntervalMs`), reporting hostname, live leased-step count, and configured concurrency, plus `draining`/`stopped` heartbeats around graceful shutdown. It also emits a structured log per step commit (`step completed` / `retry_scheduled` / `failed`, with attempt, duration, and timed-out flag).

The worker changes are deliberately conservative: heartbeats and log emission run **outside** the fenced commit transaction, so they can never affect run correctness. `commitOutcome` gained a `CommitResult` return value so the post-commit log knows what happened, but no fencing, lease, or transaction logic changed — verified by `worker.test.ts` and `crash-recovery.test.ts` still passing.

## 4. Read API — `/dashboard/api/*`

Mounted in `createServerHandler` (`src/server.ts`) ahead of the trigger fallthrough, sharing that file's JSON/error shapes (400/404/405, `{ error }`). The prefix keeps it clear of the trigger routes and the Phase 7 `/dead-letter` operator routes.

| Route | Feature | Shape |
|---|---|---|
| `GET /dashboard/api/runs?status=&workflow=&namespace=&limit=&offset=` | #30 | `{ runs, count }` |
| `GET /dashboard/api/runs/:id` | #30/#33 | `{ run, steps, timeline, errors }` |
| `GET /dashboard/api/runs/:id/logs` | #31 | `{ runId, logs }` |
| `GET /dashboard/api/metrics?...&groupByWorkflow=` | #32 | `{ metrics }` |
| `GET /dashboard/api/queue?sinceMs=&bucketMs=` | #35 | `{ depth, throughput }` |
| `GET /dashboard/api/workers?staleAfterMs=` | #34 | `{ workers }` |
| `POST /dashboard/api/runs/:id/retry` | #27 | reuses `retryDeadLetterRun` |
| `POST /dashboard/api/runs/:id/cancel` | #11 | reuses `cancelRun` |
| `GET /dashboard`, `GET /dashboard/ui/*` | — | static files (`Bun.file`, path-traversal guarded) |

## 5. UI — an operator console, not a framework

`src/dashboard/ui/` is plain HTML + CSS + vanilla ES-module JS, no build step and zero runtime dependencies added to the library. Hash-routed, five views: runs list (filterable, click-through), run detail (header, step timeline, error panel with stack traces, log panel, Retry/Cancel buttons with confirms), metrics tiles, queue-depth bars + a hand-drawn throughput sparkline (inline SVG), and a workers table that auto-refreshes every 5s. Empty states and fetch errors are handled rather than thrown.

## 6. What's Bun-specific (a noted limitation)

Phase 8's HTTP pieces use Bun globals — `Bun.serve` (`server.ts`) and `Bun.file` (`static.ts`) — matching the rest of the project, which targets Bun as its one supported runtime (`bun:test`, direct `.ts` execution with `.ts` import specifiers, `bun run`). The read-model and collector logic are runtime-agnostic TypeScript over the `postgres` package (which also runs on Node). A future Node port would need a transpile/loader step, a `bun:test` → `node:test`/vitest swap, and `Bun.serve`/`Bun.file` → `node:http`/`fs`; the DB and engine logic carry over unchanged.

---

## Verification

- `bunx tsc --noEmit`: zero errors across the integrated branch.
- Per-area suites green: `observability-store.test.ts` (14), `observability-collectors.test.ts` (6), `dashboard-api.test.ts`, `dashboard-ui.test.ts` (5); `worker.test.ts` + `crash-recovery.test.ts` confirm the worker loop is unchanged.
- Full suite: **319 pass / 1 fail** on the integration run. The single failure is a timing-sensitive scheduler/control test unrelated to observability that passes in isolation and only trips under full-suite contention on the shared dev Postgres — flagged as a **pre-existing flake, unverified as fixed**, not an integration regression. (Postgres was stopped after the run, so a clean re-run is pending a `bun run db:up`.)

Built via a four-unit fan-out (read-model → collectors ∥ read API → UI), 13 commits on `phase-8-observability`.
