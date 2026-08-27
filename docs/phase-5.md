# Phase 5 — Signals & triggers

> Status: **done & verified** · Branch: `phase-5-signals-triggers`
> Ships (per build plan): *runs pause until an external event arrives, and get started five ways — a direct API call, an internal event, a cron schedule, a future timestamp, or an inbound webhook.* ✅

Through Phase 4 a run was a closed system: you called `startRun`, and it advanced on its own until it finished. It could wait — on sleep, on a timeout, on a child — but only on things it started itself. Phase 5 opens two holes in that wall. A run can now **block on an event that hasn't happened yet** (`ctx.waitForEvent`), and a run can now **be started by the outside world** five different ways. The engine stops being a library you call and becomes a service that reacts.

Six features land: **#18 wait-for-event / signals, #21 API triggers, #22 event triggers, #23 cron / scheduled, #24 delayed starts, #25 webhook ingestion.**

No rate limiting or priorities (Phase 6), no dead-letter or compensation (Phase 7). This phase is about *ways in and ways to wait*.

---

## 1. `waitForEvent` is the same suspension you already trust

The hard part of Phase 5 is exactly one primitive; everything else is plumbing on top of it. And that primitive is not new — it is the `blocked` suspension Phase 3 (sleep) and Phase 4 (child-await) already established, pointed at a new wake condition.

When a step calls `ctx.waitForEvent(name)`:

- it durably parks in `status = 'blocked'`, recording **what it waits for** on the `step` row — `waiting_event_name` and an optional `waiting_event_correlation`;
- its lease is released and its worker is freed — a waiting run costs nothing but a row;
- it is woken **only** by a matching `publishEvent`, in the same transaction that persists the event.

So a killed process resumes when the event arrives, not before and not twice. The wake is a single UPDATE guarded by `status = 'blocked'`, which is what makes it **exactly-once**: two racing publishes cannot both move the same step out of `blocked`.

### Replay-safety, the sleep analogue

A resumed step re-executes its function from the top, so `waitForEvent` must be replay-safe the way `ctx.sleep` is. It uses the same trick, one field wider:

- `event_seq` counts how many event-waits a step has already satisfied (the `sleep_seq` analogue);
- `event_payloads` is a JSONB array holding the delivered payloads in seq order.

On replay, the Nth `waitForEvent` call sees `event_seq > N`, does **not** re-suspend, and returns `event_payloads[N - 1]` — the same value it returned the first time. Unlike sleep, an event carries data, which is why the payload has to be stored rather than just a completion flag.

### The throw→block race, closed against DB time

There is a window between "the step decided to wait" and "the wait is committed to the row". An event published inside that window must not be lost. The backstop is `findMatchingEventSince`, keyed on the step's **DB claim time** rather than any wall clock — after registering the wait, the step checks whether a matching event already landed since it began, and if so wakes itself immediately. Keying on the database's own clock means no process/DB skew can open the window back up. There is deliberately **no backlog**: a waiter is woken by events at or after its wait began, never by historical ones — signals are edges, not levels.

Both execution drivers carry the path. The worker loop is the shipping-bar path (it can publish from one process and wake a block in another). The inline executor durably suspends on a wait and treats `blocked` as *parked*, not *stuck* — but being single-process it can't self-publish mid-loop, so the cross-process wake is a worker property by construction.

---

## 2. Five ways in, two mechanisms underneath

The five triggers look distinct in the API but reduce to two durable mechanisms: **publish an event** or **create a schedule row**. Nothing starts a run by holding it in memory.

```
                        HTTP (src/server.ts, src/triggers/http.ts)
  POST /runs ─────────────────────────► startRunForWorkflowName        (#21 API)
  POST /runs {runAt|delayMs} ─────────► createSchedule kind='once'      (#24 delayed)
  POST /webhooks/:name ───────────────► publishSignal → publishEvent    (#25 webhook)
  POST /signals ──────────────────────► publishSignal                   (#18 publish)

                        trigger daemon (src/triggers/runner.ts)
  claimDueSchedules ──► start run, then reschedule/disable               (#23 cron, #24 fire)
  claimUndispatchedEvents ──► start subscribed workflows                 (#22 event)
```

### API triggers & delayed starts (#21, #24)

`POST /runs` (and the alias `POST /workflows/:name/runs`) starts a run immediately via `startRunForWorkflowName`, which materializes the run's steps from the stored DAG. If the body carries a future `runAt` or a positive `delayMs`, it instead writes a `once` schedule with that `next_run_at` and returns `202` — the run is *promised*, not started, and the daemon promotes it when it comes due. The request side and the firing side are cleanly split: the HTTP layer only ever writes a row.

### Webhook ingestion (#25)

`POST /webhooks/:name` maps an untrusted inbound request to a durable event (`mapWebhookToEvent`): the event name comes from the path (optionally qualified by a `type`/`event` body field), the correlation key and idempotency key from headers or body. It returns `2xx` only after the event is persisted — so a webhook that resumes a workflow blocked on `waitForEvent` is durable end to end. This is the phase's headline proof: *block on `ctx.waitForEvent("payment.confirmed")`, resume when a webhook posts it.*

### Event triggers (#22)

A workflow subscribes declaratively: `defineWorkflow(name, build, { triggers: [{ type: 'event', event: 'user.created' }] })`. The daemon claims freshly published events (`claimUndispatchedEvents`, which stamps `dispatched_at` under `FOR UPDATE SKIP LOCKED` so each event routes at most once) and starts a run of every subscribing workflow. The run's idempotency key is derived from `event id + workflow name`, so even a re-dispatch cannot double-start.

### Cron & scheduled (#23)

Cron lives entirely in `src/triggers/cron.ts` — a hand-rolled 5-field parser and `computeNextRun`, no dependency. The **repository layer does no cron maths**: `rescheduleCron` takes a pre-computed `next_run_at`. At daemon startup `syncCronSchedules` ensures each `{type:'cron'}` workflow has an enabled schedule row; on each tick `claimDueSchedules` hands the poller due rows and it fires them, then advances a cron to its next occurrence or disables a `once`.

---

## 3. The daemon is a poll loop with the worker's discipline

`startTriggerRunner({ db, workflows })` mirrors `createWorker`'s lifecycle exactly: it takes the workflow handles (there is no global "list all definitions" registry — the caller passes what it registered, the same pattern the worker uses for its handles), runs `syncCronSchedules` once, then polls on `ORQ_POLL_INTERVAL_MS`. It is **not a hot loop** — between ticks it sleeps and releases, and `stop()` shuts it down cleanly.

Double-firing is prevented at the database, not in the loop. `claimDueSchedules` bumps each claimed row's `next_run_at` forward by a guard interval in the same statement that claims it, under `FOR UPDATE SKIP LOCKED` — so a second daemon (or the same daemon's next tick, before this batch has been rescheduled) cannot re-claim the same rows. The guard bump is a safety net, not the cadence: a daemon that claims then crashes before rescheduling leaves the schedule due again a guard-interval later, so nothing is lost — it just fires late.

---

## 4. Integration notes (the wiring pass)

The three build units (storage+signals, HTTP ingress, trigger daemon) were assembled behind disjoint file ownership; the final wiring pass closed three seams:

- **Durable cron dedup across restarts.** `syncCronSchedules`'s original guard was an in-process `Set`, which a fresh process starts empty — so a redeploy would insert a *second* cron row and the schedule would fire twice. Closed with a durable `findCronSchedule` existence check before insert; `tests/trigger-runner.test.ts` proves it by resetting the in-memory guard (simulating a restart) and asserting no duplicate row.
- **HTTP config knobs.** The server's host/port moved into `src/config.ts` as `ORQ_HTTP_HOST` / `ORQ_HTTP_PORT`, with the same fail-fast parsing as every other knob, replacing ad-hoc `PORT`/`HOST` reads.
- **Public surface.** `src/index.ts` now exports `publishSignal`, `startRunByName`/`scheduleRun`, `startServer`, the trigger daemon and its pollers, and the cron helpers.

---

## 5. Known limitations (deliberately deferred)

- **No auth on any HTTP route.** Noted as a TODO in `http.ts`; a real deployment needs a signature check on webhooks and auth on `/runs`.
- **Delayed-start requests are not deduplicated.** `schedules` has no idempotency key, so a retried `POST /runs {delayMs}` creates a second `once` row. Deduping it needs a schema column and is left for when it bites — event routing and cron registration *are* idempotent, which is where double-firing actually hurts.
- **Cron times are process-local.** There is no per-schedule timezone; `computeNextRun` uses the process timezone. Fine for a single deployment region, a gap for multi-region.
- **The 0001 `event` / `signal_wait` placeholder tables are unused.** The append-only rule keeps them; the real mechanism is the `events` log plus `step` wait columns.

---

## Test coverage

`bun test` is green across the phase (221 tests). The load-bearing proofs:

- `tests/repositories-signals.test.ts` — `publishEvent` wake matching (name + correlation), exactly-once wake, schedule CRUD/claim.
- `tests/signals.test.ts` — end-to-end `waitForEvent`: a run blocks, releases its worker, resumes with the payload; a crash-mid-wait variant resumes exactly once.
- `tests/api-triggers.test.ts`, `tests/http-triggers.test.ts`, `tests/webhook.test.ts` — API start + idempotency, delayed-start → schedule row, and a webhook waking a real blocked worker.
- `tests/cron.test.ts` — `computeNextRun` across expressions and rollovers (pure, no DB).
- `tests/scheduled.test.ts`, `tests/event-trigger.test.ts`, `tests/trigger-runner.test.ts` — one-shot promotion without double-fire, cron reschedule, event routing without double-start, restart-safe cron dedup, and the daemon tick tying it together.
