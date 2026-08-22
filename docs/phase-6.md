# Phase 6 — Flow control at scale

> Status: **done & verified** · Branch: `phase-6-flow-control`
> Ships (per build plan): *at most N steps sharing a concurrency key run at once (e.g. "max 5 Stripe steps concurrently"); at most N steps sharing a rate key start per window (e.g. "≤100 calls/min"); higher-priority steps are claimed first, with aging so long-waiting normal-priority work is never starved.* ✅

Through Phase 5 the claim query answered one question: "is a step ready, due, and unleased?" Phase 6 adds two more gates a step can opt into — "is there room under my concurrency cap?" and "is there budget left in my rate window?" — plus a third axis, priority, that changes not *whether* a step is claimable but *in what order* claimable steps are picked. Three features land: **#12 concurrency limits, #13 rate limiting, #14 priorities + fairness.**

No dead-letter or compensation (Phase 7), no observability dashboard (Phase 8). This phase is about *keeping the queue honest under load*, not about what happens when work fails.

---

## 1. Everything is a column on `step`, enforced in one transaction

The shape decision that makes all three features cheap: each step's policy is persisted directly onto its own `step` row (`concurrency_key`/`concurrency_limit`, `rate_key`/`rate_limit`/`rate_window_ms`, plus the pre-existing `priority`), not looked up from the workflow DAG at claim time. `claimNextStep` (`src/store/repositories.ts`) stays self-contained — no join back to `workflow.dag` to learn a step's policy — and the un-keyed, unlimited, default-priority case (everything before Phase 6) is untouched: null concurrency/rate columns skip both gates entirely, and priority order with aging disabled collapses to the original `order by priority desc, run_after`.

All three gates run inside the *same* claim transaction that already does `SELECT ... FOR UPDATE SKIP LOCKED`. That matters: a step is claimed, gated, and flipped to `running` atomically, so nothing outside this one transaction can observe an inconsistent state (e.g. "claimed but over the concurrency limit").

### Why a plain `count(*) < limit` is not enough

The obvious first draft — filter candidates with a correlated `count(*) < limit` subquery — is wrong under concurrent claimers. Two workers racing the same key can each run that subquery, each see "4 running < 5" (neither has committed its own flip-to-`running` yet, and Postgres at READ COMMITTED doesn't let one see the other's in-flight write), and both proceed to claim — pushing the running count to 6. This is exactly the queue's classic double-claim race, just one level up (the base claim already solves it for "claim the same row twice" with `SKIP LOCKED`; this is "claim too many rows for one key").

**The fix, used identically for both concurrency and rate limiting:** `pg_advisory_xact_lock(hashtext(key))`. The first worker to reach a given key holds that lock until its transaction commits or rolls back; a second worker requesting the *same* key blocks until the first finishes, and because both run at READ COMMITTED, the post-lock read is a fresh snapshot that sees the first worker's committed change. Different keys, and the un-keyed fast path, never contend with each other or with this lock at all.

Concretely, in `claimNextStep`:

1. The candidate `SELECT` still applies a best-effort `count(*) < limit` filter, so a *saturated* key's steps are skipped in favor of other claimable work (otherwise a full high-priority key would be picked every tick and block everything behind it).
2. Once one candidate row is locked (`FOR UPDATE SKIP LOCKED LIMIT 1`), if it's keyed, the advisory lock is taken and the count/budget is **re-checked authoritatively**. Steps 1 is a hint; step 2 is the truth.
3. If the recount says the key/window is full, the claim returns "nothing this tick" for that gate — the step is not lost.

This is the load-bearing correctness proof in `tests/concurrency-limits.test.ts`, `tests/rate-limiting.test.ts`, and the combined `tests/flow-control-integration.test.ts`: flood a shared key with far more ready steps than the limit, race dozens of concurrent `claimStep` callers at it, and assert the running/started count never exceeds the declared limit.

---

## 2. #12 Concurrency limits

A step declares `{ concurrency: { key, limit } }`. `validateConcurrency` (`src/control/concurrency.ts`) enforces `key` non-empty and `limit` a positive integer at `defineWorkflow` time — a bad declaration fails loudly at definition, not silently at claim time. The DB mirrors this with a CHECK (`step_concurrency_coherent`, migration 0006): a step is either fully unlimited (both columns null) or fully declared (key + `limit >= 1`); a half-declared pair can never be written.

At claim time, a keyed candidate is only claimable while `count(running steps sharing this key) < limit`. **A concurrency-blocked step is left exactly `ready`** — nothing about its row changes, it's simply not picked this tick, and it's retried automatically on the next poll. This is deliberate: concurrency is about *how many run simultaneously*, not *when*, so there is no natural "try again at time X" to write, unlike rate limiting below.

Index support: `step_concurrency_running_idx`, a partial index on `(concurrency_key) where status = 'running' and concurrency_key is not null` — exactly the predicate the running-count subquery uses, so the count stays cheap as the step table grows.

---

## 3. #13 Rate limiting

A step declares `{ rateLimit: { key, limit, windowMs } }`. `validateRateLimit` (`src/control/ratelimit.ts`) mirrors the concurrency validation (key non-empty, `limit`/`windowMs` positive integers), and migration 0007's `step_rate_coherent` CHECK mirrors 0006's: all three columns null (unlimited), or all three present.

### Fixed-window model

Rate budget is tracked in a new table, `rate_window (rate_key, window_start, count)`, one row per `(key, window)`. Time is chopped into contiguous `window_ms`-wide buckets aligned to the epoch: `window_start = floor(now_ms / window_ms) * window_ms`. `src/control/ratelimit.ts`'s `windowStartMs`/`nextWindowStartMs` are the canonical TS-side reference for this math (unit-tested directly, no DB needed); the claim query computes the identical expression in SQL so the two never drift apart.

**Fixed-window over sliding-window on purpose:** a single counter row per `(key, window)` is enough to enforce "≤ N starts in this window" and needs no per-start timestamp history — cheap to write, cheap to query, no unbounded table growth. The tradeoff (a burst straddling a window boundary can start up to `2N` work in a short span around the seam) is accepted as the simple, honest cost of the simple model; a sliding-window or token-bucket refinement is a future concern, not a Phase 6 one.

### Claiming: count-and-consume, not read-then-check

Inside the same advisory-locked section concurrency uses, rate limiting does an atomic upsert:

```sql
insert into rate_window (rate_key, window_start, count)
values ($key, $window_start, 1)
on conflict (rate_key, window_start) do update
  set count = rate_window.count + 1
  where rate_window.count < $limit
returning count
```

The `where rate_window.count < $limit` on the conflict clause makes the whole check-and-increment atomic in one statement — it returns a row *iff* budget remained, and Postgres's own row lock during the upsert closes the same "two claimers both see budget" race the advisory lock already guards against (belt and suspenders: the advisory lock also serializes the surrounding logic for the key).

**If the window is exhausted, the step is DEFERRED, not left `ready`.** Its `run_after` is pushed forward to the next window boundary (`nextWindowStartMs`), so a polling worker doesn't hot-spin re-checking a step whose key has no budget until the window rolls over. This is the one place concurrency and rate limiting diverge in behavior: concurrency-blocked steps stay `ready` (no natural retry time), rate-exhausted steps get a concrete `run_after` (the window boundary *is* the natural retry time). Either way the step is never failed or lost.

Old `rate_window` rows are harmless to leave behind — a key's next window is a new row with a larger `window_start`. Vacuuming stale windows is a future operational concern, not a correctness one.

---

## 4. #14 Priorities + fairness

`priority` already existed as a plain `order by priority desc` tiebreak before Phase 6. The new piece is **aging**, which fixes the trap a naive priority order falls into: a continuous flood of fresh high-priority work can starve a low-priority step forever, because the low-priority step's rank never improves just by waiting.

The fix, in `src/control/priority.ts` (pure, DB-free, directly unit-tested):

```
ageSeconds    = now - step.run_after   (how long the step has been ready)
ageBoost      = min(maxBoost, max(0, ageSeconds * ratePerSec))
effective     = priority + ageBoost
```

The claim query orders by this `effective` value descending (tie-broken by `run_after`, the original order), computing the identical expression in SQL:

```sql
order by
  (s.priority + least($maxBoost, greatest(0, extract(epoch from (now() - s.run_after)) * $rate))) desc,
  s.run_after
```

Two config knobs tune it, both read once at process start via `src/config.ts`:

| Env var | Default | Meaning |
| --- | --- | --- |
| `ORQ_PRIORITY_AGE_RATE_PER_SEC` | `1` | Priority points added per second a step has been ready. `0` disables aging entirely. |
| `ORQ_PRIORITY_AGE_MAX_BOOST` | `100` | Cap on the boost, so aging lifts a step into contention but never to unbounded priority. |

With the default rate of 1 and cap of 100: a step at the default priority (0) that's been waiting ~100 seconds reaches the effective priority of a *fresh* priority-100 step — ordinary work is never starved indefinitely, while a step claimed the moment it becomes ready (age ≈ 0) sorts by base priority exactly as before Phase 6. Setting the rate to 0 collapses the formula back to `priority + 0`, i.e. the pre-Phase-6 `order by priority desc, run_after` — the aging-off default is a genuine no-op, not an approximation of one.

This can't be indexed (`now()` isn't immutable), but it doesn't need to be: `step_claim_idx` (from migration 0001) still narrows the candidate scan to `status = 'ready'` before the effective-priority sort runs over that already-small set, and `FOR UPDATE SKIP LOCKED LIMIT 1` stops the query the moment a row is picked.

`queue/claim.ts` (`claimStep`) is where the config defaults get injected — `repositories.ts` stays a pure mechanism that ages by whatever numbers it's handed, so tests can override the rate/cap directly without touching env vars.

---

## 5. Schema (migrations 0006, 0007)

Both are additive `alter table step add column ...` plus a coherence `CHECK`; neither touches an existing column or index.

**0006 (`#12`, `#14`):**
- `step.concurrency_key text`, `step.concurrency_limit integer` — null/null (unlimited) or both present.
- `step_concurrency_coherent` CHECK enforcing that pairing.
- `step_concurrency_running_idx` — partial index on `(concurrency_key) where status = 'running' and concurrency_key is not null`, matching the running-count subquery.
- No schema for #14 — priority aging is a pure ordering change over the existing `priority`/`run_after` columns, documented in the migration file for the record.

**0007 (`#13`):**
- `step.rate_key text`, `step.rate_limit integer`, `step.rate_window_ms integer` — all null (unlimited) or all present.
- `step_rate_coherent` CHECK enforcing that triple.
- `rate_window (rate_key, window_start, count)`, primary key `(rate_key, window_start)` — the fixed-window budget counters.

---

## 6. Postgres-first, Redis later

The build plan flags Phase 6 as roughly where a pure-Postgres design starts to strain, and calls out Redis as a likely future addition — this phase deliberately does **not** reach for it. Every gate here (concurrency count, rate window counter, priority order) is a column and a query on `step`, using the same `FOR UPDATE SKIP LOCKED` + advisory-lock toolkit the base queue already established. That keeps the "one table, one query, no extra infrastructure" property intact through Phase 6, at the cost of two things a Redis-backed limiter would avoid:

- **Advisory-lock contention is per-key, not per-row.** A single hot key serializes every claim attempt against that key through one lock, which is correct but means a very hot concurrency/rate key becomes a throughput ceiling under extreme load — acceptable at the scale this phase targets, not proven at, say, thousands of claims/sec against one key.
- **Rate limiting is fixed-window, not sliding or token-bucket.** As noted in §3, this is an accepted precision tradeoff, not a bug — a boundary-adjacent burst can briefly exceed the intended rate by up to 2x.

Both are exactly the kind of scaling wall a Redis-backed limiter (a `INCR` + `EXPIRE` counter, or a Lua-scripted token bucket, living outside Postgres transactions) is built to push past — cheaper contention under a hot key, and precise sliding-window or token-bucket semantics. The build plan's framing holds: Postgres is the right tool until a specific key's traffic outgrows what one advisory lock can serialize, and that migration is deliberately deferred rather than pre-built.

---

## Test coverage

`bun test` is green across the phase. The load-bearing proofs:

- `tests/concurrency-limits.test.ts` — the double-claim race closed by the advisory lock: a flood of same-key steps under many concurrent claimers never exceeds the limit; blocked steps stay `ready` and become claimable as running ones complete; un-keyed steps are unaffected by a saturated key.
- `tests/rate-limiting.test.ts` — the same race for rate budget: a flood never exceeds the per-window limit under concurrent claimers; exhausted steps are deferred (not failed) and become claimable again once the window rolls over; `windowStartMs`/`nextWindowStartMs` pinned against the fixed-window carve-up directly.
- `tests/priority-fairness.test.ts` — `ageBoost`/`effectivePriority` pinned as pure functions; higher priority claimed first when fresh; an aged low-priority step overtakes fresh high-priority work; aging-off (`rate = 0`) proven to collapse back to plain priority order.
- `tests/flow-control-integration.test.ts` — the Phase 6 integration slice: all three features exercised together under one flood (concurrency + rate + priority composing in a single claim race), plus the concurrency and rate invariants each re-proven under a fresh flood/many-claimer scenario matching the build plan's "max 5 Stripe steps concurrently" / "≤100 calls/min" language directly.
