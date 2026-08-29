# orqestra bench suite

Throughput and latency benchmarks for the real engine — every run goes
through `enqueueRun` and is drained by an actual `createWorker` pool talking
to Postgres, the same enqueue/claim/lease/commit path exercised by
`tests/workers-distributed.test.ts`. There is no toy in-memory engine here:
numbers reported by this suite are the engine's real behavior under load, not
a simulation of it.

## Running it

```bash
bun run db:up          # start dockerized Postgres (once)
bun run migrate        # apply migrations (idempotent)
bun run bench <scenario> [flags]
```

`<scenario>` is one of the names below. Flags tune the load a scenario runs
under:

```
--runs <n>              number of runs to enqueue
--workers <n>            size of the worker pool
--concurrency <n>        per-worker concurrent step slots
--steps <n>              steps per run (scenario-dependent meaning)
--width <n>              fan-out width (scenario-dependent meaning)
--fail <n>               how many steps/attempts are made to fail
--step-work-ms <n>       simulated per-step work duration
--lease-ttl <ms>         worker lease TTL
--poll-interval <ms>     worker poll interval
--sweep=workers=1,2,4,8  re-run the scenario once per value, sweeping one knob
--json                   emit machine-readable JSON instead of a text report
```

The exact flag parsing, defaults, and output shape are owned by
`bench/run.ts` — treat the list above as the vocabulary, not the final word
on behavior.

## Scenarios

- **chain** — a long linear sequence of dependent steps (depth via `--steps`).
- **fanout** — one step fanning out into many parallel siblings, then a join
  (width via `--width`).
- **retry** — steps that fail a controlled number of times (`--fail`) before
  succeeding, exercising the retry/backoff path.
- **contention** — many runs competing for a small worker pool, to observe
  claim contention and empty-poll behavior.
- **sleep-scale** — runs that suspend (`ctx.sleep`) and resume, checking that
  sleeping work doesn't pin a worker.
- **event-wake** — steps that block on an event and are woken by a publish,
  exercising the event-wait suspend/resume path.

## Metrics vocabulary

- **Throughput**: `runsPerSec` and `stepsPerSec` — wall-clock runs/steps
  completed per second across the whole worker pool.
- **Latency**: `p50`/`p95`/`p99`/`max`/`mean` (ms), computed from each run's
  durable `finished_at − created_at` — i.e. real time spent in the system,
  not an in-process stopwatch around a single call.
- **Scaling ratio**: how throughput moves as a knob (typically `--workers`)
  is swept — read from a `--sweep` run's series of results, not a single
  number.
- **Empty-poll ratio**: `(claimAttempts − claimsFound) / claimAttempts` —
  how often a worker's claim query came back empty. High values under low
  concurrency point at poll-interval tuning; high values under high
  concurrency point at claim contention.
- **Guardrail invariants**: `allTerminal` (every enqueued run reached a
  terminal status before the bounded timeout) and `unexpectedFailures`
  (runs that ended `failed`/`dead_letter`) — scenarios are designed to
  finish green, so a non-zero `unexpectedFailures` or `allTerminal: false`
  means something regressed, not that the benchmark is "measuring" failures.

## Caveats

- Numbers are only comparable **on the same machine, against the same
  Postgres instance, with the machine otherwise quiescent**. Don't compare
  a number from your laptop to one from CI, or a number taken while other
  processes were competing for CPU/disk.
- The dev Postgres is shared with the test suite and other bench runs, which
  adds noise (connection churn, autovacuum, concurrent namespaces). Run each
  scenario a few times and take the median rather than trusting a single
  run.
- Run the suite after phase 8 lands — it assumes the worker counters
  (`claimAttempts`/`claimsFound`) and observability plumbing phase 8 adds.
- Bench runs leave their namespaced rows behind in the dev database (they're
  isolated by a unique `bench-<scenario>-<uuid>` namespace per run, so they
  never collide with anything else, but nothing currently prunes them).
