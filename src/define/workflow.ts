// Public API: defineWorkflow(name, builderFn) — declare steps, get back a
// serializable DAG definition (the `dag` jsonb) plus a handle that can
// register that DAG into Postgres. Running the DAG is out of scope for
// Phase 0 (Phase 1 adds the executor).
//
// Phase 4 orchestration additions, both expressed without any new schema:
//   - fan-out (#15) / fan-in (#16): plain `dependsOn` — N steps depending on
//     the same name fan out; one step depending on all N names fans back in.
//     `fanOut()` below is sugar for generating the N steps; no new primitive.
//   - conditional branching (#17): a step's function calls `ctx.skip(...)`
//     (define/context.ts) naming sibling steps that are the untaken branch.
//     No new builder method — it's an ordinary `.step()` whose function
//     happens to call `ctx.skip`.

import type { Db } from '../store/client.ts'
import { getWorkflowByName, insertWorkflow, type WorkflowRow } from '../store/repositories.ts'
import type { ConcurrencyLimit, RateLimit, StepDefinition, WorkflowDefinition, WorkflowTrigger } from '../types.ts'
import { validateConcurrency } from '../control/concurrency.ts'
import { validateRateLimit } from '../control/ratelimit.ts'
import type { WorkflowContext } from './context.ts'

export type StepFn<T = unknown> = (ctx: WorkflowContext) => Promise<T>

export interface StepOptions {
  /** Names of steps that must complete before this one becomes ready. */
  dependsOn?: string[]
  maxAttempts?: number
  timeoutMs?: number
  priority?: number
  /**
   * Concurrency cap for this step (#12): at most `limit` steps sharing `key`
   * may be `running` at once, across every run. Omit for the unlimited common
   * case. `limit` must be a positive integer, or `defineWorkflow` throws.
   */
  concurrency?: ConcurrencyLimit
  /**
   * Rate cap for this step (#13): at most `limit` steps sharing `key` may
   * *start* per `windowMs` window, across every run. Omit for the unlimited
   * common case. `limit` and `windowMs` must be positive integers, or
   * `defineWorkflow` throws.
   */
  rateLimit?: RateLimit
}

export class WorkflowBuilder {
  private readonly steps: StepDefinition[] = []
  private readonly stepNames = new Set<string>()
  private readonly stepFns = new Map<string, StepFn>()

  step<T>(name: string, fn: StepFn<T>, options: StepOptions = {}): this {
    if (this.stepNames.has(name)) {
      throw new Error(`defineWorkflow: duplicate step name "${name}"`)
    }
    this.stepNames.add(name)
    this.stepFns.set(name, fn as StepFn)
    this.steps.push({
      name,
      dependsOn: options.dependsOn ?? [],
      maxAttempts: options.maxAttempts ?? 1,
      timeoutMs: options.timeoutMs,
      priority: options.priority ?? 0,
      concurrency: validateConcurrency(name, options.concurrency),
      rateLimit: validateRateLimit(name, options.rateLimit),
    })
    return this
  }

  /**
   * Feature #15 sugar: register `count` steps that share one function body
   * (parameterized by index), so a fan-out doesn't need `count` separate
   * `.step()` calls with hand-rolled names. Fan-out itself needs no new
   * schema or scheduling primitive — it falls out of ordinary `dependsOn`:
   * N steps naming the same `dependsOn` all become `ready` together when
   * that dependency resolves (see engine/dag.ts), and N workers can claim
   * and run them in parallel because `claimNextStep` (queue/claim.ts) is
   * already safe under concurrent claims.
   *
   * Returns the generated step names, in order — pass them as another
   * step's `dependsOn` to fan back in (#16):
   *
   * ```ts
   * builder.fanOut('shard', 10, (i, ctx) => processShard(i, ctx), { dependsOn: ['split'] })
   * builder.step('combine', combineFn, { dependsOn: builder.fanOut(...) }) // see below
   * ```
   *
   * (In practice, capture the returned array in a local rather than
   * inlining a second call — `fanOut` registers steps as a side effect
   * each time it's called.)
   */
  fanOut<T>(
    namePrefix: string,
    count: number,
    fn: (index: number, ctx: WorkflowContext) => Promise<T>,
    options: StepOptions = {}
  ): string[] {
    const names: string[] = []
    for (let i = 0; i < count; i++) {
      const name = `${namePrefix}-${i}`
      names.push(name)
      this.step(name, (ctx) => fn(i, ctx), options)
    }
    return names
  }

  /** @internal */
  buildSteps(): StepDefinition[] {
    return this.steps
  }

  /** @internal */
  buildStepFns(): Map<string, StepFn> {
    return this.stepFns
  }
}

/** Options for `defineWorkflow` beyond the step builder. */
export interface WorkflowOptions {
  /**
   * Declarative event/cron triggers (Phase 5). Data only — declaring a
   * trigger implements nothing here; the trigger daemon (Agent 3) reads them
   * (via `handle.triggers` / `getWorkflowTriggers`) and starts runs of this
   * workflow when a trigger fires. A workflow may carry several.
   */
  triggers?: WorkflowTrigger[]
}

export interface WorkflowHandle {
  readonly name: string
  readonly definition: WorkflowDefinition
  /** Step implementations, keyed by step name — for the Phase 1+ executor. */
  readonly stepFns: Map<string, StepFn>
  /** Declarative triggers this workflow was defined with (Phase 5). Empty if none. */
  readonly triggers: readonly WorkflowTrigger[]
  /**
   * Persist this DAG to Postgres. If an identical DAG is already the latest
   * registered version, this is a no-op; otherwise a new version is
   * inserted (workflow versioning per the build plan).
   */
  register(sql: Db): Promise<WorkflowRow>
}

const registry = new Map<string, WorkflowHandle>()

// jsonb round-trips don't preserve key order, so compare structurally
// rather than via JSON.stringify (which is order-sensitive).
function stepsEqual(a: StepDefinition, b: StepDefinition): boolean {
  return (
    a.name === b.name &&
    a.maxAttempts === b.maxAttempts &&
    a.timeoutMs === b.timeoutMs &&
    a.priority === b.priority &&
    (a.concurrency?.key ?? null) === (b.concurrency?.key ?? null) &&
    (a.concurrency?.limit ?? null) === (b.concurrency?.limit ?? null) &&
    (a.rateLimit?.key ?? null) === (b.rateLimit?.key ?? null) &&
    (a.rateLimit?.limit ?? null) === (b.rateLimit?.limit ?? null) &&
    (a.rateLimit?.windowMs ?? null) === (b.rateLimit?.windowMs ?? null) &&
    a.dependsOn.length === b.dependsOn.length &&
    a.dependsOn.every((dep, i) => dep === b.dependsOn[i])
  )
}

function dagsEqual(a: WorkflowDefinition, b: WorkflowDefinition): boolean {
  return (
    a.steps.length === b.steps.length && a.steps.every((step, i) => stepsEqual(step, b.steps[i]!))
  )
}

export function defineWorkflow(
  name: string,
  builderFn: (builder: WorkflowBuilder) => void,
  options: WorkflowOptions = {}
): WorkflowHandle {
  const builder = new WorkflowBuilder()
  builderFn(builder)

  const steps = builder.buildSteps()
  const stepFns = builder.buildStepFns()

  const definition: WorkflowDefinition = { name, version: 1, steps }
  const triggers: readonly WorkflowTrigger[] = options.triggers ?? []

  const handle: WorkflowHandle = {
    name,
    definition,
    stepFns,
    triggers,
    async register(sql: Db): Promise<WorkflowRow> {
      const latest = await getWorkflowByName(sql, name)
      if (latest && dagsEqual(latest.dag, definition)) {
        return latest
      }
      const version = latest ? latest.version + 1 : 1
      return insertWorkflow(sql, { name, version, dag: { ...definition, version } })
    },
  }

  registry.set(name, handle)
  return handle
}

/** Look up a workflow previously declared with defineWorkflow in this process. */
export function getRegisteredWorkflow(name: string): WorkflowHandle | undefined {
  return registry.get(name)
}

/**
 * The declarative triggers a workflow was defined with (Phase 5) — the
 * trigger daemon (Agent 3) reads these to know which workflows to start on an
 * event or a cron tick. Empty array for a workflow with no triggers, or one
 * not registered in this process.
 */
export function getWorkflowTriggers(name: string): readonly WorkflowTrigger[] {
  return registry.get(name)?.triggers ?? []
}
