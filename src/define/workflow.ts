// Public API: defineWorkflow(name, builderFn) — declare steps, get back a
// serializable DAG definition (the `dag` jsonb) plus a handle that can
// register that DAG into Postgres. Running the DAG is out of scope for
// Phase 0 (Phase 1 adds the executor).

import type { Db } from '../store/client.ts'
import { getWorkflowByName, insertWorkflow, type WorkflowRow } from '../store/repositories.ts'
import type { StepDefinition, WorkflowDefinition } from '../types.ts'
import type { WorkflowContext } from './context.ts'

export type StepFn<T = unknown> = (ctx: WorkflowContext) => Promise<T>

export interface StepOptions {
  /** Names of steps that must complete before this one becomes ready. */
  dependsOn?: string[]
  maxAttempts?: number
  timeoutMs?: number
  priority?: number
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
    })
    return this
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

export interface WorkflowHandle {
  readonly name: string
  readonly definition: WorkflowDefinition
  /** Step implementations, keyed by step name — for the Phase 1+ executor. */
  readonly stepFns: Map<string, StepFn>
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
  builderFn: (builder: WorkflowBuilder) => void
): WorkflowHandle {
  const builder = new WorkflowBuilder()
  builderFn(builder)

  const steps = builder.buildSteps()
  const stepFns = builder.buildStepFns()

  const definition: WorkflowDefinition = { name, version: 1, steps }

  const handle: WorkflowHandle = {
    name,
    definition,
    stepFns,
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
