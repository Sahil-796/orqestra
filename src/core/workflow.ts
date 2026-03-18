import type Redis from 'ioredis'

type StepFn<T = unknown> = (
  prev: unknown,
  ctx: Record<string, unknown>
) => Promise<T>

export class WorkflowBuilder {
  private steps = new Map<number, StepFn>()
  private redis: Redis

  constructor(redis: Redis) {
    this.redis = redis
  }

  step<T>(id: number, fn: StepFn<T>): this {
    this.steps.set(id, fn)

    return this
  }

  async run(initialCtx: Record<string, unknown> = {}): Promise<unknown> {
    const ctx = { ...initialCtx }
    const sortedIds = Array.from(this.steps.keys()).sort((a, b) => a - b)
    
    let prev: unknown = undefined
    
    for (const id of sortedIds) {
      const step = this.steps.get(id)
      if (!step) continue
      prev = await step(prev, ctx)
    }
    
    return prev
  }
}
