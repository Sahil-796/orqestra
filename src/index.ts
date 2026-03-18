import type Redis from 'ioredis'
import { createRedisClient } from './core/redis'
import { WorkflowBuilder } from './core/workflow'

export class Orquestra {
  private redis: Redis
  private prefix: string
  private workflows = new Map<string, WorkflowBuilder>()

  constructor(redisUrl: string, prefix = 'orq') {
    this.prefix = prefix
    this.redis = createRedisClient(redisUrl, prefix)
  }

  define(name: string): WorkflowBuilder {
    const workflow = new WorkflowBuilder(this.redis)
    this.workflows.set(name, workflow)
    return workflow
  }
  
  //run workflow using orq
  async run(name: string, initialCtx: Record<string, unknown> = {}): Promise<unknown> { 
    const workflow = this.workflows.get(name)
    if (!workflow) throw new Error(`Workflow ${name} not found`)
    return await workflow.run(initialCtx)
  }
}

export const orquestra = (redisUrl: string, prefix = 'orq') => new Orquestra(redisUrl, prefix)
