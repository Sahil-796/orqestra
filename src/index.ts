import { createRedisClient } from './core/redis'
import { WorkflowBuilder } from './core/workflow'

export class Orquestra {
  private redis: ReturnType<typeof createRedisClient>
  private prefix: string
  private workflows = new Map<string, WorkflowBuilder>()

  constructor(redisUrl: string, prefix = 'orq') {
    this.prefix = prefix
    this.redis = createRedisClient(redisUrl)
  }

  define(name: string): WorkflowBuilder {
    const workflow = new WorkflowBuilder()
    this.workflows.set(name, workflow)
    return workflow
  }
}

export const orquestra = (redisUrl: string, prefix = 'orq') => new Orquestra(redisUrl, prefix)
