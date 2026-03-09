import type { Redis } from 'ioredis'
import type { OrqestraOptions, WorkflowHandler } from './types/index'
import { createRedisClient } from './core/redis'

export class Orqestra {
  public readonly redis: Redis
  public readonly prefix: string
  private workflows = new Map<string, WorkflowHandler>()

  constructor(options: OrqestraOptions) {
    this.prefix = options.prefix ?? 'orq'
    this.redis = createRedisClient(options.redisUrl)
  }

  define(name: string, handler: WorkflowHandler): void {
    this.workflows.set(name, handler)
  }

  // async start(name: string, payload: unknown): Promise<string> {
  //   const handler = this.workflows.get(name)
  //   if (!handler) throw new Error(`Workflow ${name} not found`)
  //   return 'todo'
  // }
}
