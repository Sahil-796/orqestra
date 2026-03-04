import type { Redis } from 'ioredis'
import type { OrqestraOptions } from './types/index'
import { createRedisClient } from './core/redis'

export class Orqestra {
  public readonly redis: Redis
  public readonly prefix: string

  constructor(options: OrqestraOptions) {
    this.prefix = options.prefix ?? 'orq'
    this.redis = createRedisClient(options.redisUrl)
  }

  workflow(): void {

  }

  // async start(name: string, payload: unknown): Promise<string> {

  // }
}
