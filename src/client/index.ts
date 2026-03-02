import type { Redis } from 'ioredis'
import type { OrqestraOptions } from '../types'
import { createRedisClient } from './redis-client'

export class OrqestraClient {
  public redis: Redis
  public prefix: string
  public maxAttempts: number
  public visibilityTimeout: number

  constructor(options: OrqestraOptions) {
    this.prefix = options.prefix ?? 'orqestra'
    this.maxAttempts = options.maxAttempts ?? 3
    this.visibilityTimeout = options.visibilityTimeout ?? 30000
    this.redis = createRedisClient(options.redis)
  }
}
