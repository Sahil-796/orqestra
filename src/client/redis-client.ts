import Redis from 'ioredis'

export function createRedisClient(redis: Redis | string): Redis {
  if (typeof redis === 'string') {
    return new Redis(redis, {
      maxRetriesPerRequest: 3,
      lazyConnect: true,
    })
  }
  return redis
}
