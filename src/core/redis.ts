import Redis from 'ioredis'

export function createRedisClient(redisUrl: string, prefix: string): Redis {
  return new Redis(redisUrl, {
    maxRetriesPerRequest: 3,
    lazyConnect: true,
  })
}
// todo handle prefix 