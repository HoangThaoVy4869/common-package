import Redis, { Redis as RedisType } from 'ioredis'

let redisClient: RedisType | null = null

export function getRedisClient(): RedisType {
    if (!redisClient) {
        redisClient = new Redis({
            host: process.env.REDIS_HOST || 'localhost',
            port: Number(process.env.REDIS_PORT) || 6379
        })
    }
    return redisClient
}
