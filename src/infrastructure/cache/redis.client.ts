import type { Redis as RedisType } from "ioredis";

let redisClient: RedisType | null = null
const runtimeRequire = eval("require") as NodeJS.Require;

export function getRedisClient(): RedisType {
    if (!redisClient) {
        const { default: Redis } = runtimeRequire("ioredis") as typeof import("ioredis");
        redisClient = new Redis({
            host: process.env.REDIS_HOST || 'localhost',
            port: Number(process.env.REDIS_PORT) || 6379
        })
    }
    return redisClient
}
