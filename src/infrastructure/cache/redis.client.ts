import Redis, { type Redis as RedisType, type RedisOptions } from "ioredis";

export interface RedisConfigOptions extends RedisOptions {
    url?: string;
    uri?: string;
}

let redisClient: RedisType | null = null;

export function createRedisClient(options?: RedisConfigOptions): RedisType {
    const connectionUrl = options?.url || options?.uri || process.env.REDIS_URI || process.env.REDIS_URL;

    const baseOptions: RedisOptions = {
        host: options?.host || process.env.REDIS_HOST || 'localhost',
        port: options?.port || (process.env.REDIS_PORT ? Number(process.env.REDIS_PORT) : 6379),
        password: options?.password || process.env.REDIS_PASSWORD || undefined,
        username: options?.username || process.env.REDIS_USERNAME || undefined,
        db: options?.db !== undefined ? options.db : (process.env.REDIS_DB ? Number(process.env.REDIS_DB) : 0),
        keyPrefix: options?.keyPrefix || process.env.REDIS_KEY_PREFIX || undefined,
        retryStrategy: options?.retryStrategy || ((times: number) => Math.min(times * 100, 3000)),
        maxRetriesPerRequest: options?.maxRetriesPerRequest ?? 3,
        enableReadyCheck: options?.enableReadyCheck ?? true,
        lazyConnect: options?.lazyConnect ?? false,
        ...options
    };

    const client = connectionUrl ? new Redis(connectionUrl, baseOptions) : new Redis(baseOptions);

    client.on('error', (err) => {
        if (process.env.NODE_ENV !== 'test') {
            console.error('[RedisClient] Connection error:', err.message);
        }
    });

    return client;
}

export function getRedisClient(options?: RedisConfigOptions): RedisType {
    if (!redisClient) {
        redisClient = createRedisClient(options);
    }
    return redisClient;
}

export async function closeRedisClient(): Promise<void> {
    if (redisClient) {
        try {
            await redisClient.quit();
        } catch {
            redisClient.disconnect();
        } finally {
            redisClient = null;
        }
    }
}
