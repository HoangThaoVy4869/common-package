import { IIdempotencyRepository } from '@core/interfaces/IIdempotencyRepository';
import { IRedisService } from '@core/interfaces/IRedisService';

export interface RedisIdempotencyOptions {
    keyPrefix?: string;
    defaultTtlSeconds?: number;
}

export class RedisIdempotencyRepository implements IIdempotencyRepository {
    private readonly prefix: string;
    private readonly defaultTtl: number;

    constructor(
        private readonly redisService: IRedisService,
        options?: RedisIdempotencyOptions
    ) {
        this.prefix = options?.keyPrefix ?? 'idempotency:';
        this.defaultTtl = options?.defaultTtlSeconds ?? 86400; // default 24h
    }

    async claim(key: string, ownerId: string, ttlSeconds?: number): Promise<boolean> {
        const fullKey = `${this.prefix}${key}`;
        const ttl = ttlSeconds ?? this.defaultTtl;

        if (typeof this.redisService.acquireLock === 'function') {
            return this.redisService.acquireLock({
                key: fullKey,
                ttlSeconds: ttl,
                lockValue: ownerId
            });
        }

        const res = await this.redisService.set({
            key: fullKey,
            value: ownerId,
            ttl,
            mode: 'NX'
        });
        return res === 'OK' || res === true;
    }

    async release(key: string, ownerId: string): Promise<void> {
        const fullKey = `${this.prefix}${key}`;

        if (typeof this.redisService.releaseLock === 'function') {
            await this.redisService.releaseLock({
                key: fullKey,
                lockValue: ownerId
            });
            return;
        }

        const client = typeof (this.redisService as any).getClient === 'function' ? (this.redisService as any).getClient() : null;
        if (client && typeof client.eval === 'function') {
            const luaScript = `
                if redis.call("get", KEYS[1]) == ARGV[1] then
                    return redis.call("del", KEYS[1])
                else
                    return 0
                end
            `;
            await client.eval(luaScript, 1, fullKey, ownerId);
            return;
        }

        const currentOwner = await this.redisService.get({ key: fullKey });
        if (currentOwner === ownerId) {
            await this.redisService.delete({ key: fullKey });
        }
    }

    async isClaimed(key: string): Promise<boolean> {
        const fullKey = `${this.prefix}${key}`;

        if (typeof this.redisService.isLocked === 'function') {
            return this.redisService.isLocked({ key: fullKey });
        }

        const val = await this.redisService.get({ key: fullKey });
        return val !== null;
    }
}
