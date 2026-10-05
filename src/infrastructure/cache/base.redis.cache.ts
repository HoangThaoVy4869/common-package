import _ from 'lodash';
import {
    IRedisService,
    SetRedisParams,
    GetRedisParams,
    MGetRedisParams,
    DeleteRedisParams,
    DelRedisParams,
    ExpireRedisParams,
    KeyOnlyParams,
    LockParams,
    ReleaseLockParams,
    HSetParams,
    HGetParams,
    HDelParams,
    SetMemberParams,
    IsMemberParams,
    ListPushParams,
    ListRangeParams
} from '../../core/interfaces/IRedisService';

function serializeValue(value: any): string {
    if (value === null || value === undefined) return '';
    if (typeof value === 'object') return JSON.stringify(value);
    return String(value);
}

function deserializeValue<T = any>(value: string | null): T | null {
    if (value === null || value === undefined) return null;
    try {
        return JSON.parse(value);
    } catch {
        return value as unknown as T;
    }
}

export class BaseRedisService implements IRedisService {
    constructor(protected readonly client: any) {}

    getClient(): any {
        return this.client;
    }

    isReady(): boolean {
        return this.client?.status === 'ready';
    }

    async ping(): Promise<string> {
        return this.client.ping();
    }

    async disconnect(): Promise<void> {
        if (this.client) {
            try {
                await this.client.quit();
            } catch {
                this.client.disconnect();
            }
        }
    }

    // -------------------------------------------------------------
    // Basic String / Key-Value Operations
    // -------------------------------------------------------------

    async set({ key, value, ttl, mode }: SetRedisParams): Promise<any> {
        const serialized = serializeValue(value);

        if (typeof ttl === 'number' && ttl > 0) {
            return mode
                ? this.client.set(key, serialized, 'EX', ttl, mode)
                : this.client.set(key, serialized, 'EX', ttl);
        }

        return mode ? this.client.set(key, serialized, mode) : this.client.set(key, serialized);
    }

    async get<T = any>({ key }: GetRedisParams): Promise<T | null> {
        const raw = await this.client.get(key);
        return deserializeValue<T>(raw);
    }

    async mget<T = any>({ keys }: MGetRedisParams): Promise<(T | null)[]> {
        if (!keys || keys.length === 0) return [];
        const values = await this.client.mget(keys);
        return values.map((val: string | null) => deserializeValue<T>(val));
    }

    async delete({ key }: DeleteRedisParams): Promise<any> {
        return this.client.del(key);
    }

    async del({ key }: DelRedisParams): Promise<number> {
        const keys = Array.isArray(key) ? key : [key];
        if (keys.length === 0) return 0;
        return this.client.del(...keys);
    }

    async exists({ key }: KeyOnlyParams): Promise<boolean> {
        const count = await this.client.exists(key);
        return count > 0;
    }

    async ttl({ key }: KeyOnlyParams): Promise<number> {
        return this.client.ttl(key);
    }

    async pttl({ key }: KeyOnlyParams): Promise<number> {
        return this.client.pttl(key);
    }

    async incr({ key }: KeyOnlyParams): Promise<number> {
        return this.client.incr(key);
    }

    async decr({ key }: KeyOnlyParams): Promise<number> {
        return this.client.decr(key);
    }

    async expire({ key, ttl }: ExpireRedisParams): Promise<any> {
        return this.client.expire(key, ttl);
    }

    // -------------------------------------------------------------
    // Atomic & Distributed Lock Helpers
    // -------------------------------------------------------------

    async setNX({ key, value, ttl }: { key: string; value: any; ttl?: number }): Promise<boolean> {
        const serialized = serializeValue(value);
        if (typeof ttl === 'number' && ttl > 0) {
            const res = await this.client.set(key, serialized, 'EX', ttl, 'NX');
            return res === 'OK';
        }
        const res = await this.client.setnx(key, serialized);
        return res === 1;
    }

    async acquireLock({ key, ttlSeconds = 60, lockValue = 'LOCKED' }: LockParams): Promise<boolean> {
        const res = await this.client.set(key, lockValue, 'EX', ttlSeconds, 'NX');
        return res === 'OK';
    }

    async releaseLock({ key, lockValue }: ReleaseLockParams): Promise<boolean> {
        if (lockValue) {
            const lua = `
                if redis.call("get", KEYS[1]) == ARGV[1] then
                    return redis.call("del", KEYS[1])
                else
                    return 0
                end
            `;
            const res = await this.client.eval(lua, 1, key, lockValue);
            return res === 1;
        }
        const res = await this.client.del(key);
        return res > 0;
    }

    async isLocked({ key }: KeyOnlyParams): Promise<boolean> {
        const val = await this.client.get(key);
        return val !== null;
    }

    async getRemainingTime({ key }: KeyOnlyParams): Promise<number> {
        const ttl = await this.client.pttl(key);
        if (ttl <= 0) return 0;
        return Math.ceil(ttl / 1000);
    }

    // -------------------------------------------------------------
    // Hashes
    // -------------------------------------------------------------

    async hset({ key, field, value, data }: HSetParams): Promise<number> {
        if (data && typeof data === 'object') {
            const serializedObj: Record<string, string> = {};
            for (const [k, v] of Object.entries(data)) {
                serializedObj[k] = serializeValue(v);
            }
            return this.client.hset(key, serializedObj);
        }

        if (field !== undefined) {
            return this.client.hset(key, field, serializeValue(value));
        }

        return 0;
    }

    async hget<T = any>({ key, field }: HGetParams): Promise<T | null> {
        const raw = await this.client.hget(key, field);
        return deserializeValue<T>(raw);
    }

    async hgetall<T = Record<string, any>>({ key }: KeyOnlyParams): Promise<T> {
        const raw = await this.client.hgetall(key);
        const result: Record<string, any> = {};
        for (const [k, v] of Object.entries(raw || {})) {
            result[k] = deserializeValue(v as string);
        }
        return result as T;
    }

    async hdel({ key, fields }: HDelParams): Promise<number> {
        const fieldList = Array.isArray(fields) ? fields : [fields];
        if (fieldList.length === 0) return 0;
        return this.client.hdel(key, ...fieldList);
    }

    // -------------------------------------------------------------
    // Sets
    // -------------------------------------------------------------

    async sadd({ key, members }: SetMemberParams): Promise<number> {
        const memberList = (Array.isArray(members) ? members : [members]).map((m) => serializeValue(m));
        if (memberList.length === 0) return 0;
        return this.client.sadd(key, ...memberList);
    }

    async srem({ key, members }: SetMemberParams): Promise<number> {
        const memberList = (Array.isArray(members) ? members : [members]).map((m) => serializeValue(m));
        if (memberList.length === 0) return 0;
        return this.client.srem(key, ...memberList);
    }

    async smembers({ key }: KeyOnlyParams): Promise<string[]> {
        return this.client.smembers(key);
    }

    async sismember({ key, member }: IsMemberParams): Promise<boolean> {
        const res = await this.client.sismember(key, serializeValue(member));
        return res === 1;
    }

    // -------------------------------------------------------------
    // Lists
    // -------------------------------------------------------------

    async lpush({ key, values }: ListPushParams): Promise<number> {
        const valList = (Array.isArray(values) ? values : [values]).map((v) => serializeValue(v));
        if (valList.length === 0) return 0;
        return this.client.lpush(key, ...valList);
    }

    async rpush({ key, values }: ListPushParams): Promise<number> {
        const valList = (Array.isArray(values) ? values : [values]).map((v) => serializeValue(v));
        if (valList.length === 0) return 0;
        return this.client.rpush(key, ...valList);
    }

    async lpop<T = any>({ key }: KeyOnlyParams): Promise<T | null> {
        const raw = await this.client.lpop(key);
        return deserializeValue<T>(raw);
    }

    async rpop<T = any>({ key }: KeyOnlyParams): Promise<T | null> {
        const raw = await this.client.rpop(key);
        return deserializeValue<T>(raw);
    }

    async lrange<T = any>({ key, start, stop }: ListRangeParams): Promise<T[]> {
        const items = await this.client.lrange(key, start, stop);
        return (items || []).map((item: string) => deserializeValue<T>(item) as T);
    }
}
