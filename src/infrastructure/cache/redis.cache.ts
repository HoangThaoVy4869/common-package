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
} from '@core/interfaces/IRedisService';
import { createRedisClient, getRedisClient, RedisConfigOptions } from './redis.client';

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

/**
 * Universal Redis Service.
 * Provides high-performance caching, distributed locks, hashes, sets, and lists.
 * Supports both direct arguments: `get('key')`, `set('key', val, ttl)`
 * and legacy object options: `get({ key })`, `set({ key, value, ttl })`.
 */
export class RedisService implements IRedisService {
    protected readonly client: any;

    constructor(clientOrOptions?: any | RedisConfigOptions) {
        if (clientOrOptions && (typeof clientOrOptions.get === 'function' || typeof clientOrOptions.set === 'function')) {
            this.client = clientOrOptions;
        } else if (clientOrOptions?.newConnection) {
            this.client = createRedisClient(clientOrOptions);
        } else {
            this.client = getRedisClient(clientOrOptions);
        }
    }

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
                if (this.client.status !== 'end') {
                    await this.client.quit();
                }
            } catch {
                this.client.disconnect();
            }
        }
    }

    // -------------------------------------------------------------
    // Basic String / Key-Value Operations
    // -------------------------------------------------------------

    async set(keyOrParams: string | SetRedisParams, value?: any, ttl?: number, mode?: string): Promise<any> {
        let key: string;
        let val: any;
        let expireTtl: number | undefined;
        let setMode: string | undefined;

        if (typeof keyOrParams === 'string') {
            key = keyOrParams;
            val = value;
            expireTtl = ttl;
            setMode = mode;
        } else {
            key = keyOrParams.key;
            val = keyOrParams.value;
            expireTtl = keyOrParams.ttl;
            setMode = keyOrParams.mode;
        }

        const serialized = serializeValue(val);
        if (typeof expireTtl === 'number' && expireTtl > 0) {
            return setMode
                ? this.client.set(key, serialized, 'EX', expireTtl, setMode)
                : this.client.set(key, serialized, 'EX', expireTtl);
        }

        return setMode ? this.client.set(key, serialized, setMode) : this.client.set(key, serialized);
    }

    async get<T = any>(keyOrParams: string | GetRedisParams): Promise<T | null> {
        const key = typeof keyOrParams === 'string' ? keyOrParams : keyOrParams.key;
        const raw = await this.client.get(key);
        return deserializeValue<T>(raw);
    }

    async mget<T = any>(keysOrParams: string[] | MGetRedisParams): Promise<(T | null)[]> {
        const keys = Array.isArray(keysOrParams) ? keysOrParams : keysOrParams.keys;
        if (!keys || keys.length === 0) return [];
        const values = await this.client.mget(keys);
        return values.map((val: string | null) => deserializeValue<T>(val));
    }

    async delete(keyOrParams: string | DeleteRedisParams): Promise<any> {
        const key = typeof keyOrParams === 'string' ? keyOrParams : keyOrParams.key;
        return this.client.del(key);
    }

    async del(keyOrParams: string | string[] | DelRedisParams): Promise<number> {
        let keys: string[];
        if (typeof keyOrParams === 'string') {
            keys = [keyOrParams];
        } else if (Array.isArray(keyOrParams)) {
            keys = keyOrParams;
        } else {
            keys = Array.isArray(keyOrParams.key) ? keyOrParams.key : [keyOrParams.key];
        }
        if (keys.length === 0) return 0;
        return this.client.del(...keys);
    }

    async exists(keyOrParams: string | KeyOnlyParams): Promise<boolean> {
        const key = typeof keyOrParams === 'string' ? keyOrParams : keyOrParams.key;
        const count = await this.client.exists(key);
        return count > 0;
    }

    async ttl(keyOrParams: string | KeyOnlyParams): Promise<number> {
        const key = typeof keyOrParams === 'string' ? keyOrParams : keyOrParams.key;
        return this.client.ttl(key);
    }

    async pttl(keyOrParams: string | KeyOnlyParams): Promise<number> {
        const key = typeof keyOrParams === 'string' ? keyOrParams : keyOrParams.key;
        return this.client.pttl(key);
    }

    async incr(keyOrParams: string | KeyOnlyParams): Promise<number> {
        const key = typeof keyOrParams === 'string' ? keyOrParams : keyOrParams.key;
        return this.client.incr(key);
    }

    async decr(keyOrParams: string | KeyOnlyParams): Promise<number> {
        const key = typeof keyOrParams === 'string' ? keyOrParams : keyOrParams.key;
        return this.client.decr(key);
    }

    async expire(keyOrParams: string | ExpireRedisParams, ttlSeconds?: number): Promise<any> {
        const key = typeof keyOrParams === 'string' ? keyOrParams : keyOrParams.key;
        const ttl = typeof keyOrParams === 'string' ? (ttlSeconds ?? 0) : keyOrParams.ttl;
        return this.client.expire(key, ttl);
    }

    // -------------------------------------------------------------
    // Atomic & Distributed Lock
    // -------------------------------------------------------------

    async setNX(keyOrParams: string | { key: string; value: any; ttl?: number }, value?: any, ttl?: number): Promise<boolean> {
        const key = typeof keyOrParams === 'string' ? keyOrParams : keyOrParams.key;
        const val = typeof keyOrParams === 'string' ? value : keyOrParams.value;
        const expireTtl = typeof keyOrParams === 'string' ? ttl : keyOrParams.ttl;

        const serialized = serializeValue(val);
        const res = typeof expireTtl === 'number' && expireTtl > 0
            ? await this.client.set(key, serialized, 'EX', expireTtl, 'NX')
            : await this.client.set(key, serialized, 'NX');

        return res === 'OK';
    }

    async acquireLock(keyOrParams: string | LockParams, ttlSeconds = 10, lockValue?: string): Promise<boolean> {
        const key = typeof keyOrParams === 'string' ? keyOrParams : keyOrParams.key;
        const ttl = typeof keyOrParams === 'string' ? ttlSeconds : (keyOrParams.ttlSeconds ?? 10);
        const val = typeof keyOrParams === 'string' ? (lockValue || 'locked') : (keyOrParams.lockValue || 'locked');

        const res = await this.client.set(key, val, 'EX', ttl, 'NX');
        return res === 'OK';
    }

    async releaseLock(keyOrParams: string | ReleaseLockParams, lockValue?: string): Promise<boolean> {
        const key = typeof keyOrParams === 'string' ? keyOrParams : keyOrParams.key;
        const val = typeof keyOrParams === 'string' ? lockValue : keyOrParams.lockValue;

        if (val) {
            const luaScript = `
                if redis.call("get", KEYS[1]) == ARGV[1] then
                    return redis.call("del", KEYS[1])
                else
                    return 0
                end
            `;
            const res = await this.client.eval(luaScript, 1, key, val);
            return res === 1;
        }

        const res = await this.client.del(key);
        return res > 0;
    }

    async isLocked(keyOrParams: string | KeyOnlyParams): Promise<boolean> {
        return this.exists(keyOrParams);
    }

    async getRemainingTime(keyOrParams: string | KeyOnlyParams): Promise<number> {
        return this.ttl(keyOrParams);
    }

    // -------------------------------------------------------------
    // Hashes
    // -------------------------------------------------------------

    async hset(keyOrParams: string | HSetParams, fieldOrData?: string | Record<string, any>, value?: any): Promise<number> {
        if (typeof keyOrParams !== 'string') {
            const { key, field, value: val, data } = keyOrParams;
            if (data) {
                const serializedData: Record<string, string> = {};
                for (const [k, v] of Object.entries(data)) {
                    serializedData[k] = serializeValue(v);
                }
                return this.client.hset(key, serializedData);
            }
            if (field !== undefined) {
                return this.client.hset(key, field, serializeValue(val));
            }
            return 0;
        }

        const key = keyOrParams;
        if (fieldOrData && typeof fieldOrData === 'object' && !Array.isArray(fieldOrData)) {
            const serializedData: Record<string, string> = {};
            for (const [k, v] of Object.entries(fieldOrData)) {
                serializedData[k] = serializeValue(v);
            }
            return this.client.hset(key, serializedData);
        }

        if (typeof fieldOrData === 'string') {
            return this.client.hset(key, fieldOrData, serializeValue(value));
        }

        return 0;
    }

    async hget<T = any>(keyOrParams: string | HGetParams, field?: string): Promise<T | null> {
        const key = typeof keyOrParams === 'string' ? keyOrParams : keyOrParams.key;
        const f = typeof keyOrParams === 'string' ? (field ?? '') : keyOrParams.field;
        const raw = await this.client.hget(key, f);
        return deserializeValue<T>(raw);
    }

    async hgetall<T = Record<string, any>>(keyOrParams: string | KeyOnlyParams): Promise<T> {
        const key = typeof keyOrParams === 'string' ? keyOrParams : keyOrParams.key;
        const raw = await this.client.hgetall(key);
        if (!raw || Object.keys(raw).length === 0) return {} as T;

        const result: Record<string, any> = {};
        for (const [k, val] of Object.entries(raw)) {
            result[k] = deserializeValue(val as string);
        }
        return result as T;
    }

    async hdel(keyOrParams: string | HDelParams, fields?: string | string[]): Promise<number> {
        const key = typeof keyOrParams === 'string' ? keyOrParams : keyOrParams.key;
        const f = typeof keyOrParams === 'string' ? (fields ?? []) : keyOrParams.fields;
        const fieldList = Array.isArray(f) ? f : [f];
        if (fieldList.length === 0) return 0;
        return this.client.hdel(key, ...fieldList);
    }

    // -------------------------------------------------------------
    // Sets
    // -------------------------------------------------------------

    async sadd(keyOrParams: string | SetMemberParams, members?: any | any[]): Promise<number> {
        const key = typeof keyOrParams === 'string' ? keyOrParams : keyOrParams.key;
        const m = typeof keyOrParams === 'string' ? (members ?? []) : keyOrParams.members;
        const memberList = (Array.isArray(m) ? m : [m]).map((item) => serializeValue(item));
        if (memberList.length === 0) return 0;
        return this.client.sadd(key, ...memberList);
    }

    async srem(keyOrParams: string | SetMemberParams, members?: any | any[]): Promise<number> {
        const key = typeof keyOrParams === 'string' ? keyOrParams : keyOrParams.key;
        const m = typeof keyOrParams === 'string' ? (members ?? []) : keyOrParams.members;
        const memberList = (Array.isArray(m) ? m : [m]).map((item) => serializeValue(item));
        if (memberList.length === 0) return 0;
        return this.client.srem(key, ...memberList);
    }

    async smembers(keyOrParams: string | KeyOnlyParams): Promise<string[]> {
        const key = typeof keyOrParams === 'string' ? keyOrParams : keyOrParams.key;
        return this.client.smembers(key);
    }

    async sismember(keyOrParams: string | IsMemberParams, member?: any): Promise<boolean> {
        const key = typeof keyOrParams === 'string' ? keyOrParams : keyOrParams.key;
        const m = typeof keyOrParams === 'string' ? member : keyOrParams.member;
        const res = await this.client.sismember(key, serializeValue(m));
        return res === 1;
    }

    // -------------------------------------------------------------
    // Lists
    // -------------------------------------------------------------

    async lpush(keyOrParams: string | ListPushParams, values?: any | any[]): Promise<number> {
        const key = typeof keyOrParams === 'string' ? keyOrParams : keyOrParams.key;
        const v = typeof keyOrParams === 'string' ? (values ?? []) : keyOrParams.values;
        const valList = (Array.isArray(v) ? v : [v]).map((item) => serializeValue(item));
        if (valList.length === 0) return 0;
        return this.client.lpush(key, ...valList);
    }

    async rpush(keyOrParams: string | ListPushParams, values?: any | any[]): Promise<number> {
        const key = typeof keyOrParams === 'string' ? keyOrParams : keyOrParams.key;
        const v = typeof keyOrParams === 'string' ? (values ?? []) : keyOrParams.values;
        const valList = (Array.isArray(v) ? v : [v]).map((item) => serializeValue(item));
        if (valList.length === 0) return 0;
        return this.client.rpush(key, ...valList);
    }

    async lpop<T = any>(keyOrParams: string | KeyOnlyParams): Promise<T | null> {
        const key = typeof keyOrParams === 'string' ? keyOrParams : keyOrParams.key;
        const raw = await this.client.lpop(key);
        return deserializeValue<T>(raw);
    }

    async rpop<T = any>(keyOrParams: string | KeyOnlyParams): Promise<T | null> {
        const key = typeof keyOrParams === 'string' ? keyOrParams : keyOrParams.key;
        const raw = await this.client.rpop(key);
        return deserializeValue<T>(raw);
    }

    async lrange<T = any>(keyOrParams: string | ListRangeParams, start?: number, stop?: number): Promise<T[]> {
        const key = typeof keyOrParams === 'string' ? keyOrParams : keyOrParams.key;
        const s = typeof keyOrParams === 'string' ? (start ?? 0) : keyOrParams.start;
        const e = typeof keyOrParams === 'string' ? (stop ?? -1) : keyOrParams.stop;
        const items = await this.client.lrange(key, s, e);
        return (items || []).map((item: string) => deserializeValue<T>(item) as T);
    }
}

/**
 * @deprecated Use `RedisService` instead. Re-exported for backward compatibility.
 */
export { RedisService as BaseRedisService };

