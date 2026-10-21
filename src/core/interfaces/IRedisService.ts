export interface SetRedisParams {
    key: string;
    value: any;
    ttl?: number;
    mode?: string;
}

export interface GetRedisParams {
    key: string;
}

export interface MGetRedisParams {
    keys: string[];
}

export interface DeleteRedisParams {
    key: string;
}

export interface DelRedisParams {
    key: string | string[];
}

export interface ExpireRedisParams {
    key: string;
    ttl: number;
}

export interface KeyOnlyParams {
    key: string;
}

export interface LockParams {
    key: string;
    ttlSeconds?: number;
    lockValue?: string;
}

export interface ReleaseLockParams {
    key: string;
    lockValue?: string;
}

export interface HSetParams {
    key: string;
    field?: string;
    value?: any;
    data?: Record<string, any>;
}

export interface HGetParams {
    key: string;
    field: string;
}

export interface HDelParams {
    key: string;
    fields: string | string[];
}

export interface SetMemberParams {
    key: string;
    members: (string | number)[] | string | number;
}

export interface IsMemberParams {
    key: string;
    member: string | number;
}

export interface ListPushParams {
    key: string;
    values: any[] | any;
}

export interface ListRangeParams {
    key: string;
    start: number;
    stop: number;
}

/**
 * Universal Redis Service Interface.
 * Supports both natural arguments: `get('key')`, `set('key', val, ttl)`
 * and legacy object parameters: `get({ key })`, `set({ key, value, ttl })`.
 */
export interface IRedisService {
    // Basic operations
    set(keyOrParams: string | SetRedisParams, value?: any, ttl?: number, mode?: string): Promise<any>;
    get<T = any>(keyOrParams: string | GetRedisParams): Promise<T | null>;
    mget<T = any>(keysOrParams: string[] | MGetRedisParams): Promise<(T | null)[]>;
    ttl(keyOrParams: string | KeyOnlyParams): Promise<number>;
    pttl(keyOrParams: string | KeyOnlyParams): Promise<number>;
    delete(keyOrParams: string | DeleteRedisParams): Promise<any>;
    del?(keyOrParams: string | string[] | DelRedisParams): Promise<number>;
    exists?(keyOrParams: string | KeyOnlyParams): Promise<boolean>;
    incr(keyOrParams: string | KeyOnlyParams): Promise<number>;
    decr?(keyOrParams: string | KeyOnlyParams): Promise<number>;
    expire(keyOrParams: string | ExpireRedisParams, ttlSeconds?: number): Promise<any>;

    // Atomic & Distributed Lock
    setNX?(keyOrParams: string | { key: string; value: any; ttl?: number }, value?: any, ttl?: number): Promise<boolean>;
    acquireLock?(keyOrParams: string | LockParams, ttlSeconds?: number, lockValue?: string): Promise<boolean>;
    releaseLock?(keyOrParams: string | ReleaseLockParams, lockValue?: string): Promise<boolean | void>;
    isLocked?(keyOrParams: string | KeyOnlyParams): Promise<boolean>;
    getRemainingTime?(keyOrParams: string | KeyOnlyParams): Promise<number>;

    // Hash operations
    hset?(keyOrParams: string | HSetParams, fieldOrData?: string | Record<string, any>, value?: any): Promise<number>;
    hget?<T = any>(keyOrParams: string | HGetParams, field?: string): Promise<T | null>;
    hgetall?<T = Record<string, any>>(keyOrParams: string | KeyOnlyParams): Promise<T>;
    hdel?(keyOrParams: string | HDelParams, fields?: string | string[]): Promise<number>;

    // Set operations
    sadd?(keyOrParams: string | SetMemberParams, members?: any | any[]): Promise<number>;
    srem?(keyOrParams: string | SetMemberParams, members?: any | any[]): Promise<number>;
    smembers?(keyOrParams: string | KeyOnlyParams): Promise<string[]>;
    sismember?(keyOrParams: string | IsMemberParams, member?: any): Promise<boolean>;

    // List operations
    lpush?(keyOrParams: string | ListPushParams, values?: any | any[]): Promise<number>;
    rpush?(keyOrParams: string | ListPushParams, values?: any | any[]): Promise<number>;
    lpop?<T = any>(keyOrParams: string | KeyOnlyParams): Promise<T | null>;
    rpop?<T = any>(keyOrParams: string | KeyOnlyParams): Promise<T | null>;
    lrange?<T = any>(keyOrParams: string | ListRangeParams, start?: number, stop?: number): Promise<T[]>;

    // Health & Lifecycle
    ping?(): Promise<string>;
    isReady?(): boolean;
    disconnect?(): Promise<void>;
    getClient?(): any;
}
