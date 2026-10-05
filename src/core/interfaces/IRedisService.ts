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

export interface IRedisService {
    // Basic operations
    set(params: SetRedisParams): Promise<any>;
    get<T = any>(params: GetRedisParams): Promise<T | null>;
    mget<T = any>(params: MGetRedisParams): Promise<(T | null)[]>;
    ttl(params: KeyOnlyParams): Promise<number>;
    pttl(params: KeyOnlyParams): Promise<number>;
    delete(params: DeleteRedisParams): Promise<any>;
    incr(params: KeyOnlyParams): Promise<number>;
    expire(params: ExpireRedisParams): Promise<any>;

    // Extended operations
    del?(params: DelRedisParams): Promise<number>;
    exists?(params: KeyOnlyParams): Promise<boolean>;
    decr?(params: KeyOnlyParams): Promise<number>;

    // Atomic & Distributed Lock
    setNX?(params: { key: string; value: any; ttl?: number }): Promise<boolean>;
    acquireLock?(params: LockParams): Promise<boolean>;
    releaseLock?(params: ReleaseLockParams): Promise<boolean>;
    isLocked?(params: KeyOnlyParams): Promise<boolean>;
    getRemainingTime?(params: KeyOnlyParams): Promise<number>;

    // Hash operations
    hset?(params: HSetParams): Promise<number>;
    hget?<T = any>(params: HGetParams): Promise<T | null>;
    hgetall?<T = Record<string, any>>(params: KeyOnlyParams): Promise<T>;
    hdel?(params: HDelParams): Promise<number>;

    // Set operations
    sadd?(params: SetMemberParams): Promise<number>;
    srem?(params: SetMemberParams): Promise<number>;
    smembers?(params: KeyOnlyParams): Promise<string[]>;
    sismember?(params: IsMemberParams): Promise<boolean>;

    // List operations
    lpush?(params: ListPushParams): Promise<number>;
    rpush?(params: ListPushParams): Promise<number>;
    lpop?<T = any>(params: KeyOnlyParams): Promise<T | null>;
    rpop?<T = any>(params: KeyOnlyParams): Promise<T | null>;
    lrange?<T = any>(params: ListRangeParams): Promise<T[]>;

    // Health & Lifecycle
    ping?(): Promise<string>;
    isReady?(): boolean;
    disconnect?(): Promise<void>;
    getClient?(): any;
}
