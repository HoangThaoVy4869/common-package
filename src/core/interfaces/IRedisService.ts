export interface IRedisService {
    set(params: { key: string; value: any; ttl?: number; mode?: string }): Promise<void>;
    get(params: { key: string }): Promise<any>;
    mget(params: { keys: string[] }): Promise<null[]>;
    ttl(params: { key: string }): Promise<number>;
    pttl(params: { key: string }): Promise<number>;
    delete(params: { key: string }): Promise<void>;
    incr(params: { key: string }): Promise<number>;
    expire(params: { key: string; ttl: number }): Promise<void>;
}
