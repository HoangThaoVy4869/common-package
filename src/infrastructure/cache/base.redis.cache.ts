import _ from 'lodash';
import { IRedisService } from '../../core/interfaces';

export class BaseRedisService implements IRedisService {
    constructor(protected readonly client: any) {}

    async set({ key, value, ttl, mode }: any) {
        if (_.isObject(value)) value = JSON.stringify(value);
        if (_.isNumber(value)) value = String(value);

        if (_.isNumber(ttl)) {
            return mode ? this.client.set(key, value, 'EX', ttl, mode) : this.client.set(key, value, 'EX', ttl);
        }

        return mode ? this.client.set(key, value, mode) : this.client.set(key, value);
    }

    async get({ key }: { key: string }) {
        return this.client.get(key);
    }

    async mget({ keys }: { keys: string[] }) {
        const values = await this.client.mget(keys);

        return values.map((value: string | null) => {
            if (value === null) return null;

            try {
                return JSON.parse(value);
            } catch {
                return value;
            }
        });
    }

    async ttl({ key }: { key: string }) {
        return this.client.ttl(key);
    }

    async pttl({ key }: { key: string }) {
        return this.client.pttl(key);
    }

    async delete({ key }: { key: string }) {
        return this.client.del(key);
    }

    async incr({ key }: { key: string }) {
        return this.client.incr(key);
    }

    async expire({ key, ttl }: { key: string; ttl: number }) {
        await this.client.expire(key, ttl);
    }
}
