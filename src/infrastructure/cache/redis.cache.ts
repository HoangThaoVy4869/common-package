import { getRedisClient, RedisConfigOptions } from "./redis.client";
import { BaseRedisService } from "./base.redis.cache";

export class RedisService extends BaseRedisService {
    constructor(clientOrOptions?: any | RedisConfigOptions) {
        if (clientOrOptions && (typeof clientOrOptions.get === 'function' || typeof clientOrOptions.set === 'function')) {
            super(clientOrOptions);
        } else {
            super(getRedisClient(clientOrOptions));
        }
    }
}
