import { getRedisClient } from "./redis.client";
import { BaseRedisService } from "./base.redis.cache";
export class RedisService extends BaseRedisService {
    constructor() {
        super(getRedisClient())
    }
}
