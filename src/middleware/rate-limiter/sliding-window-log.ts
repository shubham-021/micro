import type Redis from "ioredis";
import { type Result, Ok, Err } from "@/lib/result";
import type { RateLimitResult } from "./fixed-window";

export interface SlidingWindowLogConfig {
  windowSeconds: number;
  maxRequests: number;
  keyPrefix: string;
}

const DEFAULT_CONFIG: SlidingWindowLogConfig = {
  windowSeconds: 60,
  maxRequests: 120,
  keyPrefix: "rl:swl"
};

export class SlidingWindowLogLimiter {
  private readonly redis: Redis;
  private readonly config: SlidingWindowLogConfig;

  constructor(redis: Redis, config?: Partial<SlidingWindowLogConfig>) {
    this.redis = redis;
    this.config = { ...DEFAULT_CONFIG, ...config };
  }

  async check(identifier: string): Promise<Result<RateLimitResult, Error>> {
    const key = `${this.config.keyPrefix}:${identifier}`;
    const now = Date.now();
    const windowStart = now - this.config.windowSeconds * 1000;

    const member = `${now}:${Math.random().toString(36).slice(2, 8)}`;

    /*
     *    We use Redis Sorted Set (ZSET) because sliding window log needs to store 
     *    request timestamps and efficiently remove requests that have expired from the window.
     */

    try {
      const tx = this.redis.multi();
      tx.zremrangebyscore(key, "-inf", windowStart);
      tx.zadd(key, now, member);
      tx.zcard(key);
      tx.expire(key, this.config.windowSeconds);
      tx.zrange(key, "0", "0", "WITHSCORES");

      const results = await tx.exec();
      if (!results) return Err(new Error("redis pipeline returned null"));

      const [zaddremrangeError, _removed] = results[0];
      const [zaddError, _addedNew] = results[1];
      const [zcardError, count] = results[2];
      const [zrangeError, oldest] = results[4];

      if (zaddremrangeError) {
        return Err(zaddremrangeError);
      }

      if (zaddError) {
        return Err(zaddError);
      }

      if (zcardError) {
        return Err(zcardError);
      }

      if (typeof count !== "number") {
        return Err(new Error("Count does not have the suitable type to proceed"));
      }

      if (zrangeError) {
        return Err(zrangeError);
      }

      if (!Array.isArray(oldest)) {
        return Err(new Error("Invalid ZRANGE result"));
      }

      const allowed = count <= this.config.maxRequests;
      const remaining = Math.max(0, this.config.maxRequests - count);

      const oldestTimestamp = Number(oldest[1]);
      if (oldest.length >= 2 && !Number.isFinite(oldestTimestamp)) {
        return Err(new Error("Invalid oldest request timestamp"));
      }

      const resetAt = (oldest.length >= 2) ? Math.ceil((Number(oldest[1]) + this.config.windowSeconds * 1000) / 1000)
        : Math.ceil(Date.now() / 1000);

      if (!allowed) {
        await this.redis.zrem(key, member);
      }

      return Ok({ allowed, limit: this.config.maxRequests, remaining, resetAt });
    } catch (err) {
      return Err(err instanceof Error ? err : new Error(String(err)));
    }
  }
}

/*    Lua Script for Atomicity:
 *
 *    const script = `
 *      local key = KEYS[1]
 *      local now = tonumber(ARGV[1])
 *      local windowMs = tonumber(ARGV[2])
 *      local maxRequests = tonumber(ARGV[3])
 *      local member = ARGV[4]
 *
 *      local windowStart = now - windowMs
 *
 *      redis.call("ZREMRANGEBYSCORE", key, "-inf", windowStart)
 *
 *      local count = redis.call("ZCARD", key)
 *
 *      local allowed = 0
 *
 *      if count < maxRequests then
 *        redis.call("ZADD", key, now, member)
 *        count = count + 1
 *        allowed = 1
 *      end
 *
 *      redis.call("PEXPIRE", key, windowMs)
 *
 *      local resetAt
 *
 *      if count > 0 then
 *        local oldest = redis.call("ZRANGE", key, 0, 0, "WITHSCORES")
 *        resetAt = math.ceil((tonumber(oldest[2]) + windowMs) / 1000)
 *      else
 *        resetAt = math.ceil((now + windowMs) / 1000)
 *      end
 *
 *      return {
 *        allowed,
 *        count,
 *        math.max(0, maxRequests - count),
 *        resetAt
 *      }
 *    `
 *
 *    const now = Date.now();
 *    const windowMs = this.config.windowMs * 1000;
 *    const member = `${now}:${Math.random().toString(36).slice(2, 8)}`;
 *
 *    const result = await this.redis.eval(
 *      script,
 *      1,
 *      key,
 *      now,
 *      windowMs,
 *      this.config.maxRequests,
 *      member
 *    ) as [number, number, number, number];
 *
 *    const [allowed, count, remaining, resetAt] = result;
 *
 *    return Ok({
 *      allowed: allowed === 1,
 *      limit: this.config.maxRequests,
 *      remaining,
 *      resetAt
 *    });
 */


