import Redis from "ioredis";
import { type Result, Ok, Err } from "@/lib/result";

export interface RateLimitResult {
  allowed: boolean;
  limit: number;
  remaining: number;
  resetAt: number;
}

export interface FixedWindowConfig {
  windowSecond: number;
  maxRequests: number;
  keyPrefix: string;
}

const DEFAULT_CONFIG: FixedWindowConfig = {
  windowSecond: 60,
  maxRequests: 120,
  keyPrefix: "rl:fw"
}

export class FixedWindowLimiter {
  private readonly redis: Redis;
  private readonly config: FixedWindowConfig;

  constructor(redis: Redis, config?: Partial<FixedWindowConfig>) {
    this.redis = redis;
    this.config = { ...DEFAULT_CONFIG, ...config };
  }

  private getWindowkey(identifier: string): { key: string; resetAt: number } {
    const now = Math.floor(Date.now() / 1000);
    const window = Math.floor(now / this.config.windowSecond);
    const resetAt = (window + 1) * this.config.windowSecond;
    const key = `${this.config.keyPrefix}:${identifier}:${window}`;

    return { key, resetAt };
  }

  async check(identifier: string): Promise<Result<RateLimitResult, Error>> {
    const { key, resetAt } = this.getWindowkey(identifier);

    try {
      const tx = this.redis.multi();
      tx.incr(key);
      const now = Date.now() / 1000;
      const expirationTime = resetAt - now;
      tx.expire(key, Math.ceil(expirationTime), "NX");

      const results = await tx.exec();
      if (!results) return Err(new Error("redis transaction returned null"));

      const [incrError, count] = results[0];
      const [expireError] = results[1];

      /*
      * with ioredis, multi().exec() normally resolves to an array of results, where each command result is a tuple:
      * [
      *    [null, 1] -> INCR succeeded, returned 1
      *    [null, 60] -> EXPIRE succeeded, returned 60
      * ]
      */

      if (incrError) {
        return Err(incrError);
      }

      if (expireError) {
        return Err(expireError);
      }

      if (typeof count !== "number") {
        return Err(new Error("Redis INCR returned an invalid result"));
      }

      const allowed = count <= this.config.maxRequests;
      const remaining = Math.max(0, this.config.maxRequests - count);

      return Ok({ allowed, limit: this.config.maxRequests, remaining, resetAt });
    } catch (err) {
      return Err(err instanceof Error ? err : new Error(String(err)));
    }
  }

  /*
  *   The main use of MULTI is to group multiple Redis commands into a transaction so they execute sequentially without 
  *   other clients' commands interleaving between them.
  *
  *   Without MULTI, another client could execute commands between your INCR and EXPIRE.
  *   However MULTI does not:
  *   - Rollback successful commands if another command fails.
  *   - Automatically make a conditional check and increment safe if the check happens outside the transaction
  *   - Guarantee every command succeeds.
  *
  *   The cleanest solution to make these instructions atomic is to use a Redis Lua Script.
  *   The problem isnt just grouping INCR and EXPIRE. We want Redis to perform the entire operation atomically:
  *   - Increment the request counter
  *   - Set expiration if this is the first request
  *   - Return the count so your application can calculate allowed and remaining
  */

  /* Implementation Using Redis Lua Script */
  /*
   *  const script = `
   *    local count = redis.call("INCR", KEYS[1])
   *    if count == 1 then
   *      redis.call("PEXPIRE", KEYS[1], ARGV[1])
   *    end
   *
   *    return count
   *  `
   *
   *  async check(identifier: string): Promise<Result<RateLimitResult, Error>> {
   *    const {key, resetAt} = this.getWindowkey(identifier);
   *
   *    try {
   *      const ttlMs = resetAt * 1000 - Date.now();
   *      const count = await this.redis.eval(
   *        script,
   *        1,
   *        key,
   *        ttlMs,
   *      ) as number;
   *
   *      const allowed = count <= this.config.maxRequests;
   *      const remaining = Math.max(0, this.config.maxRequests - count);
   *
   *      return Ok({allowed, limit: this.config.maxRequests, remaining, resetAt});
   *    } catch (err) {
   *      return Err(err instanceof Error ? err : new Error(String(err)));
   *    }
   *  }
   *
   *  - INCR and the initial PEXPIRE execute atomically inside Redis
   *  - Expiration is set only on the first request, using milliseconds until the fixed window milliseconds
   *  - Concurrent req receive distinct counter values, so the application can correctly calculate whether reach req is within the limit
  */
}
