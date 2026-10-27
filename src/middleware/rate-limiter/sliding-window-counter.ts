import type Redis from "ioredis";
import { type Result, Ok, Err } from "@/lib/result";
import type { RateLimitResult } from "./fixed-window";

export interface SlidingWindowCounterConfig {
  windowSeconds: number;
  maxRequests: number;
  keyPrefix: string;
}

const DEFAULT_CONFIG: SlidingWindowCounterConfig = {
  windowSeconds: 60,
  maxRequests: 120,
  keyPrefix: "rl:swc",
}

/*  Sliding Window Counter:
 *  Keeps two fixed-window Counters: previous and current. Approximates a sliding window via weighted interpolation:
 *    weight = 1 - (elapsedInCurrentWindow/ windowSize)
 *    estimate = (previousCount * weight) + currentCount
 *
 *  Example: (60s window, now 45s into current window):
 *    weight = 1 - 45/60 = 0.25
 *    estimate = prev * 0.25 + current
 *
 *  Pros: O(1) memory (2 keys per client), near perfect accuracy
 *  Cons: Still an Approximation - assumes uniform distribution in previous window.
*/

export class SlidingWindowCounterLimiter {
  private readonly redis: Redis;
  private readonly config: SlidingWindowCounterConfig;

  constructor(redis: Redis, config?: Partial<SlidingWindowCounterConfig>) {
    this.redis = redis;
    this.config = { ...DEFAULT_CONFIG, ...config };
  }

  private getWindowInfo(): { currentWindow: number; previousWindow: number; elapsedRatio: number } {
    const now = Math.floor(Date.now() / 1000);
    const currentWindow = Math.floor(now / this.config.windowSeconds);
    const previousWindow = currentWindow - 1;
    const elapsedInWindow = now % this.config.windowSeconds;
    const elapsedRatio = elapsedInWindow / this.config.windowSeconds;

    return { currentWindow, previousWindow, elapsedRatio };
  }

  async check(identifier: string): Promise<Result<RateLimitResult, Error>> {
    const { currentWindow, previousWindow, elapsedRatio } = this.getWindowInfo();

    const currentKey = `${this.config.keyPrefix}:${identifier}:${currentWindow}`;
    const previousKey = `${this.config.keyPrefix}:${identifier}:${previousWindow}`;

    try {
      const tx = this.redis.multi();
      tx.get(previousKey);
      tx.incr(currentKey);
      tx.expire(currentKey, this.config.windowSeconds * 2);

      const results = await tx.exec();
      if (!results) return Err(new Error("redis pipeline returned null"));

      const [previousCountError, previousCount] = results[0];
      const [currentCountError, currentCount] = results[1];

      if (previousCountError) return Err(previousCountError);
      if (currentCountError) return Err(currentCountError);

      const ppreviousCount = Number.parseInt((previousCount as string) || "0", 10);
      const pcurrentCount = currentCount as number;

      // Weighted estimate
      const previousWeight = 1 - elapsedRatio;
      const estimate = ppreviousCount * previousWeight + pcurrentCount;

      const allowed = estimate <= this.config.maxRequests;
      const remaining = Math.max(0, Math.floor(this.config.maxRequests - estimate));

      const resetAt = (currentWindow + 1) * this.config.windowSeconds;

      // If denied, undo the increment so we don't inflate the count 
      if (!allowed) {
        await this.redis.decr(currentKey);
      }

      return Ok({
        allowed,
        limit: this.config.maxRequests,
        remaining,
        resetAt
      });
    } catch (err) {
      return Err(err instanceof Error ? err : new Error(String(err)));
    }
  }
}


/*    Lua script implementation for SWC
 * 
 *    -- KEYS[1] = current window key
 *    -- KEYS[2] = previous window key
 *
 *    -- ARGV[1] = elapsedRatio
 *    -- ARGV[2] = maxRequests
 *    -- ARGV[3] = windowSeconds
 *
 *    const script = `local currentKey = KEYS[1]
 *    local previousKey = KEYS[2]
 *
 *    local elapsedRatio = tonumber(ARGV[1])
 *    local maxRequests = tonumber(ARGV[2])
 *    local windowSeconds = tonumber(ARGV[3])
 *
 *    local currentCount = tonumber(redis.call("GET", currentKey) or "0")
 *    local previousCount = tonumber(redis.call("GET", previousKey) or "0")
 *
 *    local previousWeight = 1 - elapsedRatio
 *    local estimateBefore = previousCount * previousWeight + currentCount
 *
 *    -- Including the incoming request when deciding whether to allow it
 *    local estimateAfter = estimateBefore + 1
 *    local allowed = 0
 *    local remaining
 *
 *    if estimateAfter <= maxRequests then
 *      local newCount = redis.call("INCR", currentKey)
 *
 *      if newCount == 1 then
 *        redis.call("EXPIRE", currentKey, windowSeconds * 2)
 *      end
 *
 *      allowed = 1
 *      remaining = math.max(0, math.floor(maxRequests - estimateAfter))
 *
 *    else
 *      remaining = math.max(0, math.floor(maxRequests - estimateBefore))
 *    end
 *
 *    return { allowed, remaining }`;
 *
 *   async check(identifier: string): Promise<Result<RateLimitResult, Error>> {
 *      const {currentWindow, previousWindow, elapsedRatio} = this.getWindowInfo();
 *
 *      const currentKey = `${this.config.keyPrefix}:${identifier}:${currentWindow}`;
 *      const previousKey = `${this.config.keyPrefix}:${identifier}:${previousWindow}`;
 *
 *      const resetAt = (currentWindow + 1) * this.config.windowSeconds;
 *
 *      try {
 *        const result = await this.redis.eval(
 *          script,
 *          2,
 *          currentKey,
 *          previousKey,
 *          elapsedRatio,
 *          this.config.maxRequests,
 *          this.config.windowSeconds,
 *        ) as [number, number];
 *
 *        const [allowed, remaining] = result;
 *
 *        return Ok({
 *          allowed: allowed === 1,
 *          limit: this.config.maxRequests,
 *          remaining,
 *          resetAt
 *        });
 *      } catch (err) {
 *        return Err(err instanceof Error ? err : new Error(String(err)));
 *      }
 *   }
 *
 */
