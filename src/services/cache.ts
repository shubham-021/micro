import type Redis from "ioredis";
import { env } from "@/config/env";
import { type Result, Ok, Err } from "@/lib/result";
import { type CacheError, createCacheError } from "@/lib/error";
import { L1Cache } from "./l1-cache";

/*
*   Suppose you have:
*   {
*       d: "Some API response",
*       f: Date.now() + 60_000
*   }
* 
*   This means: "Cache this response, and consider it fresh for the next 60 seconds"
*/
interface CachedEntry {
    d: unknown; // the cached data
    f: number; // timestamp until which it is considered fresh
}

/*  Read-through Caching:
*   
*   The application asks the cache for the data, and if the cache doesn't have it, the cache itself fetches
*   the data from the underlying database/ source, stores it, and returns it to the application.
* 
*   Read-through describes who handles the cache miss:
*    - Normal Caching: web server handles the miss -> DB -> Cache
*    - Read-through caching: cache layer handles the miss -> DB -> Cache   
* 
*   In read-through caching, the `cache itself accesses` the DB when it gets a cache miss, fetches the data, updates
*   the cache, and returns the data to the server.
* 
*   Normally, when the server gets a cache miss, the `server accesses` the DB, fetches the data, updates the cache, 
*   and then sends the data in the response to the client
* 
*/

/*  ReadThroughOptions:
*
*   Motive behind stale data: "Sometimes getting slightly old data is better than making the user wait for fresh data"
*   
*   This file has two periods: ttl and staleTtl | <-----ttl-----><---staleTtl--->
*   The redis entry itself survies for: totalTtl = ttl + staleTtl;
* 
*   skipL1; skipL2
*   They exist because this cache has two cache levels, and sometimes we want to deliberately bypass on them
*   L1 -> in-process memory
*   L2 -> Redis
* 
*   Why skip L1(in-process memory) ?
*   A practical reason is that L1 is local to the current application instance, while L2 Redis is shared. So we might delibaretly
*   want to bypass the local copy and check the shared cache.
* 
*   Why skip L2(Redis) ?
*   This can be useful when we deliberately want the sauce(iykyk) of truth rather than an existing Redis value
* 
*/

/*  WriteOptions
*   
*   It is the configuration for cache-writing operations. It contains only:
*   ttlSeconds: How long the newly written value should be considered fresh
*   staleTtlSeconds: How much additional time the value should remain in Redis after becoming stale.
* 
*/

/*  INVALIDATION_CHANNEL
*
*   This is a Redis PubSub channel name. The important point is that this cache can exist in mutiple application instances.
*   Suppose:
*       Application Instance A
*           |--- L1 cache A
*           |--- Redis L2
* 
*       Application Instance B
*           |--- L1 cache B
*           |--- Redis L2
* 
*   L2 is shared Redis, but L1 is local to each application process. Therefore, suppose instance A does:
*   `cache.delete("user:123")` : 
*       It removes its own L1: L1 A -> user:123 (deleted)
*       and Redis: Redis -> user:123 (deleted)
*   
*   but instance B might still have: L1 B -> user:123
*   So B could continue returning the old value from its local L1. That's why the file has Redis PubSub invalidation.
*   
*   Instance A publishes:
*       channel: cache:invalidate
*       message: {"key":"user:123"}
* 
*   Other instances subscribed to this channel receive the message and remove their local L1 copy
*/

/*  InvalidationMessage
*
*   This defines the shape of the message sent through cache:invalidate
*   There are two possible kinds of invalidation:
*   1. Invalidate one key:
*       {
*           key: "user:123"
*       }
* 
*   2. Invalidate a prefix:
*       {
*           prefix: "user:"
*       }
*/

// Options for get or set (read-through)
export interface ReadThroughOptions {
    ttlSecond?: number;
    staleTtlSeconds?: number;
    skipL1?: boolean;
    skipL2?: boolean;
}

// Options for write operations
export interface WriteOptions {
    ttlSeconds?: number;
    staleTtlSeconds?: number;
}

const INVALIDATION_CHANNEL = "cache:invalidate";

interface InvalidationMessage {
    key?: string;
    prefix?: string;
}

/*  Why inFlight ?
*
*   inFlight is used for `stampede protection`. It keeps track of cache keys whose data is currently being loaded from the source.
*   
*   Need ?
*   Suppose 100 requests arrive at almost the same time for: `user:123` and user:123 isn't in L1 or L2.
*   
*   Without inFlight:
*   Request 1 -> cache miss -> database
*   Request 2 -> cache miss -> database
*   Request 3 -> cache miss -> database
*   Request 4 -> cache miss -> database
*   ...
* 
*   We could end up making 100 database req for the exact same data.
* 
*   With inFlight:
*   Req 1 -> cache miss -> start loader() -> Promise -> inFlight["user:123"]
*   Req 2 -> cache miss -> inFlight has `user:123` -> wait for same promise
*   ...
* 
*   inFlight is not a cache. It doesnt store the actual result for future requests. It only temporarily stores:
*   "A req for this key is currently being loaded; here's the promise representing that"
*/

export class CacheService {
    private readonly l1: L1Cache;
    private readonly redis: Redis;
    private readonly pubRedis: Redis;
    private readonly subRedis: Redis;
    private readonly prefix: string;
    private readonly defaultTtl: number;
    private readonly defaultStaleTtl: number;

    private readonly inFlight: Map<string, Promise<unknown>>;

    constructor(cacheRedis: Redis, pubsubRedis: Redis) {
        this.l1 = new L1Cache(env.L1_CACHE_MAX_ENTRIES, env.L1_CACHE_TTL_SECONDS);
        this.redis = cacheRedis;
        this.prefix = env.CACHE_KEY_PREFIX;
        this.defaultTtl = env.CACHE_DEFAULT_TTL_SECONDS;
        this.defaultStaleTtl = env.CACHE_STALE_TTL_SECONDS;
        this.inFlight = new Map();

        this.subRedis = pubsubRedis.duplicate();
        this.pubRedis = pubsubRedis;

        this.setupInvalidationListener();
    }

    private fullKey(key: string): string {
        return `${this.prefix}:${key}`;
    }

    private setupInvalidationListener(): void {
        this.subRedis.subscribe(INVALIDATION_CHANNEL).catch((err: Error) => {
            console.error("cache failed to subscribe to invalidation channel: ", err.message);
        })

        this.subRedis.on("message", (channel: string, message: string) => {
            if (channel !== INVALIDATION_CHANNEL) return;

            try {
                const parsed: InvalidationMessage = JSON.parse(message);
                if (parsed.key) {
                    this.l1.delete(parsed.key);
                } else if (parsed.prefix) {
                    this.l1.deleteByPrefix(parsed.prefix);
                }
            } catch {
                // --------------
            }
        })
    }

    private async publishInvalidation(msg: InvalidationMessage): Promise<void> {
        try {
            await this.pubRedis.publish(INVALIDATION_CHANNEL, JSON.stringify(msg));
        } catch {
            // ---------------
        }
    }

    /* L2(Redis) operations */
    /* Every redis call returns Result. If redis is down, we return CACHE_UNAVAILABLE instead of caching */

    private async l2Get(key: string): Promise<Result<CachedEntry | null, CacheError>> {
        try {
            const raw = await this.redis.get(this.fullKey(key));
            if (raw === null) return Ok(null);

            const parsed: CachedEntry = JSON.parse(raw);
            return Ok(parsed);
        } catch (err) {
            return Err(createCacheError("CACHE_UNAVAILABLE", `L2 get failed for ${key}`, err));
        }
    }

    private async l2Set(key: string, entry: CachedEntry, totalTtlSeconds: number): Promise<Result<void, CacheError>> {
        try {
            const serialized = JSON.stringify(entry);
            await this.redis.set(this.fullKey(key), serialized, "EX", totalTtlSeconds);
            return Ok(undefined);
        } catch (err) {
            return Err(createCacheError("CACHE_WRITE_FAILED", `L2 set failed for ${key}`, err));
        }
    }

    private async l2Delete(key: string): Promise<Result<void, CacheError>> {
        try {
            await this.redis.del(this.fullKey(key));
            return Ok(undefined);
        } catch (err) {
            return Err(createCacheError("CACHE_UNAVAILABLE", `L2 delete failed for ${key}`, err));
        }
    }

    /* Core: get */
    /* Multi-tier read L1 -> L2 -> null */
    /* Does not call a loader on miss. Use getOrSet for that */

    async get<T>(key: string): Promise<Result<T | null, CacheError>> {
        // L1 check
        const l1Hit = this.l1.get<T>(key);
        if (l1Hit !== undefined) return Ok(l1Hit);

        // L2 check
        const l2Result = await this.l2Get(key);
        if (!l2Result.ok) return l2Result;

        const entry = l2Result.value;
        if (entry === null) return Ok(null);

        // Populate L1 from L2 hit
        this.l1.set(key, entry.d);

        return Ok(entry.d as T);
    }

    /* Core: set */
    /* Multi tier write: writes to both l1 and l2 */

    async set<T>(key: string, value: T, opts?: WriteOptions): Promise<Result<void, CacheError>> {
        const ttl = opts?.ttlSeconds ?? this.defaultTtl;
        const staleTtl = opts?.staleTtlSeconds ?? this.defaultStaleTtl;
        const totalTtl = ttl + staleTtl;

        const enrty: CachedEntry = {
            d: value,
            f: Date.now() + ttl * 1000
        };

        this.l1.set(key, value, ttl);
        return this.l2Set(key, enrty, totalTtl);
    }

    /* Core: delete */
    /* Deletes from L1, L2 and publishes invalidation so other instances clear their L1 */

    async delete(key: string): Promise<Result<void, CacheError>> {
        this.l1.delete(key);
        const result = await this.l2Delete(key);
        await this.publishInvalidation({ key });
        return result;
    }

    async deleteByPrefix(domainPrefix: string): Promise<Result<number, CacheError>> {
        // Clear l1
        const l1Count = this.l1.deleteByPrefix(domainPrefix);

        // Clear l2 using SCAN
        try {
            let cursor = "0";
            let totalDeleted = l1Count;
            const pattern = this.fullKey(domainPrefix) + "*";

            do {
                const [nextCursor, keys] = await this.redis.scan(cursor, "MATCH", pattern, "COUNT", 100);
                cursor = nextCursor;

                if (keys.length > 0) {
                    await this.redis.del(...keys);
                    totalDeleted += keys.length;
                }
            } while (cursor !== "0");

            await this.publishInvalidation({ prefix: domainPrefix });
            return Ok(totalDeleted);
        } catch (err) {
            return Err(createCacheError("CACHE_UNAVAILABLE", "deleteByPrefix failed", err));
        }
    }

    /*  Read-Through with Stampede Protection + SWR(Stale-While-Revalidate) */
    /*  SWR: If cached data is stale, return the old value while fetching fresh data in background */

    /*
    *   1. Check L1
    *   2. Check L2
    *       - If fresh: return
    *       - If stale: return stale + trigger background refresh
    *   3. On miss: inFlight check, fetch from source, populate L1 + L2
    *   4. If redis is down: call loader directly (fallback/ degradation)    
    */

    async getOrSet<T>(key: string, loader: () => Promise<T>, opts?: ReadThroughOptions): Promise<Result<T, CacheError>> {
        const ttl = opts?.ttlSecond ?? this.defaultTtl;
        const staleTtl = opts?.staleTtlSeconds ?? this.defaultStaleTtl;

        // L1
        if (!opts?.skipL1) {
            const l1Hit = this.l1.get<T>(key);
            if (l1Hit !== undefined) return Ok(l1Hit);
        }

        // L2
        if (!opts?.skipL2) {
            const l2Result = await this.l2Get(key);

            if (l2Result.ok && l2Result.value !== null) {
                const entry = l2Result.value;
                const data = entry.d as T;
                const now = Date.now();

                if (now < entry.f) {
                    // fresh hit: populate L1 and return
                    this.l1.set(key, data, ttl);
                    return Ok(data);
                }

                // stale hit: return stale data, refresh in background
                this.l1.set(key, data, staleTtl);
                this.backgroundRefresh(key, loader, ttl, staleTtl);
                return Ok(data);
            }
        }

        return this.loadWithSingleFlight<T>(key, loader, ttl, staleTtl);
    }

    private async loadWithSingleFlight<T>(key: string, loader: () => Promise<T>, ttlSeconds: number, staleTtlSeconds: number): Promise<Result<T, CacheError>> {
        const existing = this.inFlight.get(key);
        if (existing) {
            try {
                const value = (await existing) as T;
                return Ok(value);
            } catch (err) {
                return Err(createCacheError("CACHE_MISS", `loader failed for ${key}`, err));
            }
        }

        const promise = loader();
        this.inFlight.set(key, promise);

        try {
            const value = await promise;

            // populate cache ( fire and forget; if redis is down, we still return the value)
            this.set(key, value, { ttlSeconds, staleTtlSeconds }).catch(() => { });
            return Ok(value);
        } catch (err) {
            return Err(createCacheError("CACHE_MISS", `loader failed for ${key}`, err));
        } finally {
            this.inFlight.delete(key);
        }
    }

    private backgroundRefresh<T>(key: string, loader: () => Promise<T>, ttlSeconds: number, staleTtlSeconds: number): void {
        if (this.inFlight.has(key)) return;

        const promise = loader();
        this.inFlight.set(key, promise);

        promise.then((value) => {
            this.set(key, value, { ttlSeconds, staleTtlSeconds }).catch(() => { });
        }).catch((err) => {
            console.error(`cache background refresh failed for ${key}: `, err);
        }).finally(() => {
            this.inFlight.delete(key);
        })
    }
}