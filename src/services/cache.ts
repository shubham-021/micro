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
    ttlSeconds?: number;
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

        const entry: CachedEntry = {
            d: value,
            f: Date.now() + ttl * 1000
        };

        this.l1.set(key, value, ttl);
        return this.l2Set(key, entry, totalTtl);
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
        const ttl = opts?.ttlSeconds ?? this.defaultTtl;
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

    /*  Write-Through 
    *
    *   Whenever your application writes data, the write goes to the cache, and the cache synchronously writes the data to the database before 
    *   considering the operation complete.
    * 
    *   Client -> Application Server -> Cache -> Database
    * 
    *   Comparing it with cache-aside:
    *   Cache-Aside: The application is responsible for both cache and DB, server -> DB, server -> Cache, the application has to coordinate the two
    *   Write-through: The cache becomes the part of the write path, the cache handles the DB write.
    * 
    *   Advantages:
    *       1. Reduces stale data in the cache
    *           Since the cache is updated as part of every write, it is less likely to contain an old version of the data
    *       2. Fast subsequent reads
    *           After a successful write, the updated data is already present in the cache, therefore, a later read can be served without accessing the database
    *       3. Simpler application logic
    *           The application does not have to separately perform: 1. Database update 2. Cache update
    *       4. Good read performance
    *           It works particularly well when the same data is written occasinally but read many times. The cache remain populated with the lates written value.
    *       5. Better consistency between cache and database
    *           Compared with approaches where the cache is updated asynchronously, write-through provides a stronger guarantee that a successful write has also been
    *           persisted to the database
    * 
    *   Disadvantages:
    *       1.Higher write latency
    *           The application must wait for the database write before completing the request.
    *       2. Does not reduce database write load
    *           Write-through does not reduce database write load because every write is synchronously propagated to the database (Not a problem, rather a simple tradeoff)
    *       3. Not ideal for write-heavy system
    *           If an application performs a very large number of write and relatively few reads, mainting the cache on every write may provide little benefit
    *       4. Cache and Database can still become inconsistent
    *           Write-through reduces one source of inconsistency, but it does not completely solve distributed-system problem. Failures, concurrent writes, direct
    *           database modifications, or multiple caching layers can still create inconsistencies
    *       5. More dependency on the cache during writes
    *           Because the cache is part of the write path, problem s in the caching layer can affect write operation depending on the implementation.
    * 
    * 
    *   When to use ?
    *       - The data is read much more frequently than it is written
    *       - The data should be available in the cache immediately after a write
    *       - Stale data is undesirable
    *       - The application can tolerate the additional write latency
    *       - Database writes do not need to be heavily optimized or batched
    *       - The same data is likely to be read soon after being written
    * 
    *   When to avoid ?
    *       - The system is extremely write-heavy
    *       - Very low write latency is more important than immediate database presistence
    *       - The database cannot handle the write volume efficiently
    *       - Writes can safely be processed asynchronously
    *       - The system benefits from batching multiple writes together
    *       - The cached data is rarely read, so maintaing it on every write provides a little benefit
    * 
    * 
    *   What does it mean reducing Database write load ?
    *       Write-back caching can reduce the immediate datbase workload, not necessarily the total amount of data the must eventually be persisted. It can temporarily
    *       store updates in the cache and later write them to the db in batches or combine multiple updates when the intermediate states are not important.
    * 
    *       For example, if a value changes from 100 -> 101 -> 102 -> 103 and only the final value matters, a write-back system may persist only 103 instead of performing
    *       four separate database writes.
    * 
    * 
    *   How does batching operations reduces DB costs ?
    *       A write can involve:
    *           1. Network Overhead: The application has to communicate with the database
    *           2. Query parsing/ plannig: The database may need to parse and plan each statement
    *           3. Transaction overhead: Starting/ committing transactions has a cost
    *           4. WAL/ redo logging: Databases such as PSQL write changes to a write-ahead log for durability
    *           5. Disk I/O / fsync: Durable commits may require flushing data/ logs to storage
    *           6. Index Updates: If you update a row, relevant indexes may also need to be updated
    *           7. Locking and concurrency management: The database has to coordinate concurrent transactions
    *           8. Buffer/ cache management: Pages may need to be loaded, modified, and eventually flushed
    * 
    *   So,
    *   One DB operation: Network -> Parse/ plan -> Transaction -> Modify data -> Update indexes -> Write WAL -> Commit/ Sync
    *   
    *   Now if you do multiple writes one by one, you might perform:
    *       INSERT 1 -> transaction -> commit
    *       INSERT 2 -> transaction -> commit
    *       ....
    *       INSERT 1000 -> transaction -> commit
    * 
    *   You are paying the txn/ commit overhead 1000 times
    * 
    *   With batching, you can do:
    *       BEGIN
    *       INSERT 1
    *       INSERT 2
    *       ....
    *       INSERT 1000
    *       COMMIT
    * 
    *   Now you still write 1000 records, but you may perform the txn/commit work only once
    * 
    * 
    *   Think of the cost as: Total DB cost = per-operation overhead + cost of processing the actual data
    * 
    *   There's another optimization: Sometimes batching can reduce the amount of actual db work, not just overhead
    *   for example:
    *       UPDATE counter
    *       SET value = value + 1
    *   
    *   performed 1000 times, if the application only cares about the final counter, you could accumulate: 1000 increments and perform:
    *       UPDATE counter
    *       SET value = value + 1000
    */

    async writeThrough<T>(key: string, value: T, writer: () => Promise<void>, opts?: WriteOptions): Promise<Result<void, CacheError>> {
        try {
            await writer();
        } catch (err) {
            return Err(createCacheError("CACHE_WRITE_FAILED", "source write failed", err));
        }

        const cacheResult = await this.set(key, value, opts);
        if (!cacheResult.ok) {
            console.error(`cache write-through update failed for ${key}`);
        }

        return Ok(undefined);
    }

    /*  Write-Behind (Write-Back) 
    *
    *   It is a technique where a write is first made to the cache, and the database is updated asynchronously at a later time.
    *   
    *   When the application receives a write:
    *       1. The application writes the new value to the cache
    *       2. The cache immediately acknowledges the write
    *       3. The application can return the response to the client
    *       4. The cache later writes the change to the database.
    *       5.  Multiple changes may be combined or written in batches
    *   
    *   So the database is not immediately updated when the client receives a successful response
    *   
    *   Advantages:
    *       - Lower write latency
    *       - Can reduce immediate database workload
    *       - Can handle high write traffic
    *       - Can combine redundant updates
    * 
    *   Disadvantages:
    *       - Risk of data loss
    *       - DB can temporarily contain stale data
    *       - More complex failure handling
    *       - More memory/ storage requirements
    * 
    *   When to use ?
    *       - Very low write latency is important
    *       - The system receives a large volume of writes
    *       - The database can tolerate delayed writes
    *       - Writes can be batched or combined
    *       - Temporary inconsistency between cache and database is acceptable
    *       - Losing a write can be prevented through a reliable presistence mechanism
    * 
    *   When to avoid ?
    *       - Every write must be immediately persisted
    *       - Losing even one write is unacceptable
    *       - The database must always contain the latest state.
    *       - Strong consistency is required
    *       - The system cannot tolerate complex recovery logic
    * 
    *   For asychronous database updates with reliable recovery, we usually use a durable message queue/ event log between the application and the db
    *   The common pattern is:
    *       Application -> Cache -> Durable Queue/ Log -> Worker -> DB
    * 
    *   Why a queue ?
    *       The queue stores the pending write until a worker successfully writes it to the database.
    * 
    *   If DB is temporarily unavailable ?
    *       Queue -> Worker -> DB (fails) -> retry later
    * 
    *   The write remains in the queue, so it isnt lost simply because the db was unavailable
    * 
    *   Recovery Mechanisms:
    *       A reliable implementation generally uses:
    *       - Durable queue/ log - Keeps pending writes safe
    *       - Acknowledgements - remove/ mark a message processed only after successful DB persistence
    *       - Retries - retry failed database writes
    *       - Dead-letter queue (DLQ) - move repeatedly failing messages aside for investigatio/ reprocessing
    *       - Idempotancey - ensure retrying the same write doesn't create duplicate effects
    *       - Ordering/ versioning - important when multiple updates to the same record must be applied in order
    */

    async writeBehind<T>(key: string, value: T, writer: () => Promise<void>, opts?: WriteOptions): Promise<Result<void, CacheError>> {
        const cacheResult = await this.set(key, value, opts);
        writer().catch((err) => {
            console.error(`cache write-behind flush failed for ${key}: `, err);
        })

        // queue implementation (to-be done)

        return cacheResult;
    }

    /*  Write-Around 
    *
    *   Caching technique where write operations go directly to the database and do not update the cache
    *   
    *   Write: Application -> Database
    *   Read: Application -> Cache -> miss -> Database -> Cache
    * 
    *   "Write bypass the cache. The cache is populated only when the data is sunsequently read"
    * 
    *   Advantages:
    *       - Prevents cache pollution
    *       - Reduces unnecessary cache writes
    *       - Good for write-heavy workloads
    *       - Simple Write path
    * 
    *   Disadvantes:
    *       - First read after a write is a cache miss
    *       - Database can temporarily be newer than the cache
    * 
    *   When to use ?
    *       - Data is written frequently but read infrequently
    *       - You dont want every write to populate the cache
    *       - Cache space is limited
    *       - You want to avoid cache pollution
    *       - A cache miss on the first read after a write is acceptable
    *       - The database is the source of truth
    * 
    *   Bypassing cache, can leave cache with outdated data, thats why write-around requires cache invalidation
    *   The write operation is typically: Application -> (DB <- update) -> invalidate cache
    */

    async writeAround(key: string, writer: () => Promise<void>): Promise<Result<void, CacheError>> {
        try {
            await writer();
        } catch (err) {
            return Err(createCacheError("CACHE_WRITE_FAILED", "source write failed", err));
        }

        return this.delete(key);
    }

    /*  Refresh-Ahead 
    *   
    *   Caching technique in which the system refreshes a cache entry before it expires, rather than waiting for the entry to expire and cause a cache miss
    *   The main goal is:
    *       "Keep frequently accessed data in the cache so that users rarely experience a cache miss"
    * 
    *   Advantages:
    *       - Reduces cache-miss latency    
    *       - Good for frequently accessed data
    *       - Protects the db from sudden read spikes
    *       - Provides predictable read performance
    * 
    *   Disadvantages:
    *       - Unnecessary database work
    *       - More complexity
    *       - Can increase db load
    * 
    *   When to use it ?
    *       - Data is very frequently accessed
    *       - A cache miss would be expensive or slow
    *       - The data changes relatively infrequently
    *       - You can tolerate slightly stale data during refresh
    *       - The cost of refreshing is lower than the cost of allowing frequent cache misses
    * 
    *   When to avoid ?
    *       - The data is rarely accessed
    *       - DB queries are expensive and unnecessary refreshes are costly
    *       - The data changes very frequently
    *       - It is acceptable for the first request after expiration to experience a cache miss
    *       - You have too many cache entries to refresh efficiently
    */

    async getWithRefreshAhead<T>(key: string, loader: () => Promise<T>, opts?: ReadThroughOptions & { refreshThreshold?: number }): Promise<Result<T, CacheError>> {
        const ttl = opts?.ttlSeconds ?? this.defaultTtl;
        const staleTtl = opts?.staleTtlSeconds ?? this.defaultStaleTtl;
        const threshold = opts?.refreshThreshold ?? 0.2;

        // L1
        const l1Hit = this.l1.get<T>(key);
        if (l1Hit !== undefined) return Ok(l1Hit);

        // L2
        const l2Result = await this.l2Get(key);
        if (l2Result.ok && l2Result.value !== null) {
            const entry = l2Result.value;
            const data = entry.d as T;
            const now = Date.now();
            const ttlMs = ttl * 1000;
            const freshUntil = entry.f;

            const remaining = freshUntil - now;

            this.l1.set(key, data, ttl);

            if (remaining > 0 && remaining < ttlMs * threshold) {
                this.backgroundRefresh(key, loader, ttl, staleTtl);
            }

            if (now < freshUntil) {
                return Ok(data);
            }

            this.backgroundRefresh(key, loader, ttl, staleTtl);
            return Ok(data);
        }

        return this.loadWithSingleFlight<T>(key, loader, ttl, staleTtl);
    }

    /* Bulk: getMany */

    async getMany<T>(keys: string[]): Promise<Result<Map<string, T | null>, CacheError>> {
        const results = new Map<string, T | null>();
        const l2Keys: string[] = [];
        const l2KeysIndexMap: string[] = [];

        for (const key of keys) {
            const l1Hit = this.l1.get<T>(key);
            if (l1Hit !== undefined) {
                results.set(key, l1Hit);
            } else {
                l2Keys.push(this.fullKey(key));
                l2KeysIndexMap.push(key);
            }
        }

        if (l2Keys.length === 0) return Ok(results);

        try {
            const rawValues = await this.redis.mget(...l2Keys);

            for (let i = 0; i < rawValues.length; i++) {
                const originalKey = l2KeysIndexMap[i];
                const raw = rawValues[i];

                if (raw === null) {
                    results.set(originalKey, null);
                    continue;
                }

                try {
                    const entry: CachedEntry = JSON.parse(raw);
                    const data = entry.d as T;
                    results.set(originalKey, data);
                    this.l1.set(originalKey, data);
                } catch {
                    results.set(originalKey, null);
                }
            }

            return Ok(results);
        } catch (err) {
            return Err(createCacheError("CACHE_UNAVAILABLE", "mget failed", err));
        }
    }

    /* Cache Warming */

    async warm(entries: Array<{ key: string; loader: () => Promise<unknown>; ttlSeconds?: number }>, concurrency: number = 5): Promise<{ succeeded: number; failed: number }> {
        let succeeded = 0;
        let failed = 0;

        for (let i = 0; i < entries.length; i += concurrency) {
            const batch = entries.slice(i, i + concurrency);
            const results = await Promise.allSettled(
                batch.map(async (entry) => {
                    const value = await entry.loader();
                    await this.set(entry.key, value, { ttlSeconds: entry.ttlSeconds });
                })
            );

            for (const result of results) {
                if (result.status === "fulfilled") succeeded++;
                else failed++;
            }
        }

        console.log(`cache warming complete: ${succeeded} succeeded, ${failed} failed`);
        return { succeeded, failed };
    }

    /* Lifecycle */

    async destroy(): Promise<void> {
        this.l1.destroy();
        this.inFlight.clear();

        try {
            await this.subRedis.unsubscribe(INVALIDATION_CHANNEL);
            await this.subRedis.quit();
        } catch {

        }
    }
}