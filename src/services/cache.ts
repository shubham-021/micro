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
    f: unknown; // timestamp until which it is considered fresh
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
    staleTtlSecond?: number;
}

const INVALIDATION_CHANNEL = "cache:invalidate";

interface InvalidationMessage {
    key?: string;
    prefix?: string;
}

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

        // this.set 
    }
}