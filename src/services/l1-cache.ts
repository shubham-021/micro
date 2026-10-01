/*
 * In-memory LRU cache with TTL expiration.
 * This is the L1 layer in the multi-tier cache. It serves hot keys in microseconds without a Redis round-trip.
 *
 * Eviction: LRU (least recently used) when maxEntries is exceeded.
 * Expiration: Per-entry TTL. Expired entries are cleaned up lazily on access and periodically via a sweep interval.
 */

interface L1Entry<T> {
    value: T;
    expiresAt: number;
}

export class L1Cache {
    private store: Map<string, L1Entry<unknown>>;
    private readonly maxEntries: number;
    private readonly defaultTtlMs: number;
    private sweepTimer: ReturnType<typeof setInterval> | null = null;

    constructor(maxEntries: number, defaultTtlSeconds: number) {
        this.store = new Map();
        this.maxEntries = maxEntries;
        this.defaultTtlMs = defaultTtlSeconds * 1000;

        // Periodic sweep: remove expired entries every 30 second, so they don't linger if never accessed again
        this.sweepTimer = setInterval(() => this.sweep(), 30_000);
    }

    /*  LRU
    *   Map puts the key-value pair to the end of the map's insertion order
    */
    get<T>(key: string): T | undefined {
        const entry = this.store.get(key);
        if (!entry) return undefined;

        this.store.delete(key);
        this.store.set(key, entry);
    }

    /*
    *   `.keys()` returns an iterator over the keys and `.next()` asks that iterator for its next item.
    *   Example:
    *   const  map = new Map([["a", 10], ["b", 20], ["c", 30]]);
    *   const iterator = map.keys();
    *   console.log(iterator.next());
    *   O/P: { value: "a", done: false }
    *   
    *   iterator.next() => { value: "b", done: false }
    *   iterator.next() => { value: "c", done: false }
    *   iterator.next() => { value: undefined, done: true }
    * 
    *   So, map.keys().next() means "Give me the first key from this Map's key iterator"
    *   Therefore, if you only want the first key:
    *   const firstKey = map.keys().next().value;
    */
    set<T>(key: string, value: T, ttlSeconds?: number): void {
        // if key exists, delete first to reset position
        this.store.delete(key);

        if (this.store.size >= this.maxEntries) {
            const oldest = this.store.keys().next();
            if (!oldest.done) {
                this.store.delete(oldest.value);
            }
        }

        const ttlMs = ttlSeconds ? ttlSeconds * 1000 : this.defaultTtlMs;
        this.store.set(key, { value, expiresAt: Date.now() + ttlMs });
    }

    delete(key: string): boolean {
        return this.store.delete(key);
    }

    deleteByPrefix(prefix: string): number {
        let count = 0;
        for (const key of this.store.keys()) {
            if (key.startsWith(prefix)) {
                this.store.delete(key);
                count++;
            }
        }

        return count;
    }

    clear(): void {
        this.store.clear();
    }

    /* 
    *   get size() is a getter. It lets you access a method like a property.
    *   Example: const m = new MyMap(); if myMap has a getter named size we can use it like: m.size
    *   
    *   We don't write: m.size() because size is being exposed as a property through getter.
    * 
    *   Note: A getter cannot accept arguments, and a setter must accept exactly one argument.
    */
    get size(): number {
        return this.store.size;
    }

    sweep(): void {
        const now = Date.now();
        for (const [key, entry] of this.store) {
            if (now > entry.expiresAt) {
                this.store.delete(key);
            }
        }
    }

    destroy(): void {
        if (this.sweepTimer) {
            clearInterval(this.sweepTimer);
            this.sweepTimer = null;
        }

        this.store.clear();
    }
}