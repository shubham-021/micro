import Redis from "ioredis";
import { env } from "@/config/env";

export interface RedisClient {
    readonly cache: Redis;
    readonly queue: Redis;
    readonly pubsub: Redis;
}

function createClient(url: string, name: string): Redis {
    const client = new Redis(url, {
        maxRetriesPerRequest: 3,
        retryStrategy(times: number): number | null {
            if (times > 10) {
                console.error(`[redis:${name}] max reconnection attempts reached`);
                return null;
            }

            return Math.min(times * 200, 5000);
        },
        commandTimeout: 2000,
        enableReadyCheck: true,
        lazyConnect: true
    });

    client.on("connect", () => {
        console.log(`[redis:${name}] connected`);
    });

    client.on("ready", () => {
        console.log(`[redis:${name}] ready`);
    });

    client.on("error", (err: Error) => {
        console.error(`[redis:${name}] error: `, err.message);
    });

    client.on("close", () => {
        console.error(`[redis:${name}] connection closed`);
    });

    return client;
}

let clients: RedisClient | null = null;

export async function connectRedis(): Promise<RedisClient> {
    if (clients) return clients;

    const cache = createClient(env.REDIS_CACHE_URL, "cache");
    const queue = createClient(env.REDIS_QUEUE_URL, "queue");
    const pubsub = createClient(env.REDIS_PUBSUB_URL, "pubsub");

    await Promise.all([cache.connect(), queue.connect(), pubsub.connect()]);

    clients = { cache, queue, pubsub };
    return clients;
}

export function getRedisClients(): RedisClient {
    if (!clients) {
        throw new Error("Redis client not initialized. Call connectRedis() first");
    }

    return clients;
}

export async function disconnectRedis(): Promise<void> {
    if (!clients) return;

    await Promise.all([clients.cache.quit(), clients.queue.quit(), clients.pubsub.quit()]);

    clients = null;
}