import { z } from "zod/v4";

const envSchema = z.object({
    NODE_ENV: z.enum(["development", "production", "test"]).default("development"),
    PORT: z.coerce.number().int().min(1).max(65535).default(3000),

    // Redis - Cache Session
    REDIS_CACHE_URL: z.url().default("redis://localhost:6379/0"),

    // Redis - Queue
    REDIS_QUEUE_URL: z.url().default("redis://localhost:6379/1"),

    // Redis - Pub/ Sub
    REDIS_PUBSUB_URL: z.url().default("redis://localhost:6379/2"),

    // Cache defaults
    CACHE_DEFAULT_TTL_SECONDS: z.coerce.number().int().min(1).default(300),
    CACHE_KEY_PREFIX: z.string().default("cache"),
    CACHE_STALE_TTL_SECONDS: z.coerce.number().int().min(0).default(60),

    // L1 (in-memory) cache
    L1_CACHE_MAX_ENTRIES: z.coerce.number().int().min(0).default(1000),
    L1_CACHE_TTL_SECONDS: z.coerce.number().int().min(1).default(60)
});

export type Env = z.infer<typeof envSchema>;

function loadEnv(): Env {
    const result = envSchema.safeParse(process.env);

    if (!result.success) {
        const formatted = z.prettifyError(result.error);
        console.error("Environment validation failed:\n", formatted);
        process.exit(1);
    }

    return Object.freeze(result.data);
}

export const env = loadEnv();


/* 
*   What is z.coerce ?
*   z.number() will invalidate any input other than number and the environment variables are received as strings.
*   So: PORT = 3000 ends up roughly as: process.env.PORT === "3000" <-- string.
*   z.coerce.number() tells zod: Before validating this value as a number, try to convert it into a number
*/

/* 
*   Why safeparse insteaf of parse in `const result = envSchema.safeParse(process.env)` ?
*   Zod provides both:
* 
*   parse():
*   const env = envSchema.parse(process.env);
* 
*   if validation fails, Zod throws an exception
* 
*   safeParse():
*   doesnt throw. It gives you a result describing whether parsing succeeded or failed. Zod's parsing
*   API exposes `safeParse` specifically for this non-throwing pattern.
*/


/*
*   What does Object.freeze() do ?
*   Object.freeze() makes that object's existing properties non-writable/ non-configurable and prevents
*   adding or removing properties. It returns the same object rather than making a copy.
*/

/*
*   `const result = envSchema.safeParse(process.env)` reads the process.env then if some env variables are
*   missing it adds them to the object since we have default value set in the schema, if default was not given then 
*   a missing variable would not be filled in. It would cause validation to fail, unless the field is optional.
* 
*   After safeParse, result only contains those variables that were defined in the schema
*/

/*
*   When safeParse fails, result.error contains a structured ZodError with detailed issue object. Zod's official doc
*   says z.prettifyError() converts that error into a human-readable string.
*/