export type CacheErrorKind =
    | "CACHE_MISS"
    | "CACHE_UNAVAILABLE"
    | "CACHE_DESERIALIZATION_FAILED"
    | "CACHE_SERIALIZATION_FAILED"
    | "CACHE_LOCK_FAILED"
    | "CACHE_WRITE_FAILED";

export interface CacheError {
    readonly kind: CacheErrorKind;
    readonly message: string;
    readonly cause?: unknown;
}

export function createCacheError(kind: CacheErrorKind, message: string, cause?: unknown): CacheError {
    return { kind, message, cause };
}