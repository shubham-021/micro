import argon2 from "argon2";
import { type Result, Ok, Err } from "@/lib/result";

export type HashErrorKind = "HASH_FAILED" | "VERIFY_FAILED";

export interface HashError {
    readonly kind: HashErrorKind;
    readonly message: string;
    readonly cause?: unknown
}

function createHashError(kind: HashErrorKind, message: string, cause?: unknown): HashError {
    return { kind, message, cause };
}

const ARGON2_OPTIONS: argon2.HashOptions & { raw?: false } = {
    type: argon2.argon2id,
    timeCost: 3,
    memoryCost: 65536,
    parallelism: 4,
    hashLength: 32
};

export async function hashPassword(password: string): Promise<Result<string, HashError>> {
    try {
        const hash = await argon2.hash(password, ARGON2_OPTIONS);
        return Ok(hash);
    } catch (err) {
        return Err(createHashError("HASH_FAILED", "failed to hash password", err));
    }
}

export async function verifyPassword(hash: string, password: string): Promise<Result<boolean, HashError>> {
    try {
        const matches = await argon2.verify(hash, password);
        return Ok(matches);
    } catch (err) {
        return Err(createHashError("VERIFY_FAILED", "failed to verify password", err));
    }
}

export function needsRehash(hash: string): boolean {
    const currentParams = `m=${ARGON2_OPTIONS.memoryCost},t=${ARGON2_OPTIONS.timeCost},p=${ARGON2_OPTIONS.parallelism}`;
    return !hash.includes(currentParams);
}