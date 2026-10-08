import { SignJWT, jwtVerify, importPKCS8, importSPKI, errors as joseErrors } from "jose";
import { randomBytes } from "node:crypto";
import { type Result, Ok, Err } from "@/lib/result";

export type CryptoErrorKind =
    | "SIGN_FAILED"
    | "VERIFY_FAILED"
    | "TOKEN_EXPIRED"
    | "TOKEN_INVALID"
    | "KEY_LOAD_FAILED";

export interface CryptoError {
    readonly kind: CryptoErrorKind;
    readonly message: string;
    readonly cause?: unknown;
}

function createCryptoError(kind: CryptoErrorKind, message: string, cause?: unknown): CryptoError {
    return { kind, message, cause };
}

const ALG = "RS256";

let privateKey: Awaited<ReturnType<typeof importPKCS8>> | null = null;
let publicKey: Awaited<ReturnType<typeof importSPKI>> | null = null;

export async function loadKeyPair(privatePem: string, publicPem: string): Promise<Result<void, CryptoError>> {
    try {
        privateKey = await importPKCS8(privatePem, ALG);
        publicKey = await importSPKI(publicPem, ALG);
        return Ok(undefined);
    } catch (err) {
        return Err(createCryptoError("KEY_LOAD_FAILED", "failed to import key pair", err));
    }
}

export interface AccessTokenClaims {
    sub: string;
    jti: string;
    iat: number;
    exp: number;
}

export interface SignedAccessToken {
    token: string;
    jti: string;
    expiresAt: number;
}

export function generateRefreshToken(): string {
    return randomBytes(64).toString("hex");
}

export function generateTokenId(): string {
    return randomBytes(16).toString("hex");
}

export async function signAccessToken(userId: string, ttlSeconds: number): Promise<Result<SignedAccessToken, CryptoError>> {
    if (!privateKey) return Err(createCryptoError("KEY_LOAD_FAILED", "private key not loaded"));

    const jti = generateTokenId();
    const now = Math.floor(Date.now() / 1000);
    const expiresAt = now + ttlSeconds;

    try {
        const token = await new SignJWT({ jti })
            .setProtectedHeader({ alg: ALG })
            .setSubject(userId)
            .setIssuedAt(now)
            .setExpirationTime(expiresAt)
            .sign(privateKey);

        return Ok({ token, jti, expiresAt });
    } catch (err) {
        return Err(createCryptoError("SIGN_FAILED", "failed to sign access token", err));
    }
}

export async function verifyAccessToken(token: string): Promise<Result<AccessTokenClaims, CryptoError>> {
    if (!publicKey) return Err(createCryptoError("KEY_LOAD_FAILED", "public key not loaded"));

    try {
        const { payload } = await jwtVerify(token, publicKey, { algorithms: [ALG] });

        return Ok({
            sub: payload.sub as string,
            jti: payload.jti as string,
            iat: payload.iat as number,
            exp: payload.exp as number,
        });
    } catch (err) {
        if (err instanceof joseErrors.JWTExpired) {
            return Err(createCryptoError("TOKEN_EXPIRED", "access token has expired", err));
        }

        return Err(createCryptoError("TOKEN_INVALID", "invalid access token", err));
    }
}
