import { IIdempotencyRepository } from '../../core/interfaces/IIdempotencyRepository';

export interface WithIdempotencyOptions<T> {
    key: string;
    ownerId: string;
    ttlSeconds?: number;
    releaseOnError?: boolean;
    onDuplicate?: () => Promise<T | void> | T | void;
}

/**
 * Wraps an asynchronous action with idempotency protection.
 * If the key has already been claimed, execution is skipped (or onDuplicate is called).
 * If execution throws an error and releaseOnError is true (default true), the claim is released
 * so that consumer retries can safely re-process.
 */
export async function withIdempotency<T>(
    idempotencyRepo: IIdempotencyRepository,
    options: WithIdempotencyOptions<T>,
    action: () => Promise<T>
): Promise<T | void> {
    const { key, ownerId, ttlSeconds, releaseOnError = true, onDuplicate } = options;

    const claimed = await idempotencyRepo.claim(key, ownerId, ttlSeconds);
    if (!claimed) {
        if (onDuplicate) {
            return onDuplicate();
        }
        return;
    }

    try {
        return await action();
    } catch (error) {
        if (releaseOnError) {
            try {
                await idempotencyRepo.release(key, ownerId);
            } catch {
                // Ignore release errors to preserve original exception
            }
        }
        throw error;
    }
}
