export interface IIdempotencyRepository {
    /**
     * Attempts to claim an idempotency key with an owner/operation ID.
     * @param key The idempotency key (e.g. event ID, request ID)
     * @param ownerId The owner or execution ID (e.g. notification ID, message ID)
     * @param ttlSeconds Optional time-to-live in seconds (useful for Redis/TTL storage)
     * @returns true if claimed successfully, false if already claimed / duplicate
     */
    claim(key: string, ownerId: string, ttlSeconds?: number): Promise<boolean>;

    /**
     * Releases the claim on the idempotency key (e.g. on execution failure if retry is allowed).
     * @param key The idempotency key
     * @param ownerId The owner ID
     */
    release(key: string, ownerId: string): Promise<void>;

    /**
     * Checks if a key has already been claimed.
     * @param key The idempotency key
     */
    isClaimed?(key: string): Promise<boolean>;
}
