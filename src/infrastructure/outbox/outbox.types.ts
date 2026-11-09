/**
 * One outbox row that this instance has claimed and must now publish.
 */
export interface ClaimedOutboxEvent {
    /** Event id. Also sent as `messageId` so consumers can deduplicate. */
    id: string;
    aggregateId: string;
    aggregateType?: string;
    /** Subject the event is published under, for example `order.status-changed.v1`. */
    eventType: string;
    payload: Record<string, unknown>;
    /** Number of failed publish attempts so far. Drives the backoff delay. */
    retryCount: number;
    /** When this instance's lease on the row ends. */
    claimedUntil: Date;
}

/**
 * Storage port of the outbox relay. Each service implements it against its own table or collection.
 *
 * Every method that changes a row takes `claimedBy` and must only touch the row while that instance
 * still holds an unexpired lease. Return `false` (or leave the id out of the renewed set) when the lease
 * was lost, so the relay does not report success for a row another instance may now own.
 */
export interface IOutboxStore {
    /**
     * Atomically claims up to `batchSize` rows that are due: not processed, not dead,
     * `nextAttemptAt <= now` and with no unexpired lease. Oldest first.
     */
    claimBatch(claimedBy: string, batchSize: number, leaseMs: number): Promise<ClaimedOutboxEvent[]>;

    /** Extends the lease of the given rows. Returns the ids that are still owned by `claimedBy`. */
    renewClaims(ids: string[], claimedBy: string, leaseMs: number): Promise<Set<string>>;

    /** Marks a row as published and clears its lease. Returns false when the lease was lost. */
    markPublished(id: string, claimedBy: string): Promise<boolean>;

    /**
     * Records a failed attempt: increments the retry count, stores the error, sets `nextAttemptAt`
     * and clears the lease. Returns false when the lease was lost.
     */
    markFailed(id: string, claimedBy: string, nextAttemptAt: Date, error: string): Promise<boolean>;

    /** Clears the lease on rows this instance claimed but did not finish (used on shutdown). */
    releaseClaims(ids: string[], claimedBy: string): Promise<number | void>;
}

export interface OutboxRelayConfig {
    pollIntervalMs: number;
    batchSize: number;
    leaseMs: number;
    /** Renew the leases when less than this much of the lease is left. */
    leaseRenewThresholdMs: number;
    retryBaseMs: number;
    retryMaxMs: number;
    /** How long shutdown waits for the running batch before releasing the remaining claims. */
    shutdownGraceMs: number;
}
