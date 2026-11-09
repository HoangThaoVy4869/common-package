import type { OutboxRelayConfig } from './outbox.types';

/** Reads one setting by name, for example from `process.env` or a config service. */
export type OutboxConfigReader = (key: string) => string | number | undefined;

const positiveInteger = (read: OutboxConfigReader, key: string, fallback: number): number => {
    const value = Number(read(key));
    return Number.isSafeInteger(value) && value > 0 ? value : fallback;
};

/**
 * Builds the relay settings from `OUTBOX_*` keys. Invalid or missing values fall back to the defaults.
 *
 *  OUTBOX_POLL_INTERVAL_MS (1000), OUTBOX_BATCH_SIZE (50, max 1000), OUTBOX_LEASE_MS (60000),
 *  OUTBOX_LEASE_RENEW_THRESHOLD_MS (15000), OUTBOX_RETRY_BASE_MS (1000), OUTBOX_RETRY_MAX_MS (300000),
 *  OUTBOX_SHUTDOWN_GRACE_MS (30000)
 */
export function loadOutboxRelayConfig(
    read: OutboxConfigReader = (key) => process.env[key],
    overrides: Partial<OutboxRelayConfig> = {}
): OutboxRelayConfig {
    const leaseMs = positiveInteger(read, 'OUTBOX_LEASE_MS', 60_000);
    const requestedThreshold = positiveInteger(read, 'OUTBOX_LEASE_RENEW_THRESHOLD_MS', 15_000);
    const retryBaseMs = positiveInteger(read, 'OUTBOX_RETRY_BASE_MS', 1_000);
    const retryMaxMs = Math.max(retryBaseMs, positiveInteger(read, 'OUTBOX_RETRY_MAX_MS', 300_000));

    return {
        pollIntervalMs: positiveInteger(read, 'OUTBOX_POLL_INTERVAL_MS', 1_000),
        batchSize: Math.min(1_000, positiveInteger(read, 'OUTBOX_BATCH_SIZE', 50)),
        leaseMs,
        leaseRenewThresholdMs: Math.min(requestedThreshold, Math.max(1, leaseMs - 1)),
        retryBaseMs,
        retryMaxMs,
        shutdownGraceMs: positiveInteger(read, 'OUTBOX_SHUTDOWN_GRACE_MS', 30_000),
        ...overrides
    };
}

/** Exponential backoff with up to 20% jitter, capped at `maxMs`. */
export function calculateOutboxRetryDelay(
    retryCount: number,
    baseMs: number,
    maxMs: number,
    random: () => number = Math.random
): number {
    const exponent = Math.min(Math.max(retryCount, 0), 30);
    const capped = Math.min(baseMs * 2 ** exponent, maxMs);
    return Math.min(maxMs, Math.round(capped + capped * 0.2 * random()));
}
