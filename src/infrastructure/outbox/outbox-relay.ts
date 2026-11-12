import { randomUUID } from 'node:crypto';
import { hostname } from 'node:os';
import type { IEventPublisher } from '../../core/interfaces/IMessageBus';
import type { ILogger } from '../../core/interfaces/ILogger';
import { calculateOutboxRetryDelay, loadOutboxRelayConfig } from './outbox-relay.config';
import type { ClaimedOutboxEvent, IOutboxStore, OutboxRelayConfig } from './outbox.types';

export interface OutboxRelayOptions {
    /** Used in log tags, for example `OrderOutboxRelay`. */
    name: string;
    /** Defaults to `loadOutboxRelayConfig()`, which reads the `OUTBOX_*` environment variables. */
    config?: OutboxRelayConfig;
    /** Builds the published payload. Defaults to the stored payload. */
    mapPayload?: (event: ClaimedOutboxEvent) => unknown;
    /** Override the lease holder id. Defaults to `${hostname}:${pid}:${randomUUID()}`. */
    instanceId?: string;
}

type PublishOutcome = 'succeeded' | 'failed' | 'lost';

/**
 * Polls an outbox store and publishes the rows to an event bus, at least once.
 *
 * Several instances can run at the same time: each row is claimed under a lease, the lease is renewed
 * while a batch is being published, and a row whose lease was lost is never reported as published.
 * Publishing a row also passes its id as `messageId` so consumers can deduplicate redeliveries.
 *
 * The class has no framework dependency. Call `start()` when the application starts and
 * `await stop()` when it shuts down.
 */
export class OutboxRelay {
    private readonly instanceId: string;
    private readonly settings: OutboxRelayConfig;
    private readonly tag: string;
    private timer?: NodeJS.Timeout;
    private activeRun?: Promise<void>;
    private stopping = false;
    private ownedIds = new Set<string>();

    constructor(
        private readonly store: IOutboxStore,
        private readonly publisher: IEventPublisher,
        private readonly logger: ILogger,
        private readonly options: OutboxRelayOptions
    ) {
        this.instanceId = options.instanceId ?? `${hostname().slice(0, 64)}:${process.pid}:${randomUUID()}`;
        this.settings = options.config ?? loadOutboxRelayConfig();
        this.tag = `[${options.name}]`;
    }

    start(): void {
        if (this.timer) return;
        this.stopping = false;
        this.logger.info(`${this.tag} started`, {
            instanceId: this.instanceId,
            pollIntervalMs: this.settings.pollIntervalMs,
            batchSize: this.settings.batchSize,
            leaseMs: this.settings.leaseMs
        });
        this.timer = setInterval(() => this.runIfIdle(), this.settings.pollIntervalMs);
        this.timer.unref();
        this.runIfIdle();
    }

    async stop(): Promise<void> {
        this.stopping = true;
        if (this.timer) {
            clearInterval(this.timer);
            this.timer = undefined;
        }

        if (this.activeRun) {
            await Promise.race([
                this.activeRun,
                new Promise<void>((resolve) => {
                    const timeout = setTimeout(resolve, this.settings.shutdownGraceMs);
                    timeout.unref();
                })
            ]);
        }

        const remaining = [...this.ownedIds];
        if (remaining.length > 0) {
            try {
                await this.store.releaseClaims(remaining, this.instanceId);
            } catch (error) {
                this.logger.warn(`${this.tag} failed to release claims during shutdown`, {
                    instanceId: this.instanceId,
                    claimCount: remaining.length,
                    error: this.errorMessage(error)
                });
            }
        }
        this.logger.info(`${this.tag} stopped`, { instanceId: this.instanceId });
    }

    /** Runs one batch. Exposed so a service can trigger a run on demand (for example in tests). */
    async runOnce(): Promise<void> {
        const startedAt = Date.now();
        const events = await this.store.claimBatch(this.instanceId, this.settings.batchSize, this.settings.leaseMs);
        if (events.length === 0) return;

        this.ownedIds = new Set(events.map((event) => event.id));
        let leaseDeadline = Math.min(...events.map((event) => event.claimedUntil.getTime()));
        let succeeded = 0;
        let failed = 0;
        let lost = 0;

        this.logger.info(`${this.tag} batch claimed`, { instanceId: this.instanceId, batchSize: events.length });

        for (const event of events) {
            if (this.stopping) break;

            if (leaseDeadline - Date.now() <= this.settings.leaseRenewThresholdMs) {
                const renewed = await this.store.renewClaims([...this.ownedIds], this.instanceId, this.settings.leaseMs);
                for (const id of this.ownedIds) {
                    if (!renewed.has(id)) {
                        this.ownedIds.delete(id);
                        lost += 1;
                    }
                }
                leaseDeadline = Date.now() + this.settings.leaseMs;
            }

            if (!this.ownedIds.has(event.id)) continue;
            const outcome = await this.publishOne(event);
            this.ownedIds.delete(event.id);
            if (outcome === 'succeeded') succeeded += 1;
            else if (outcome === 'failed') failed += 1;
            else lost += 1;
        }

        this.logger.info(`${this.tag} batch completed`, {
            instanceId: this.instanceId,
            claimed: events.length,
            succeeded,
            failed,
            lost,
            unprocessed: this.ownedIds.size,
            durationMs: Date.now() - startedAt
        });
    }

    private runIfIdle(): void {
        if (this.stopping || this.activeRun) return;
        this.activeRun = this.runOnce()
            .catch((error) => {
                this.logger.error(`${this.tag} batch failed unexpectedly`, {
                    instanceId: this.instanceId,
                    error: this.errorMessage(error)
                });
            })
            .finally(() => {
                this.activeRun = undefined;
            });
    }

    private async publishOne(event: ClaimedOutboxEvent): Promise<PublishOutcome> {
        const startedAt = Date.now();
        try {
            const payload = this.options.mapPayload ? this.options.mapPayload(event) : event.payload;
            await this.publisher.publishEvent(event.eventType, payload, { messageId: event.id });

            if (!(await this.store.markPublished(event.id, this.instanceId))) {
                this.logLost(event);
                return 'lost';
            }
            this.logger.info(`${this.tag} event published`, {
                eventId: event.id,
                eventType: event.eventType,
                aggregateId: event.aggregateId,
                retryCount: event.retryCount,
                durationMs: Date.now() - startedAt
            });
            return 'succeeded';
        } catch (error) {
            const message = this.errorMessage(error);
            const delay = calculateOutboxRetryDelay(event.retryCount, this.settings.retryBaseMs, this.settings.retryMaxMs);
            const nextAttemptAt = new Date(Date.now() + delay);

            if (!(await this.store.markFailed(event.id, this.instanceId, nextAttemptAt, message))) {
                this.logLost(event);
                return 'lost';
            }
            this.logger.warn(`${this.tag} event publish failed, scheduled retry`, {
                eventId: event.id,
                eventType: event.eventType,
                aggregateId: event.aggregateId,
                retryCount: event.retryCount + 1,
                nextAttemptAt: nextAttemptAt.toISOString(),
                error: message
            });
            return 'failed';
        }
    }

    private logLost(event: ClaimedOutboxEvent): void {
        this.logger.warn(`${this.tag} lease ownership lost`, {
            eventId: event.id,
            eventType: event.eventType,
            aggregateId: event.aggregateId
        });
    }

    private errorMessage(error: unknown): string {
        return error instanceof Error ? error.message.slice(0, 500) : 'Unknown error';
    }
}
