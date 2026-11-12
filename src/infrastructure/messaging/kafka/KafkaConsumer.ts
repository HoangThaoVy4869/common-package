/**
 * KafkaConsumer
 *
 * One durable consumer-group subscription per logical subscription. Handles:
 *  - envelope parsing + inbox dedup (at-least-once / idempotency)
 *  - per-partition pause/back-off retry (offsets stay uncommitted while paused)
 *  - dead-letter topic after `maxRetries`
 *
 * Powered by @confluentinc/kafka-javascript.
 */

import type { KafkaJS } from '@confluentinc/kafka-javascript';
import { KafkaConnection } from '@kafka/KafkaConnection';
import { buildMessageContext, KAFKA_HEADERS, parseEnvelope } from '@kafka/kafka.codec';
import { isPermanentError, type KafkaConsumerGroupOptions, type KafkaConsumerHandle, type KafkaMessageContext } from '@kafka/kafka.types';
import type { MessageHandler } from '@messaging-common';
import type { ILogger } from '@core/interfaces';

export class KafkaConsumer implements KafkaConsumerHandle {
    readonly consumer: KafkaJS.Consumer;
    private readonly logger: ILogger;
    private running = false;
    private stopping = false;
    private readonly retryTimers = new Map<string, NodeJS.Timeout>();
    /** Partition retry counts are intentionally process-local. */
    private readonly retryCounts = new Map<string, number>();

    constructor(
        private readonly connection: KafkaConnection,
        private readonly kind: 'event' | 'command',
        private readonly name: string,
        private readonly handler: MessageHandler,
        private readonly topic: string,
        private readonly options: KafkaConsumerGroupOptions,
        id: number
    ) {
        this.logger = options.logger ?? connection.logger;
        const groupId = options.groupId ?? `anonymous-${name}-${id}`;
        const rawConsumerConfig = (options.consumerConfig ?? {}) as Record<string, unknown>;
        const { groupId: _ignored, ...consumerConfigRest } = rawConsumerConfig;
        const assignmentStrategy = options.partitionAssignmentStrategy ?? 'cooperative-sticky';
        const maxPollIntervalMs = options.maxPollIntervalMs ?? 300_000;

        this.consumer = connection.client.consumer({
            'group.id': groupId,
            'enable.auto.commit': false,
            'auto.offset.reset': options.fromBeginning ? 'earliest' : 'latest',
            'partition.assignment.strategy': assignmentStrategy,
            'max.poll.interval.ms': maxPollIntervalMs,
            ...consumerConfigRest,
            kafkaJS: {
                groupId,
                ...((consumerConfigRest.kafkaJS as Record<string, unknown>) ?? {})
            }
        } as any);

        if ('on' in this.consumer && typeof (this.consumer as any).on === 'function') {
            try {
                (this.consumer as any).on('error', (error: unknown) => {
                    this.logger.error(`[Kafka] Consumer error group=${groupId}`, { error });
                });
            } catch {
                // Ignore: .on() is a notImplemented stub in @confluentinc/kafka-javascript
            }
        }
    }

    async start(): Promise<void> {
        try {
            await this.connection.ensureTopic(this.topic);
        } catch {
            // best-effort topic creation before subscribing
        }

        const subscribe: KafkaJS.ConsumerSubscribeTopics = {
            topics: [this.topic]
        };

        const run: KafkaJS.ConsumerRunConfig = {
            eachMessage: async ({ topic, partition, message }) => {
                if (this.stopping) return;
                const shouldCommit = await this.consume(topic, partition, message as KafkaJS.KafkaMessage);
                if (shouldCommit) {
                    await this.consumer.commitOffsets([
                        { topic, partition, offset: nextOffset(message.offset) }
                    ]);
                }
            }
        };

        await this.consumer.connect();
        await this.consumer.subscribe(subscribe);
        await this.consumer.run(run);
        this.running = true;
    }

    isRunning(): boolean {
        return this.running && !this.stopping;
    }

    async stop(drainTimeoutMs = 15_000): Promise<void> {
        if (this.stopping) return;
        this.stopping = true;
        this.running = false;

        for (const timer of this.retryTimers.values()) clearTimeout(timer);
        this.retryTimers.clear();

        await this.consumer.stop().catch((error) => {
            if (error?.code !== -170 && error?.type !== 'ERR__NOT_IMPLEMENTED') {
                this.logger.warn('[Kafka] consumer.stop failed', { error });
            }
        });
        await this.consumer.disconnect().catch((error) => this.logger.warn('[Kafka] consumer.disconnect failed', { error }));
    }

    /** Returns true only when it is safe to commit this record's offset. */
    private async consume(topic: string, partition: number, message: KafkaJS.KafkaMessage): Promise<boolean> {
        const partitionKey = `${topic}:${partition}`;
        const key = readHeaderString(message.headers, KAFKA_HEADERS.PARTITION_KEY) ?? message.key?.toString('utf8');
        const envelope = parseEnvelope(message.value, message.headers, this.kind, this.name);
        const context: KafkaMessageContext = buildMessageContext(
            envelope,
            topic,
            partition,
            message.offset,
            key,
            message.headers
        );

        const parseError = (envelope.metadata as Record<string, unknown> | undefined)?.parseError;
        if (parseError) {
            const error = new Error(`[Kafka] Poison pill / Deserialization error: ${parseError}`);
            (error as any).isPermanent = true;
            return this.handleFailure(topic, partition, partitionKey, context, error);
        }

        try {
            await this.handler(envelope.payload, message as unknown, envelope);
            this.retryCounts.delete(partitionKey);
            return true;
        } catch (error) {
            return this.handleFailure(topic, partition, partitionKey, context, error);
        }
    }

    private async handleFailure(topic: string, partition: number, partitionKey: string, context: KafkaMessageContext, error: unknown): Promise<boolean> {
        // Classify error: Permanent or retryable (Transient)
        const isRetriable = this.options.isRetriable
            ? this.options.isRetriable(error)
            : !isPermanentError(error);

        if (!isRetriable) {
            this.logger.warn(
                `[Kafka] Permanent error on ${this.kind}=${this.name} topic=${topic} partition=${partition}, forwarding directly to DLQ without pause.`,
                { error: error instanceof Error ? error.message : error }
            );
            const deadLettered = await this.sendToDeadLetter(topic, context, 'permanent-failure', error);
            if (deadLettered) {
                this.retryCounts.delete(partitionKey);
                return true;
            }
            await this.pauseAndReschedule(topic, partition, partitionKey, context.offset, this.options.retryDelayMs ?? 5_000);
            return false;
        }

        const maxRetries = this.options.maxRetries ?? 3;
        const retryDelayMs = this.options.retryDelayMs ?? 5_000;
        const attempts = this.retryCounts.get(partitionKey) ?? 0;

        if (attempts < maxRetries) {
            this.retryCounts.set(partitionKey, attempts + 1);
            this.logger.warn(
                `[Kafka] handler failed ${this.kind}=${this.name} topic=${topic} partition=${partition} attempt=${attempts + 1}/${maxRetries}, pausing ${retryDelayMs}ms`,
                { error: error instanceof Error ? error.message : error }
            );
            await this.pauseAndReschedule(topic, partition, partitionKey, context.offset, retryDelayMs);
            return false;
        }

        // Only skip a poison record after the DLQ publish succeeds. If the DLQ
        // is unavailable, keep the source offset uncommitted and try again.
        const deadLettered = await this.sendToDeadLetter(topic, context, 'max-retries-exceeded', error);
        if (deadLettered) {
            this.retryCounts.delete(partitionKey);
            return true;
        }

        await this.pauseAndReschedule(topic, partition, partitionKey, context.offset, retryDelayMs);
        return false;
    }

    private async pauseAndReschedule(topic: string, partition: number, partitionKey: string, offset: string, retryDelayMs: number): Promise<void> {
        const existing = this.retryTimers.get(partitionKey);
        if (existing) clearTimeout(existing);

        // Pause fetches, then explicitly seek back before resuming. Seeking is
        // required because the consumer position may already have advanced even
        // though the offset was deliberately left uncommitted.
        this.consumer.pause([{ topic, partitions: [partition] }]);

        this.retryTimers.set(
            partitionKey,
            setTimeout(() => {
                this.retryTimers.delete(partitionKey);
                if (this.stopping) return;
                try {
                    this.consumer.seek({ topic, partition, offset });
                    this.consumer.resume([{ topic, partitions: [partition] }]);
                } catch (error) {
                    this.logger.error('[Kafka] resume failed', { error });
                }
            }, retryDelayMs)
        );
    }

    private async sendToDeadLetter(topic: string, context: KafkaMessageContext, reason = 'max-retries-exceeded', error?: unknown): Promise<boolean> {
        const dlqTopic = this.dlqTopic(topic);
        const envelope = {
            ...context.envelope,
            metadata: {
                ...(context.envelope.metadata ?? {}),
                dlq: {
                    originalTopic: topic,
                    originalPartition: context.partition,
                    originalOffset: context.offset,
                    reason,
                    errorMessage: error instanceof Error ? error.message : (error ? String(error) : undefined),
                    movedAt: new Date().toISOString()
                }
            }
        };

        const headers: KafkaJS.IHeaders = {
            [KAFKA_HEADERS.MESSAGE_ID]: Buffer.from(envelope.id, 'utf8'),
            [KAFKA_HEADERS.MESSAGE_KIND]: Buffer.from(this.kind, 'utf8'),
            [KAFKA_HEADERS.MESSAGE_NAME]: Buffer.from(envelope.name, 'utf8'),
            [KAFKA_HEADERS.OCCURRED_AT]: Buffer.from(envelope.occurredAt, 'utf8'),
            [KAFKA_HEADERS.SCHEMA]: Buffer.from('vyhoang.eventbus.v1', 'utf8'),
            [KAFKA_HEADERS.RETRY_COUNT]: Buffer.from(String(this.options.maxRetries ?? 3), 'utf8')
        };

        try {
            // Production consumer identities commonly have no Admin/CreateTopics
            // permission. Provision DLQs through IaC, or opt into this only for
            // local development.
            if (this.options.dlq?.provision === true) {
                await this.connection.ensureTopic(dlqTopic, { numPartitions: 1 });
            }
            await this.connection.producer.send({
                topic: dlqTopic,
                messages: [{ key: context.key, value: Buffer.from(JSON.stringify(envelope), 'utf8'), headers }]
            });
            this.logger.warn(`[Kafka] ${this.kind}=${this.name} dead-lettered to ${dlqTopic} (${reason})`, { originalOffset: context.offset });
            return true;
        } catch (err) {
            this.logger.error(`[Kafka] could not dead-letter ${this.kind}=${this.name} to ${dlqTopic}`, { error: err });
            return false;
        }
    }

    private dlqTopic(topic: string): string {
        const suffix = this.options.dlq?.dlqSuffix ?? '-dlq';
        return `${topic}${suffix}`;
    }
}

function nextOffset(offset: string): string {
    return (BigInt(offset) + 1n).toString();
}

function readHeaderString(headers: KafkaJS.IHeaders | undefined, key: string): string | undefined {
    const value = headers?.[key];
    if (value === null || value === undefined) return undefined;
    const values = Array.isArray(value) ? value : [value];
    const first = values[0];
    if (first === null || first === undefined) return undefined;
    return Buffer.isBuffer(first) ? first.toString('utf8') : String(first);
}
