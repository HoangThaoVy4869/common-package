/**
 * KafkaEventBus
 *
 * Implements `IEventBus` on top of Kafka for integration events:
 *  - publishEvent / subscribeEvent → Kafka topics
 *  - publishBatch                  → Batch publication
 */

import { IEventBus } from '@core/interfaces';
import type { MessageHandler, PublishOptions, SubscriptionOptions } from '@messaging-common';
import { KafkaConnection } from '@kafka/KafkaConnection';
import { KafkaConsumer } from '@kafka/KafkaConsumer';
import { KafkaProducer } from '@kafka/KafkaProducer';
import type { KafkaBatchItem, KafkaConsumerGroupOptions, KafkaConsumerHandle, KafkaEventBusOptions, KafkaTopicResolver } from '@kafka/kafka.types';

const DEFAULT_TOPIC_RESOLVER: KafkaTopicResolver = {
    resolveTopic(eventName: string): string {
        const segments = eventName.split('.');
        return segments.length >= 2 ? `${segments[0]}.events` : `${eventName}.events`;
    }
};

export class KafkaEventBus implements IEventBus {
    readonly connection: KafkaConnection;
    readonly producer: KafkaProducer;
    private readonly drainTimeoutMs: number;
    private readonly consumerOptions: KafkaConsumerGroupOptions;
    private readonly topicResolver: KafkaTopicResolver;
    private readonly consumers: KafkaConsumerHandle[] = [];
    private closed = false;

    constructor(connection: KafkaConnection, options: KafkaEventBusOptions = {}) {
        this.connection = connection;
        this.producer = new KafkaProducer(connection, options.topics?.resolveTopic ? { resolveTopic: options.topics.resolveTopic } : DEFAULT_TOPIC_RESOLVER, {
            partitionKey: (eventName, payload) => resolvePartitionKey(eventName, payload)
        });
        this.drainTimeoutMs = options.drainTimeoutMs ?? 30_000;
        this.consumerOptions = options.consumer ?? {};
        this.topicResolver = options.topics?.resolveTopic ? { resolveTopic: options.topics.resolveTopic } : DEFAULT_TOPIC_RESOLVER;
    }

    get isReady(): boolean {
        return this.connection.isReady;
    }

    get consumerCount(): number {
        return this.consumers.length;
    }

    async publishEvent<T>(eventName: string, payload: T, options: PublishOptions = {}): Promise<void> {
        await this.producer.publish(eventName, payload, options);
    }

    async subscribeEvent<T>(eventName: string, handler: MessageHandler<T>, options: SubscriptionOptions = {}): Promise<void> {
        const topic = this.topicResolver.resolveTopic(eventName);
        const consumerOptions: KafkaConsumerGroupOptions = {
            ...this.consumerOptions,
            groupId: options.queueName ?? (this.consumerOptions.groupId || undefined),
            maxRetries: options.retry?.maxRetries ?? this.consumerOptions.maxRetries ?? 3,
            retryDelayMs: options.retry?.retryDelayMs ?? this.consumerOptions.retryDelayMs ?? 5_000,
            consumerConfig: this.consumerOptions.consumerConfig
        };

        const consumer = new KafkaConsumer(this.connection, 'event', eventName, handler, topic, consumerOptions, this.consumers.length + 1);
        await consumer.start();
        this.consumers.push(consumer);
    }

    /**
     * Publish a batch of events to the resolved topic in one send.
     * Note: Ordering is strictly guaranteed per partition key, not across the entire batch.
     */
    async publishBatch<T>(eventName: string, items: Array<KafkaBatchItem<T>>, options: PublishOptions = {}): Promise<void> {
        await this.producer.publishBatch(eventName, items, options);
    }

    async close(options: { drainTimeoutMs?: number } = {}): Promise<void> {
        if (this.closed) return;
        this.closed = true;
        const timeout = options.drainTimeoutMs ?? this.drainTimeoutMs;
        await Promise.all(this.consumers.map((consumer) => consumer.stop(timeout)));
        this.consumers.length = 0;
        console.info('[Kafka] Event bus closed');
    }
}

function resolvePartitionKey(eventName: string, payload: unknown): string | undefined {
    if (payload && typeof payload === 'object') {
        const record = payload as Record<string, unknown>;
        const candidate = record.aggregateId ?? record.id ?? record.orderId ?? record.checkoutId;
        if (typeof candidate === 'string' && candidate.length > 0) return candidate;
        if (typeof candidate === 'number') return String(candidate);
    }
    return undefined;
}
