/**
 * KafkaProducer
 *
 * Idempotent producer wrapper backed by @confluentinc/kafka-javascript. Enforces
 * the standardized `MessageEnvelope` contract, partition-key ordering, and batching.
 */

import { KafkaConnection } from '@kafka/KafkaConnection';
import { createEnvelope, buildProduceMessages, toProduceResult } from '@kafka/kafka.codec';
import { KafkaBatchItem, KafkaProduceResult, KafkaProducerOptions, KafkaTopicResolver } from '@kafka/kafka.types';
import { type MessageEnvelope, type PublishOptions } from '@messaging-common';

const DEFAULT_TOPIC_RESOLVER: KafkaTopicResolver = {
    resolveTopic(eventName: string): string {
        const segments = eventName.split('.'); // order.created.v1 => order.events
        return segments.length >= 2 ? `${segments[0]}.events` : `${eventName}.events`;
    }
};

export class KafkaProducer {
    private readonly options: KafkaProducerOptions;

    constructor(
        private readonly connection: KafkaConnection,
        private readonly topicResolver: KafkaTopicResolver = DEFAULT_TOPIC_RESOLVER,
        options: KafkaProducerOptions = {}
    ) {
        this.options = options ?? {};
    }

    get isReady(): boolean {
        return this.connection.isReady;
    }

    /**
     * Publish an integration event to the topic resolved from `eventName`.
     * Kind defaults to 'event' unless overridden in `options.kind`.
     */
    async publish<T>(eventName: string, payload: T, options: PublishOptions = {}): Promise<KafkaProduceResult> {
        const topic = this.resolveTopic(eventName);
        const kind = options.kind ?? 'event';
        const envelope = createEnvelope(kind, eventName, payload, options);
        const records = buildProduceMessages([envelope], {
            partitionKey: this.options.partitionKey,
            defaultField: 'aggregateId'
        });

        const metadata = await this.connection.producer.send({
            topic,
            messages: records
        });

        return toProduceResult(metadata, topic);
    }

    /**
     * Publish a command message to a Kafka topic.
     * Convenience helper setting `options.kind = 'command'`.
     */
    async sendCommand<T>(commandName: string, payload: T, options: PublishOptions = {}): Promise<KafkaProduceResult> {
        return this.publish(commandName, payload, { ...options, kind: options.kind ?? 'command' });
    }

    /**
     * Batches multiple messages of the same topic into a single network round-trip for high throughput.
     * Accepts either a raw array of payloads or an array of items with individual options and partition keys.
     *
     * Note on ordering: Kafka guarantees strict ordering ONLY per partition. Messages with different
     * partition keys may be distributed across different partitions and consumed out of batch order.
     */
    async publishBatch<T>(
        eventName: string,
        items: Array<KafkaBatchItem<T>>,
        defaultOptions: PublishOptions = {}
    ): Promise<KafkaProduceResult[]> {
        if (!items || items.length === 0) return [];

        const topic = this.resolveTopic(eventName);
        const explicitKeys: Array<string | undefined> = [];

        const envelopes: MessageEnvelope<T>[] = items.map((item) => {
            const isBatchItem = item !== null && typeof item === 'object' && 'payload' in (item as Record<string, unknown>);
            const payload = isBatchItem ? (item as { payload: T }).payload : (item as T);
            const itemOptions = isBatchItem ? (item as { options?: PublishOptions }).options : undefined;
            const itemKey = isBatchItem ? (item as { partitionKey?: string }).partitionKey : undefined;

            explicitKeys.push(itemKey);

            const mergedOptions: PublishOptions = {
                ...defaultOptions,
                ...(itemOptions ?? {})
            };
            const kind = mergedOptions.kind ?? defaultOptions.kind ?? 'event';
            return createEnvelope(kind, eventName, payload, mergedOptions);
        });

        const records = buildProduceMessages(envelopes, {
            partitionKey: this.options.partitionKey,
            defaultField: 'aggregateId',
            explicitKeys
        });

        const metadata = await this.connection.producer.send({
            topic,
            messages: records
        });

        return metadata.map((entry, index) => ({
            topic: entry.topicName ?? topic,
            partition: entry.partition,
            offset: entry.baseOffset ?? '',
            key: records[index]?.key
        }));
    }

    /**
     * Publishes an array of pre-built MessageEnvelopes directly to a topic in a single batch (e.g. for Outbox workers).
     */
    async publishBatchRaw<T>(
        topic: string,
        envelopes: MessageEnvelope<T>[],
        partitionKey?: (envelope: MessageEnvelope<T>) => string | undefined
    ): Promise<KafkaProduceResult[]> {
        if (!envelopes || envelopes.length === 0) return [];

        const explicitKeys = partitionKey ? envelopes.map(partitionKey) : undefined;
        const records = buildProduceMessages(envelopes, {
            partitionKey: this.options.partitionKey,
            defaultField: 'aggregateId',
            explicitKeys
        });

        const metadata = await this.connection.producer.send({
            topic,
            messages: records
        });

        return metadata.map((entry, index) => ({
            topic: entry.topicName ?? topic,
            partition: entry.partition,
            offset: entry.baseOffset ?? '',
            key: records[index]?.key
        }));
    }

    /**
     * Publish a raw message envelope without topic resolution (power users).
     */
    async sendRaw<T>(topic: string, envelope: MessageEnvelope<T>, partitionKey?: string): Promise<KafkaProduceResult> {
        const records = buildProduceMessages([envelope], {
            partitionKey: () => partitionKey,
            defaultField: 'aggregateId',
            explicitKeys: [partitionKey]
        });
        const metadata = await this.connection.producer.send({
            topic,
            messages: records
        });
        return toProduceResult(metadata, topic);
    }

    private resolveTopic(eventName: string): string {
        return this.topicResolver.resolveTopic(eventName);
    }
}