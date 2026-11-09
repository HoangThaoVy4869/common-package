/**
 * Kafka messaging types.
 *
 * Defines configuration, consumer options, topic resolution, and record
 * metadata types for Kafka integration events backed by @confluentinc/kafka-javascript.
 */

import type {
    KafkaJS,
    GlobalConfig,
    ProducerGlobalConfig,
    ProducerTopicConfig,
    ConsumerGlobalConfig,
    ConsumerTopicConfig
} from '@confluentinc/kafka-javascript';

export type Consumer = KafkaJS.Consumer;
export type Producer = KafkaJS.Producer;
export type IHeaders = KafkaJS.IHeaders;
export type RecordMetadata = KafkaJS.RecordMetadata;
export type {
    GlobalConfig,
    ProducerGlobalConfig,
    ProducerTopicConfig,
    ConsumerGlobalConfig,
    ConsumerTopicConfig
};

/** Re-exported common contracts shared across transports. */
import type { MessageEnvelope, MessageHandler, MessageInbox, PublishOptions, RetryPolicy, SubscriptionOptions } from '@messaging-common';
import type { IEventPublisher, IEventSubscriber } from '@core/interfaces';

export type { MessageEnvelope, MessageHandler, MessageInbox, PublishOptions, SubscriptionOptions };

export type KafkaMessageKind = 'event' | 'command';

/**
 * Event name → topic mapping strategy.
 *
 * Integration events are grouped into logical topics. A wildcard event name
 * (e.g. `order.*`, `payment.*`) is resolved through {@link resolveEventTopic}.
 */
export interface KafkaTopicResolver {
    /** Resolve an event name to a destination topic. */
    resolveTopic(eventName: string): string;
    /** Optional: ensure the topic exists at bootstrap (defaults to `true`). */
    ensure?: boolean;
}

export interface KafkaTopicDefinition {
    topic: string;
    numPartitions: number;
    replicationFactor: number;
    retentionMs?: number;
    minInSyncReplicas?: number;
    cleanupPolicy?: 'delete' | 'compact' | 'compact,delete';
    /** Additional topic config entries passed to Admin client. */
    configEntries?: Array<{ name: string; value: string }>;
}

/** Backward compatibility alias for KafkaTopicDefinition */
export type KafkaTopicConfig = KafkaTopicDefinition;

export interface KafkaDlqTopicConfig {
    /** Suffix appended to the dead-letter topic (default `-dlq`). */
    dlqSuffix?: string;
    /**
     * Create the DLQ through the Admin API when it is first needed (default
     * false). Keep this false in production and provision DLQ topics via IaC.
     */
    provision?: boolean;
}

export interface KafkaSASLOptions {
    mechanism: 'plain' | 'scram-sha-256' | 'scram-sha-512';
    username: string;
    password: string;
}

export interface KafkaConnectionOptions {
    clientId: string;
    brokers: string[];
    ssl?: boolean;
    sasl?: KafkaSASLOptions;
    /**
     * Topic management strategy at startup:
     * - 'validate': (Default in production) Verifies expected topics exist and partition counts match. Throws and fails-fast on mismatch without attempting to auto-create.
     * - 'apply': (For Dev/CI) Creates missing topics with expected partition/replication parameters.
     * - 'disabled': Skips topic verification and creation entirely.
     */
    topicManagement?: 'validate' | 'apply' | 'disabled';
    /** Expected topic definitions acting as the Single Source of Truth. */
    topics?: KafkaTopicDefinition[];
    /** Maximum TCP/TLS/SASL broker connection setup time in ms (default 10_000). */
    connectTimeoutMs?: number;
    /** Native librdkafka request timeout in ms. Omit to use librdkafka's default. */
    requestTimeoutMs?: number;
    /** Whether `connect()` waits for the cluster metadata (default true). */
    waitForCluster?: boolean;
    /** Whether the client producer is allowed to auto-create topics (default false). */
    allowAutoTopicCreation?: boolean;
    /** Default replication factor for created topics (falls back to process.env.KAFKA_DEFAULT_REPLICATION_FACTOR or 1). */
    defaultTopicReplication?: number;
    /** Default min in-sync replicas for created topics (falls back to process.env.KAFKA_DEFAULT_MIN_INSYNC_REPLICAS or 1). */
    defaultMinInSyncReplicas?: number;
    /** Default number of partitions for created topics (falls back to process.env.KAFKA_DEFAULT_NUM_PARTITIONS or 4). */
    defaultNumPartitions?: number;
    /** Extra options forwarded to the client producer constructor (native librdkafka Producer properties). */
    producerConfig?: Partial<ProducerGlobalConfig & ProducerTopicConfig>;
    /**
     * Extra native librdkafka options passed directly to the client. This is
     * intentionally exposed so callers can adopt new librdkafka settings
     * without waiting for a package release.
     */
    librdkafkaConfig?: GlobalConfig;
}

export interface KafkaProducerOptions {
    /**
     * Partition key resolver. Defaults to `payload.aggregateId ?? payload.id`
     * (falls back to a random value) so events of the same aggregate stay
     * ordered within one partition.
     */
    partitionKey?: (eventName: string, payload: unknown) => string | undefined;
    /** @deprecated Configure this through {@link KafkaConnectionOptions.allowAutoTopicCreation}. */
    allowAutoTopicCreation?: boolean;
}

export interface KafkaConsumerGroupOptions {
    /**
     * Consumer group id.
     * A missing group id creates a generated, process-local group id suitable only for scratch subscriptions.
     */
    groupId?: string;
    /** `fromBeginning` for a new consumer group (default false, maps to `auto.offset.reset: 'earliest'`). */
    fromBeginning?: boolean;
    /** Native librdkafka ConsumerConfig overrides ('session.timeout.ms', 'heartbeat.interval.ms', etc.). */
    consumerConfig?: Partial<ConsumerGlobalConfig & ConsumerTopicConfig>;
    /** Number of retries before the message is sent to the DLQ topic (default 3). */
    maxRetries?: number;
    /** Retry delay in ms when a handler fails (default 5_000). */
    retryDelayMs?: number;
    /**
     * Partition assignment strategy (default 'cooperative-sticky').
     * Avoids stop-the-world eager rebalances during rolling deployments.
     */
    partitionAssignmentStrategy?: 'cooperative-sticky' | 'range,roundrobin' | string;
    /**
     * Maximum time in ms between polls before the consumer is considered dead (default 300_000ms / 5 mins).
     */
    maxPollIntervalMs?: number;
    /**
     * Custom error classifier.
     * Return `true` if the error is transient/retriable (triggers partition pause + seek backoff).
     * Return `false` if the error is permanent (forwards directly to DLQ and commits offset immediately).
     */
    isRetriable?: (error: unknown) => boolean;
    /** Optional topic/dql/retry naming config when the defaults do not fit. */
    dlq?: KafkaDlqTopicConfig;
}

export interface GroupMemberDescription {
    groupId: string;
    members: number;
}

export interface KafkaConsumerHandle {
    /** Stop consuming and release the consumer (graceful). */
    stop(drainTimeoutMs?: number): Promise<void>;
    /** Whether the consumer group is currently running. */
    isRunning(): boolean;
    /** KafkaJS Consumer instance backing this handle. */
    readonly consumer?: KafkaJS.Consumer;
}

export interface KafkaConsumerRegistry {
    readonly handles: ReadonlyArray<KafkaConsumerHandle>;
    /** Advanced: expose the underlying running KafkaJS consumers. */
    getConsumers(): KafkaJS.Consumer[];
}

export interface KafkaGroupAdmin {
    listGroups(): Promise<GroupMemberDescription[]>;
    offsets(topic: string): Promise<Record<string, string>>;
}

export interface KafkaEventBusOptions {
    /**
     * @deprecated Idempotency is handled at the application/service level (e.g. via DB unique constraints, transactional outbox/inbox, or RedisIdempotencyRepository).
     */
    inbox?: MessageInbox;
    /** Drain timeout in ms on shutdown (default 30_000). */
    drainTimeoutMs?: number;
    /** Default consumer group options. */
    consumer?: KafkaConsumerGroupOptions;
    /** Topic resolution strategy. */
    topics?: KafkaTopicResolver & { config?: KafkaTopicConfig[] };
}

export interface KafkaProduceResult {
    topic: string;
    partition: number;
    offset: string;
    key?: string;
}

/** Envelope + kafka record metadata handed to handlers. */
export interface KafkaMessageContext<T = unknown> {
    envelope: MessageEnvelope<T>;
    topic: string;
    partition: number;
    offset: string;
    key?: string;
    headers?: Record<string, string>;
}

/** Internal decorated handler carrying the parsed envelope + context. */
export interface DecoratedKafkaHandler<T = unknown> {
    payload: T;
    context: KafkaMessageContext<T>;
    /** Message envelope's `id` used for inbox dedup. */
    envelopeId: string;
}

/** Individual item in a batch publication, optionally carrying its own options or partitionKey. */
export type KafkaBatchItem<T> = T | { payload: T; options?: PublishOptions; partitionKey?: string };

export function isKafkaError(error: unknown): boolean {
    if (!error || typeof error !== 'object') return false;
    if (error instanceof Error) {
        if ('code' in error || 'origin' in error || 'errno' in error) return true;
        if (error.name.startsWith('Kafka') || error.name.startsWith('LibrdKafka')) return true;
    }
    return false;
}

export function isRetriableKafkaError(error: unknown): boolean {
    if (!error || typeof error !== 'object') return false;
    if ('isRetriable' in error && typeof (error as any).isRetriable === 'boolean') {
        return (error as any).isRetriable;
    }
    if ('retriable' in error && typeof (error as any).retriable === 'boolean') {
        return (error as any).retriable;
    }
    const code = (error as any).code;
    if (typeof code === 'number') {
        return [-192, -187, -195, 7, 19, 20].includes(code);
    }
    const message = (error as any).message;
    if (typeof message === 'string') {
        return /timeout|retriable|temporary|leader not available|not enough replicas|connection reset|disconnect/i.test(message);
    }
    return false;
}

export { isPermanentError } from '@messaging-common';

export interface KafkaRetryPolicy {
    maxRetries?: number;
    retryDelayMs?: number;
}

export interface KafkaSubscriptionOptions {
    /**
     * Kafka consumer group id.
     */
    groupId?: string;
    /** Whether to start consuming from earliest offset when no offset is committed. */
    fromBeginning?: boolean;
    /** Retry configuration for transient errors. */
    retry?: KafkaRetryPolicy | RetryPolicy;
    /** Fine-grained consumer configuration overrides. */
    consumerConfig?: Partial<ConsumerGlobalConfig & ConsumerTopicConfig>;
}

export interface IKafkaSubscriber extends IEventSubscriber<KafkaSubscriptionOptions> {
    subscribeEvent<T = unknown>(
        eventName: string,
        handler: MessageHandler<T>,
        options?: KafkaSubscriptionOptions
    ): Promise<void>;
}

export interface IKafkaEventBus extends IEventPublisher, IKafkaSubscriber {
    publishBatch<T>(eventName: string, items: Array<KafkaBatchItem<T>>, options?: PublishOptions): Promise<void>;
}
