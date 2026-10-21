/**
 * KafkaConnection
 *
 * Owns one long-lived client, an Admin client, and a producer backed by @confluentinc/kafka-javascript.
 * Follows the singleton transport lifecycle: one connection per service process,
 * injected into the event bus, closed on application shutdown.
 */

import type { KafkaJS } from '@confluentinc/kafka-javascript';
import type { KafkaConnectionOptions, KafkaTopicConfig, KafkaTopicDefinition, GroupMemberDescription } from '@kafka/kafka.types';

export interface KafkaConnectionEventHandlers {
    onConnect?: () => void;
    onDisconnect?: (error?: Error) => void;
    onError?: (error: Error) => void;
}

const DEFAULT_CONNECT_TIMEOUT_MS = 10_000;
const DEFAULT_TOPIC_REPLICATION = 1;
const DEFAULT_TOPIC_PARTITIONS = 4;

export class KafkaConnection {
    readonly client: KafkaJS.Kafka;
    readonly admin: KafkaJS.Admin;
    readonly producer: KafkaJS.Producer;
    private readonly options: KafkaConnectionOptions;
    private readonly topology = new Map<string, Promise<void>>();
    private closed = false;
    private connected = false;

    private handlers: KafkaConnectionEventHandlers = {};

    constructor(options: KafkaConnectionOptions) {
        this.options = {
            ...options,
            clientId: options.clientId ?? 'microservice',
            brokers: options.brokers ?? [],
            connectTimeoutMs: options.connectTimeoutMs ?? DEFAULT_CONNECT_TIMEOUT_MS
        };

        const nativeConfig: Record<string, unknown> = {
            'bootstrap.servers': this.options.brokers.join(','),
            'client.id': this.options.clientId,
            // This covers TCP plus TLS/SASL setup. `socket.timeout.ms` is a request timeout, not a connection timeout.
            'socket.connection.setup.timeout.ms': this.options.connectTimeoutMs ?? DEFAULT_CONNECT_TIMEOUT_MS,
            ...(this.options.requestTimeoutMs !== undefined ? { 'socket.timeout.ms': this.options.requestTimeoutMs } : {}),
            ...(this.options.librdkafkaConfig ?? {})
        };

        if (this.options.ssl) {
            nativeConfig['security.protocol'] = 'ssl';
        }

        if (this.options.sasl) {
            nativeConfig['security.protocol'] = this.options.ssl ? 'sasl_ssl' : 'sasl_plaintext';
            nativeConfig['sasl.mechanisms'] = this.options.sasl.mechanism.toUpperCase();
            nativeConfig['sasl.username'] = this.options.sasl.username;
            nativeConfig['sasl.password'] = this.options.sasl.password;
        }

        let kafkaJsModule: any;
        try {
            kafkaJsModule = require('@confluentinc/kafka-javascript').KafkaJS;
        } catch (err) {
            throw new Error('Failed to load @confluentinc/kafka-javascript: ' + (err as Error).message);
        }
        this.client = new kafkaJsModule.Kafka(nativeConfig as any);
        this.admin = this.client.admin();
        const defaultCompression = process.env.KAFKA_PRODUCER_COMPRESSION ?? options.producerConfig?.['compression.type'];
        const defaultLingerMs = process.env.KAFKA_PRODUCER_LINGER_MS
            ? Number(process.env.KAFKA_PRODUCER_LINGER_MS)
            : options.producerConfig?.['linger.ms'];

        this.producer = this.client.producer({
            // librdkafka derives compatible acks/retry/in-flight settings from
            // this. Callers can explicitly opt out through producerConfig.
            'enable.idempotence': true,
            'allow.auto.create.topics': options.allowAutoTopicCreation ?? false,
            ...(defaultCompression ? { 'compression.type': defaultCompression } : {}),
            ...(defaultLingerMs !== undefined ? { 'linger.ms': defaultLingerMs } : {}),
            ...(options.producerConfig ?? {})
        } as any);
    }

    get isReady(): boolean {
        return !this.closed && this.connected;
    }

    get clientId(): string {
        return this.options.clientId;
    }

    set eventHandlers(handlers: KafkaConnectionEventHandlers) {
        this.handlers = handlers;
    }

    async connect(timeoutMs = this.options.connectTimeoutMs ?? DEFAULT_CONNECT_TIMEOUT_MS): Promise<void> {
        if (this.closed) throw new Error('Kafka connection is closed');
        if (this.connected) return;

        try {
            await Promise.all([this.admin.connect(), this.producer.connect()]);

            if (this.options.waitForCluster !== false) {
                await this.waitForCluster(timeoutMs);
            }

            if (this.options.topics?.length) {
                const mode = this.options.topicManagement ?? 'validate';
                if (mode === 'validate') {
                    await this.validateTopics(this.options.topics);
                } else if (mode === 'apply') {
                    await this.applyTopics(this.options.topics);
                }
            }

            this.connected = true;
            this.handlers.onConnect?.();
            console.info(`[Kafka] Connected (clientId=${this.options.clientId})`);
        } catch (error) {
            await Promise.all([this.producer.disconnect().catch(() => undefined), this.admin.disconnect().catch(() => undefined)]);
            const normalized = error instanceof Error ? error : new Error(String(error));
            this.handlers.onError?.(normalized);
            throw normalized;
        }
    }

    async waitForCluster(timeoutMs = DEFAULT_CONNECT_TIMEOUT_MS): Promise<void> {
        const startedAt = Date.now();
        for (; ;) {
            try {
                const topics = await this.admin.listTopics().catch(() => undefined);
                if (topics !== undefined) return;
            } catch (error) {
                // cluster metadata not ready yet
            }
            if (Date.now() - startedAt > timeoutMs) {
                throw new Error(`Kafka cluster not reachable within ${timeoutMs}ms`);
            }
            await sleep(500);
        }
    }

    /**
     * Production safety check: Verifies that expected topics exist on the cluster
     * with matching or sufficient partitions. Throws a descriptive error (fail-fast)
     * if any topic is missing instead of attempting runtime auto-creation.
     */
    async validateTopics(expected: KafkaTopicDefinition[]): Promise<void> {
        if (!expected || expected.length === 0) return;

        const topicNames = expected.map((t) => t.topic);
        const metadata = await this.admin.fetchTopicMetadata({ topics: topicNames }).catch((err) => {
            throw new Error(`[Kafka] Failed to fetch topic metadata for validation: ${err instanceof Error ? err.message : String(err)}`);
        });

        const clusterTopics = new Map<string, number>();
        const topicList = Array.isArray(metadata) ? metadata : (metadata as any).topics ?? [];
        for (const topicMeta of topicList) {
            clusterTopics.set(topicMeta.name, topicMeta.partitions?.length ?? 0);
        }

        for (const def of expected) {
            if (!clusterTopics.has(def.topic)) {
                throw new Error(
                    `[Kafka] Topic validation failed: Topic "${def.topic}" does not exist on cluster. ` +
                    `In production, topics must be pre-provisioned via IaC or CI scripts. Set topicManagement: 'apply' only for development.`
                );
            }
            const actualPartitions = clusterTopics.get(def.topic) ?? 0;
            if (actualPartitions < def.numPartitions) {
                throw new Error(
                    `[Kafka] Topic validation failed: Topic "${def.topic}" has ${actualPartitions} partition(s), ` +
                    `but expected at least ${def.numPartitions}. Adjust broker partition configuration.`
                );
            }
        }

        console.info(`[Kafka] Topics validated successfully (${expected.length} topics confirmed)`);
    }

    /**
     * Applies topic definitions by creating missing topics using the Admin API.
     * Intended for Development, CI, and local test setups.
     */
    async applyTopics(topics: KafkaTopicDefinition[]): Promise<void> {
        for (const topic of topics) {
            const key = `topic:${topic.topic}`;
            if (this.topology.has(key)) {
                await this.topology.get(key);
                continue;
            }
            const promise = this.createTopic(topic).finally(() => this.topology.delete(key));
            this.topology.set(key, promise);
            try {
                await promise;
            } catch (error) {
                this.topology.delete(key);
                throw error;
            }
        }
    }

    /** Alias for `applyTopics` to maintain backwards compatibility. */
    async ensureTopics(topics: KafkaTopicConfig[]): Promise<void> {
        return this.applyTopics(topics);
    }

    async ensureTopic(topic: string, options: Partial<KafkaTopicDefinition> = {}): Promise<void> {
        const defaultPartitions = resolveDefaultPartitions(this.options);
        const defaultReplication = resolveDefaultReplication(this.options);
        const defaultMinIsr = resolveDefaultMinIsr(this.options, options.replicationFactor ?? defaultReplication);

        await this.applyTopics([
            {
                topic,
                numPartitions: options.numPartitions ?? defaultPartitions,
                replicationFactor: options.replicationFactor ?? defaultReplication,
                retentionMs: options.retentionMs,
                minInSyncReplicas: options.minInSyncReplicas ?? defaultMinIsr,
                cleanupPolicy: options.cleanupPolicy,
                configEntries: options.configEntries
            }
        ]);
    }

    private async createTopic(config: KafkaTopicDefinition): Promise<void> {
        const exists = await this.admin
            .fetchTopicMetadata({ topics: [config.topic] })
            .then(() => true)
            .catch(() => false);
        if (exists) return;

        const configEntries = [
            ...(config.retentionMs !== undefined ? [{ name: 'retention.ms', value: String(config.retentionMs) }] : []),
            ...(config.minInSyncReplicas !== undefined ? [{ name: 'min.insync.replicas', value: String(config.minInSyncReplicas) }] : []),
            ...(config.cleanupPolicy !== undefined ? [{ name: 'cleanup.policy', value: config.cleanupPolicy }] : []),
            ...(config.configEntries ?? [])
        ];

        try {
            await this.admin.createTopics({
                topics: [
                    {
                        topic: config.topic,
                        numPartitions: config.numPartitions,
                        replicationFactor: config.replicationFactor,
                        configEntries
                    }
                ]
            });
        } catch (error) {
            // May have been created concurrently by another service.
            const stillMissing = await this.admin
                .fetchTopicMetadata({ topics: [config.topic] })
                .then(() => false)
                .catch(() => true);
            if (stillMissing) throw error;
        }
    }

    async offsets(topic: string): Promise<Record<string, string>> {
        const metadata = await this.admin.fetchTopicOffsets(topic);
        return Object.fromEntries(metadata.map((entry) => [String(entry.partition), entry.offset ?? '']));
    }

    async listGroups(): Promise<GroupMemberDescription[]> {
        const { groups } = await this.admin.listGroups();
        const result: GroupMemberDescription[] = [];
        for (const group of groups) {
            const description = await this.admin.describeGroups([group.groupId]);
            const memberCount = description.groups?.[0]?.members?.length ?? 0;
            result.push({ groupId: group.groupId, members: memberCount });
        }
        return result;
    }

    async close(): Promise<void> {
        if (this.closed) return;
        this.closed = true;
        this.connected = false;
        await this.producer.disconnect().catch(() => undefined);
        await this.admin.disconnect().catch(() => undefined);
        this.topology.clear();
        this.handlers.onDisconnect?.();
        console.info(`[Kafka] Connection closed (clientId=${this.options.clientId})`);
    }
}

export function sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
}

export type KafkaConnectionLike = Pick<
    KafkaConnection,
    'client' | 'admin' | 'producer' | 'isReady' | 'ensureTopic' | 'ensureTopics' | 'connect' | 'close' | 'offsets' | 'listGroups' | 'clientId'
>;

export const KAFKA_TOPIC_DEFAULTS = {
    partitions: DEFAULT_TOPIC_PARTITIONS,
    replicationFactor: DEFAULT_TOPIC_REPLICATION
} as const;

function resolveDefaultPartitions(options: KafkaConnectionOptions): number {
    return (
        options.defaultNumPartitions ??
        (process.env.KAFKA_DEFAULT_NUM_PARTITIONS ? Number(process.env.KAFKA_DEFAULT_NUM_PARTITIONS) : undefined) ??
        DEFAULT_TOPIC_PARTITIONS
    );
}

function resolveDefaultReplication(options: KafkaConnectionOptions): number {
    return (
        options.defaultTopicReplication ??
        (process.env.KAFKA_DEFAULT_REPLICATION_FACTOR ? Number(process.env.KAFKA_DEFAULT_REPLICATION_FACTOR) : undefined) ??
        DEFAULT_TOPIC_REPLICATION
    );
}

function resolveDefaultMinIsr(options: KafkaConnectionOptions, replication: number): number | undefined {
    if (options.defaultMinInSyncReplicas !== undefined) return options.defaultMinInSyncReplicas;
    if (process.env.KAFKA_DEFAULT_MIN_INSYNC_REPLICAS) {
        return Number(process.env.KAFKA_DEFAULT_MIN_INSYNC_REPLICAS);
    }
    return replication > 1 ? 2 : undefined;
}
