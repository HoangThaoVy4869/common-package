/**
 * Unified Messaging Surface (Kafka, RabbitMQ & Common Contracts)
 *
 * Supports independent messaging infrastructures:
 * - Common: Shared contracts (MessageEnvelope, MessageKind, PublishOptions, MessageInbox, etc.)
 * - KafkaEventBus: High-throughput event streaming / integration events
 * - RabbitMQEventBus: Point-to-point commands, queues, and task dispatching
 */

// Shared Common Contracts & Helpers
export * from './common';

// Kafka Components & Types
export * from './kafka/KafkaConnection';
export * from './kafka/KafkaProducer';
export * from './kafka/KafkaConsumer';
export * from './kafka/KafkaEventBus';
export * from './kafka/kafka.types';
export {
    KAFKA_ENVELOPE_SCHEMA,
    KAFKA_HEADERS,
    DEFAULT_PARTITION_KEY_FIELD,
    envelopeToHeaders,
    headersToIHeaders,
    retryCountFromHeaders,
    readBufferHeader,
    toIHeaders,
    jsonReplacer,
    safeSerializeEnvelope,
    buildProduceMessages,
    toProduceResult,
    buildMessageContext,
    parseEnvelope as parseKafkaEnvelope
} from './kafka/kafka.codec';

// RabbitMQ Components & Types
export * from './rabbitmq/RabbitMQConnection';
export * from './rabbitmq/rabbitmq.consumer';
export * from './rabbitmq/RabbitMQEventBus';
export type {
    AlternateExchangeOptions,
    RabbitMQConnectionOptions,
    RabbitMQEventBusOptions,
    RabbitMQMessageHandler
} from './rabbitmq/rabbitmq.types';
export * from './rabbitmq/rabbitmq.codec';

// Namespaced submodules for modular access
export * as common from './common';
export * as kafka from './kafka';
export * as rabbitmq from './rabbitmq';
