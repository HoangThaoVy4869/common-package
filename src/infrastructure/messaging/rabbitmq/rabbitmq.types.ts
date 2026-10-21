import { Message } from 'amqplib';
import type {
    MessageEnvelope,
    MessageKind,
    PublishOptions,
    RetryPolicy,
    SubscriptionOptions,
    MessageInbox,
    MessageHandler as CommonMessageHandler
} from '@messaging-common';

// Re-export common contracts
export type {
    MessageEnvelope,
    MessageKind,
    PublishOptions,
    RetryPolicy,
    SubscriptionOptions,
    MessageInbox
};

export type RabbitMQMessageHandler<T = unknown> = CommonMessageHandler<T, Message>;
export type MessageHandler<T = unknown> = RabbitMQMessageHandler<T>;

export interface AlternateExchangeOptions {
    enabled?: boolean;
    exchangeName?: string;
    queueName?: string;
}

export interface RabbitMQConnectionOptions {
    url?: string;
    urls?: string[];
    connectTimeoutMs?: number;
    heartbeatIntervalInSeconds?: number;
    reconnectTimeInSeconds?: number;
    publishTimeoutMs?: number;
    alternateExchange?: AlternateExchangeOptions;
}

export interface RabbitMQEventBusOptions {
    /**
     * @deprecated Idempotency is handled at the application/service level (e.g. via DB unique constraints, transactional outbox/inbox, or RedisIdempotencyRepository).
     */
    inbox?: MessageInbox;
    drainTimeoutMs?: number;
}
