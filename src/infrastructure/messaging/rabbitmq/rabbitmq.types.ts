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
import type { ICommandBus, IEventPublisher, IEventSubscriber, ILogger } from '@core/interfaces';

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

export interface RabbitMQSubscriptionOptions extends SubscriptionOptions {
    /** AMQP Queue name. */
    queueName?: string;
    /** Channel prefetch count. */
    prefetch?: number;
    /** Retry policy and DLQ routing. */
    retry?: RetryPolicy;
}

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
    /** Optional custom logger. Defaults to console. */
    logger?: ILogger;
}

export interface RabbitMQEventBusOptions {
    /**
     * @deprecated Idempotency is handled at the application/service level (e.g. via DB unique constraints, transactional outbox/inbox, or RedisIdempotencyRepository).
     */
    inbox?: MessageInbox;
    drainTimeoutMs?: number;
}

export interface IRabbitMQSubscriber extends IEventSubscriber<RabbitMQSubscriptionOptions> {
    subscribeEvent<T = unknown>(
        eventName: string,
        handler: RabbitMQMessageHandler<T>,
        options?: RabbitMQSubscriptionOptions
    ): Promise<void>;
}

export interface IRabbitMQCommandBus extends ICommandBus<RabbitMQSubscriptionOptions> {
    sendCommand<T = unknown>(queueName: string, payload: T, options?: PublishOptions): Promise<void>;
    handleCommand<T = unknown>(
        queueName: string,
        handler: RabbitMQMessageHandler<T>,
        options?: RabbitMQSubscriptionOptions
    ): Promise<void>;
}

export interface IRabbitMQEventBus extends IEventPublisher, IRabbitMQSubscriber, IRabbitMQCommandBus {}
