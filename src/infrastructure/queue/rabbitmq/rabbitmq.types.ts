import { Message } from 'amqplib';

export type MessageKind = 'event' | 'command';

export interface MessageEnvelope<T = unknown> {
    id: string;
    kind: MessageKind;
    name: string;
    occurredAt: string;
    payload: T;
    correlationId?: string;
    causationId?: string;
    metadata?: Record<string, unknown>;
}

export interface PublishOptions {
    persistent?: boolean;
    messageId?: string;
    correlationId?: string;
    causationId?: string;
    metadata?: Record<string, unknown>;
    headers?: Record<string, unknown>;
}

export interface RetryPolicy {
    maxRetries?: number;
    retryDelayMs?: number;
    deadLetterQueue?: string;
}

export interface SubscriptionOptions {
    queueName?: string;
    prefetch?: number;
    retry?: RetryPolicy;
}

export interface MessageInbox {
    claim(messageId: string): Promise<boolean>;
    complete(messageId: string): Promise<void>;
    release(messageId: string): Promise<void>;
}

export type MessageHandler<T = unknown> = (payload: T, rawMessage?: Message, envelope?: MessageEnvelope<T>) => Promise<void>;

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
    inbox?: MessageInbox;
    drainTimeoutMs?: number;
}
