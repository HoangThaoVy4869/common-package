/**
 * RabbitMQ Codec & Topology Helpers.
 *
 * Consolidates AMQP envelope serialization/deserialization and subscription topology
 * declaration (exchange binding, retry queues, and dead-letter queues).
 */

import { randomUUID } from 'node:crypto';
import { once } from 'node:events';
import { ConfirmChannel, Message, Options } from 'amqplib';
import { ChannelWrapper } from 'amqp-connection-manager';
import type { MessageEnvelope, MessageKind, SubscriptionOptions } from '@messaging-common';

/** Re-export createEnvelope helper for convenient access from the RabbitMQ module */
export { createEnvelope } from '@messaging-common';

/* =========================================================================
 * 1. Topology Declaration (Exchanges, Queues, DLQ)
 * ========================================================================= */

export interface SubscriptionTopology {
    queue: string;
    retryQueue: string;
    deadLetterQueue: string;
}

export interface TopologySubscription {
    kind: MessageKind;
    name: string;
    options: SubscriptionOptions;
    alternateExchange?: string;
    logger?: { warn(msg: string): void };
}

/**
 * Asserts exchanges, main queue, retry queue (with TTL dead-letter-exchange), and DLQ.
 */
export async function declareSubscriptionTopology(
    channel: ConfirmChannel,
    subscription: TopologySubscription
): Promise<SubscriptionTopology> {
    const durableName = subscription.options.queueName;
    const retry = subscription.options.retry ?? {};
    const maxRetries = retry.maxRetries ?? 3;

    if (subscription.kind === 'event') {
        await channel.assertExchange(subscription.name, 'fanout', {
            durable: true,
            ...(subscription.alternateExchange && { alternateExchange: subscription.alternateExchange })
        });
    }

    if (!durableName) {
        if (maxRetries > 0) {
            if (subscription.logger) {
                subscription.logger.warn(`[MQ] Retry/DLQ disabled for anonymous subscription ${subscription.name}; set queueName to enable it`);
            } else if (process.env.NODE_ENV !== 'test') {
                console.warn(`[MQ] Retry/DLQ disabled for anonymous subscription ${subscription.name}; set queueName to enable it`);
            }
        }
        const reply = await channel.assertQueue('', { exclusive: true, autoDelete: true });
        if (subscription.kind === 'event') await channel.bindQueue(reply.queue, subscription.name, '');
        return { queue: reply.queue, retryQueue: '', deadLetterQueue: '' };
    }

    const retryQueue = `${durableName}.retry`;
    const deadLetterQueue = retry.deadLetterQueue ?? `${durableName}.dlq`;
    await channel.assertQueue(durableName, { durable: true });
    await channel.assertQueue(deadLetterQueue, { durable: true });

    if (maxRetries > 0) {
        await channel.assertQueue(retryQueue, {
            durable: true,
            arguments: {
                'x-message-ttl': retry.retryDelayMs ?? 5_000,
                // Retry only this subscriber; a fanout DLX would duplicate healthy consumers.
                'x-dead-letter-exchange': '',
                'x-dead-letter-routing-key': durableName
            }
        });
    }

    if (subscription.kind === 'event') await channel.bindQueue(durableName, subscription.name, '');
    return { queue: durableName, retryQueue, deadLetterQueue };
}

/* =========================================================================
 * 2. Message Serialization & Deserialization
 * ========================================================================= */

/**
 * Parses raw AMQP message content into a typed MessageEnvelope.
 */
export function parseEnvelope(message: Message, fallbackKind: MessageKind, fallbackName: string): MessageEnvelope {
    let parsed: unknown;
    try {
        parsed = JSON.parse(message.content.toString('utf8'));
    } catch {
        parsed = undefined;
    }

    if (
        parsed &&
        typeof parsed === 'object' &&
        typeof (parsed as MessageEnvelope).id === 'string' &&
        typeof (parsed as MessageEnvelope).kind === 'string' &&
        'payload' in parsed
    ) {
        return parsed as MessageEnvelope;
    }

    return {
        id: message.properties.messageId ?? randomUUID(),
        kind: fallbackKind,
        name: fallbackName,
        occurredAt: new Date().toISOString(),
        payload: parsed
    };
}

/**
 * Publishes an envelope to an exchange via a ChannelWrapper.
 */
export async function publishEnvelope(
    channel: ChannelWrapper,
    exchange: string,
    routingKey: string,
    envelope: MessageEnvelope,
    options: Options.Publish
): Promise<void> {
    await channel.publish(exchange, routingKey, Buffer.from(JSON.stringify(envelope)), {
        ...options,
        contentType: 'application/json',
        messageId: envelope.id,
        correlationId: envelope.correlationId,
        headers: { ...(options.headers || {}), 'x-message-kind': envelope.kind, 'x-message-name': envelope.name }
    });
}

/**
 * Sends an envelope directly to a queue via a ChannelWrapper.
 */
export async function sendEnvelope(
    channel: ChannelWrapper,
    queue: string,
    envelope: MessageEnvelope,
    options: Options.Publish
): Promise<void> {
    await channel.sendToQueue(queue, Buffer.from(JSON.stringify(envelope)), {
        ...options,
        contentType: 'application/json',
        messageId: envelope.id,
        correlationId: envelope.correlationId,
        headers: { ...(options.headers || {}), 'x-message-kind': envelope.kind, 'x-message-name': envelope.name }
    });
}

/**
 * Sends raw bytes to a queue with publisher confirmation and backpressure drain handling.
 */
export async function confirmedRawSendToQueue(
    channel: ConfirmChannel,
    queue: string,
    body: Buffer,
    options: Options.Publish
): Promise<void> {
    let writable = true;
    const confirmation = new Promise<void>((resolve, reject) => {
        writable = channel.sendToQueue(queue, body, options, (error) => {
            if (error) reject(error);
            else resolve();
        });
    });
    if (writable) return confirmation;
    await Promise.all([confirmation, once(channel, 'drain')]);
}
