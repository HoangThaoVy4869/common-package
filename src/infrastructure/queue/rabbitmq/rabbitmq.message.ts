import { randomUUID } from 'node:crypto';
import { once } from 'node:events';
import { ConfirmChannel, Message, Options } from 'amqplib';
import { ChannelWrapper } from 'amqp-connection-manager';
import { MessageEnvelope, MessageKind, PublishOptions } from './rabbitmq.types';

export function createEnvelope<T>(kind: MessageKind, name: string, payload: T, options: PublishOptions): MessageEnvelope<T> {
    return {
        id: options.messageId ?? randomUUID(),
        kind,
        name,
        occurredAt: new Date().toISOString(),
        payload,
        correlationId: options.correlationId,
        causationId: options.causationId,
        metadata: options.metadata
    };
}

export function parseEnvelope(message: Message, fallbackKind: MessageKind, fallbackName: string): MessageEnvelope {
    const parsed = JSON.parse(message.content.toString()) as unknown;
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
        headers: { 'x-message-kind': envelope.kind, 'x-message-name': envelope.name }
    });
}

export async function sendEnvelope(channel: ChannelWrapper, queue: string, envelope: MessageEnvelope, options: Options.Publish): Promise<void> {
    await channel.sendToQueue(queue, Buffer.from(JSON.stringify(envelope)), {
        ...options,
        contentType: 'application/json',
        messageId: envelope.id,
        correlationId: envelope.correlationId,
        headers: { 'x-message-kind': envelope.kind, 'x-message-name': envelope.name }
    });
}

export async function confirmedRawSendToQueue(channel: ConfirmChannel, queue: string, body: Buffer, options: Options.Publish): Promise<void> {
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
