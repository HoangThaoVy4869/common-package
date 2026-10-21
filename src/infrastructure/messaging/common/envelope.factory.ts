import { randomUUID } from 'node:crypto';
import type { MessageEnvelope, MessageKind, PublishOptions } from './messaging.types';

/**
 * Creates a standardized MessageEnvelope used across all messaging transports.
 */
export function createEnvelope<T>(
    kind: MessageKind,
    name: string,
    payload: T,
    options: PublishOptions = {}
): MessageEnvelope<T> {
    return {
        id: options.messageId ?? randomUUID(),
        kind: options.kind ?? kind,
        name,
        occurredAt: new Date().toISOString(),
        payload,
        correlationId: options.correlationId,
        causationId: options.causationId,
        metadata: options.metadata
    };
}
