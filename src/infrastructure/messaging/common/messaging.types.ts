/**
 * Common messaging contracts and types shared across all messaging transports (Kafka, RabbitMQ, etc.).
 */

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
    /** Optional message kind override ('event' | 'command'). Defaults to transport default. */
    kind?: MessageKind;
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

export type MessageHandler<T = unknown, TRaw = unknown> = (
    payload: T,
    rawMessage?: TRaw,
    envelope?: MessageEnvelope<T>
) => Promise<void>;

/**
 * Classifies whether an error is permanent (e.g. schema invalid, unparseable payload,
 * validation violation, syntax/type error). Permanent errors must not be retried with
 * delay/backoff; they should be forwarded directly to the DLQ and their message acknowledged/committed.
 */
export function isPermanentError(error: unknown): boolean {
    if (!error || typeof error !== 'object') return false;
    if ('isPermanent' in error && (error as any).isPermanent === true) return true;
    if ('isRetriable' in error && (error as any).isRetriable === false) return true;
    if ('retriable' in error && (error as any).retriable === false) return true;
    if (error instanceof SyntaxError || error instanceof TypeError || error instanceof RangeError) {
        return true;
    }
    const code = (error as any).code;
    if (code === 'VALIDATION_ERROR' || code === 'INVALID_PAYLOAD' || code === 'SCHEMA_MISMATCH') {
        return true;
    }
    const message = (error as any).message;
    if (typeof message === 'string') {
        return /validation|schema|syntax|malformed|unparseable|cannot be null/i.test(message);
    }
    return false;
}
