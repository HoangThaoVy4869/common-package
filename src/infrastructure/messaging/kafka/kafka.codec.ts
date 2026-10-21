/**
 * Kafka message codec.
 *
 * Provides serialization (encode) for Kafka producers and deserialization (decode)
 * for Kafka consumers. Bridges the standardized `MessageEnvelope` to Kafka records
 * with tracing headers and partition-key support.
 */

import { randomUUID } from 'node:crypto';
import type { MessageEnvelope, MessageKind } from '@messaging-common';
import type { IHeaders, RecordMetadata, KafkaMessageContext } from '@kafka/kafka.types';

/* =========================================================================
 * 1. Constants, Schema & Re-exports
 * ========================================================================= */

/** Re-export createEnvelope helper for convenient access from the Kafka module */
export { createEnvelope } from '@messaging-common';

/** Schema identifier stamped into record headers for schema evolution */
export const KAFKA_ENVELOPE_SCHEMA = 'microservice.event-envelope.v1';

/** Standardized Kafka header keys carrying trace and metadata */
export const KAFKA_HEADERS = {
    MESSAGE_ID: 'x-event-bus-id',
    MESSAGE_KIND: 'x-message-kind',
    MESSAGE_NAME: 'x-message-name',
    CORRELATION_ID: 'x-correlation-id',
    CAUSATION_ID: 'x-causation-id',
    OCCURRED_AT: 'x-occurred-at',
    RETRY_COUNT: 'x-event-bus-retry-count',
    SCHEMA: 'x-event-bus-schema',
    PARTITION_KEY: 'x-event-bus-partition-key'
} as const;

/** Default payload property name used to extract partition key */
export const DEFAULT_PARTITION_KEY_FIELD = 'aggregateId';

/* =========================================================================
 * 2. PRODUCER (ENCODE) - Encoding Envelopes into Kafka Records
 * ========================================================================= */

/**
 * Maps metadata from a `MessageEnvelope` into plain string headers.
 */
export function envelopeToHeaders(envelope: MessageEnvelope): Record<string, string> {
    return {
        [KAFKA_HEADERS.MESSAGE_ID]: envelope.id,
        [KAFKA_HEADERS.MESSAGE_KIND]: envelope.kind as string,
        [KAFKA_HEADERS.MESSAGE_NAME]: envelope.name,
        [KAFKA_HEADERS.OCCURRED_AT]: envelope.occurredAt,
        [KAFKA_HEADERS.SCHEMA]: KAFKA_ENVELOPE_SCHEMA,
        ...(envelope.correlationId ? { [KAFKA_HEADERS.CORRELATION_ID]: envelope.correlationId } : {}),
        ...(envelope.causationId ? { [KAFKA_HEADERS.CAUSATION_ID]: envelope.causationId } : {})
    };
}

/**
 * Converts a plain string-header map into Kafka `IHeaders` (Buffer values).
 */
export function headersToIHeaders(headers: Record<string, string>): IHeaders {
    return Object.fromEntries(Object.entries(headers).map(([key, value]) => [key, Buffer.from(value)]));
}

/**
 * Converts an array of [key, Buffer] header entries into Kafka `IHeaders`.
 */
export function toIHeaders(entries: Array<[string, Buffer]>): IHeaders {
    return Object.fromEntries(entries.map(([key, value]) => [key, value]));
}

/**
 * Custom JSON replacer supporting BigInt serialization.
 */
export function jsonReplacer(_key: string, value: unknown): unknown {
    if (typeof value === 'bigint') {
        return value.toString();
    }
    return value;
}

/**
 * Safely serializes an envelope into a UTF-8 Buffer with BigInt support.
 * Throws a descriptive error if serialization fails (e.g. circular structure).
 */
export function safeSerializeEnvelope<T>(envelope: MessageEnvelope<T>): Buffer {
    try {
        const json = JSON.stringify(envelope, jsonReplacer);
        return Buffer.from(json, 'utf8');
    } catch (error) {
        throw new Error(
            `[KafkaCodec] Failed to serialize message envelope "${envelope.name}" (id=${envelope.id}): ${
                error instanceof Error ? error.message : String(error)
            }`
        );
    }
}

/**
 * Serializes an array of `MessageEnvelope` instances into Kafka producer records.
 * Resolves partition key based on explicit key, custom resolver, default aggregateId field, or fallback UUID.
 */
export function buildProduceMessages<T>(
    envelopes: Array<MessageEnvelope<T>>,
    options: {
        partitionKey?: (eventName: string, payload: unknown) => string | undefined;
        defaultField?: string;
        explicitKeys?: Array<string | undefined>;
    }
): Array<{ key?: string; value: Buffer; headers: IHeaders }> {
    return envelopes.map((envelope, index) => {
        let key: string | undefined = options.explicitKeys?.[index];
        if (key === undefined && options.partitionKey) {
            key = options.partitionKey(envelope.name, envelope.payload);
        }
        if (key === undefined && envelope.payload && typeof envelope.payload === 'object') {
            const record = envelope.payload as Record<string, unknown>;
            const fallback = record[options.defaultField ?? DEFAULT_PARTITION_KEY_FIELD];
            if (typeof fallback === 'string' && fallback.length > 0) key = fallback;
            else if (typeof fallback === 'number') key = String(fallback);
        }
        const partitionKey = key ?? randomUUID();

        const headers = envelopeToHeaders(envelope);
        headers[KAFKA_HEADERS.PARTITION_KEY] = partitionKey;
        return {
            key: partitionKey,
            value: safeSerializeEnvelope(envelope),
            headers: headersToIHeaders(headers)
        };
    });
}

/**
 * Normalizes Kafka RecordMetadata returned by the broker into our structured result shape.
 */
export function toProduceResult(metadata: RecordMetadata[], fallbackTopic: string): { topic: string; partition: number; offset: string } {
    const first = metadata[0];
    if (!first) return { topic: fallbackTopic, partition: 0, offset: '' };
    return {
        topic: first.topicName ?? fallbackTopic,
        partition: first.partition,
        offset: first.baseOffset ?? ''
    };
}

/* =========================================================================
 * 3. CONSUMER (DECODE) - Decoding Kafka Records into Envelopes & Context
 * ========================================================================= */

/**
 * Safely extracts the first string value from an `IHeaders` entry (string or Buffer).
 */
function firstHeaderValue(headers: IHeaders | undefined, key: string): string | undefined {
    const value = headers?.[key];
    if (value === null || value === undefined) return undefined;
    const values = Array.isArray(value) ? value : [value];
    if (values.length === 0) return undefined;
    const first = values[0];
    if (first === null || first === undefined) return undefined;
    return Buffer.isBuffer(first) ? first.toString('utf8') : String(first);
}

/**
 * Reads a buffer-valued header as a string (public helper for reading trace/keys).
 */
export function readBufferHeader(headers: IHeaders | undefined, key: string): string | undefined {
    return firstHeaderValue(headers, key);
}

/**
 * Extracts and parses the retry count from Kafka message headers.
 */
export function retryCountFromHeaders(headers: IHeaders | undefined): number {
    const raw = firstHeaderValue(headers, KAFKA_HEADERS.RETRY_COUNT);
    const parsed = raw !== undefined ? Number(raw) : NaN;
    return Number.isSafeInteger(parsed) && parsed >= 0 ? parsed : 0;
}

/**
 * Deserializes a raw Kafka record value (Buffer) and headers into a typed `MessageEnvelope<T>`.
 * Handles envelopes with pre-existing envelope shape or wraps raw JSON payloads gracefully.
 */
export function parseEnvelope<T = unknown>(
    rawValue: Buffer | null | undefined,
    headers: IHeaders | undefined,
    fallbackKind: MessageKind,
    fallbackName: string
): MessageEnvelope<T> {
    let parsed: unknown;
    let parseError: string | undefined;
    try {
        parsed = rawValue ? JSON.parse(rawValue.toString('utf8')) : undefined;
    } catch (err) {
        parsed = undefined;
        parseError = err instanceof Error ? err.message : String(err);
    }

    const hasEnvelopeShape =
        parsed &&
        typeof parsed === 'object' &&
        typeof (parsed as MessageEnvelope).id === 'string' &&
        typeof (parsed as MessageEnvelope).kind === 'string' &&
        'payload' in parsed;

    if (hasEnvelopeShape) {
        const env = parsed as MessageEnvelope<T>;
        return {
            ...env,
            correlationId: env.correlationId ?? firstHeaderValue(headers, KAFKA_HEADERS.CORRELATION_ID),
            causationId: env.causationId ?? firstHeaderValue(headers, KAFKA_HEADERS.CAUSATION_ID)
        };
    }

    const rawString = rawValue ? rawValue.toString('utf8') : undefined;

    return {
        id: firstHeaderValue(headers, KAFKA_HEADERS.MESSAGE_ID) ?? randomUUID(),
        kind: (firstHeaderValue(headers, KAFKA_HEADERS.MESSAGE_KIND) as MessageKind | undefined) ?? fallbackKind,
        name: firstHeaderValue(headers, KAFKA_HEADERS.MESSAGE_NAME) ?? fallbackName,
        occurredAt: firstHeaderValue(headers, KAFKA_HEADERS.OCCURRED_AT) ?? new Date().toISOString(),
        payload: (parseError ? { raw: rawString, parseError } : parsed) as T,
        correlationId: firstHeaderValue(headers, KAFKA_HEADERS.CORRELATION_ID),
        causationId: firstHeaderValue(headers, KAFKA_HEADERS.CAUSATION_ID),
        metadata: parseError ? { parseError, isPoisonPill: true, raw: rawString } : undefined
    };
}

/**
 * Constructs a `KafkaMessageContext` containing the decoded envelope and full broker metadata
 * (topic, partition, offset, key, parsed headers) for consumers and handlers.
 */
export function buildMessageContext<T>(
    envelope: MessageEnvelope<T>,
    topic: string,
    partition: number,
    offset: string,
    key: string | undefined,
    headers: IHeaders | undefined
): KafkaMessageContext<T> {
    const headerMap: Record<string, string> = {};
    for (const [name, value] of Object.entries(headers ?? {})) {
        if (value !== null && value !== undefined) {
            const values = Array.isArray(value) ? value : [value];
            const first = values.find((v): v is Buffer => Buffer.isBuffer(v));
            if (first) headerMap[name] = first.toString('utf8');
        }
    }
    return { envelope, topic, partition, offset, key, headers: headerMap };
}