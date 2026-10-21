/**
 * Generic message handler callback.
 * Receives the typed payload, optional raw broker message, and standardized message envelope.
 */
export type BusMessageHandler<T = unknown, M = unknown, E = unknown> = (
    payload: T,
    rawMessage?: M,
    envelope?: E
) => Promise<void> | void;

/**
 * Event Bus (Pub / Sub):
 * Broadcasts domain/integration events to multiple subscribers (1-to-Many).
 * Implemented by: KafkaEventBus, RabbitMQEventBus.
 */
export interface IEventBus {
    /** Publish a fan-out event. */
    publishEvent<T = unknown>(eventName: string, payload: T, options?: unknown): Promise<void>;
    /** Subscribe to a fan-out event. */
    subscribeEvent<T = unknown>(
        eventName: string,
        handler: BusMessageHandler<T, any, any>,
        options?: unknown
    ): Promise<void>;
}

/**
 * Command Bus (Task / Job Queue):
 * Dispatches point-to-point commands to a single worker queue (1-to-1).
 * Implemented by: RabbitMQEventBus.
 */
export interface ICommandBus {
    /** Send a point-to-point command to a target queue. */
    sendCommand<T = unknown>(queueName: string, payload: T, options?: unknown): Promise<void>;
    /** Register a handler for a point-to-point command queue. */
    handleCommand<T = unknown>(
        queueName: string,
        handler: BusMessageHandler<T, any, any>,
        options?: unknown
    ): Promise<void>;
}

/**
 * Unified Message Bus:
 * Combines fan-out events and point-to-point commands.
 * Implemented by brokers supporting both paradigms (RabbitMQEventBus).
 */
export interface IMessageBus extends IEventBus, ICommandBus { }

export const EVENT_BUS = 'EVENT_BUS';
