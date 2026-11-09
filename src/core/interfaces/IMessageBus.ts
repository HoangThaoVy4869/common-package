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
 * Event Publisher (Outbound):
 * Publishes/broadcasts domain or integration events.
 */
export interface IEventPublisher {
    publishEvent<T = unknown>(eventName: string, payload: T, options?: unknown): Promise<void>;
}

/**
 * Event Subscriber (Inbound):
 * Subscribes to domain or integration events.
 */
export interface IEventSubscriber<TOptions = any> {
    subscribeEvent<T = unknown>(
        eventName: string,
        handler: BusMessageHandler<T, any, any>,
        options?: TOptions
    ): Promise<void>;
}

/**
 * Event Bus (Pub / Sub):
 * Broadcasts domain/integration events to multiple subscribers (1-to-Many).
 * Implemented by: KafkaEventBus, RabbitMQEventBus.
 */
export interface IEventBus<TOptions = any> extends IEventPublisher, IEventSubscriber<TOptions> {
    publishEvent<T = unknown>(eventName: string, payload: T, options?: unknown): Promise<void>;
    subscribeEvent<T = unknown>(
        eventName: string,
        handler: BusMessageHandler<T, any, any>,
        options?: TOptions
    ): Promise<void>;
}

/**
 * Command Bus (Task / Job Queue):
 * Dispatches point-to-point commands to a single worker queue (1-to-1).
 * Implemented by: RabbitMQEventBus.
 */
export interface ICommandBus<TOptions = any> {
    sendCommand<T = unknown>(queueName: string, payload: T, options?: unknown): Promise<void>;
    handleCommand<T = unknown>(
        queueName: string,
        handler: BusMessageHandler<T, any, any>,
        options?: TOptions
    ): Promise<void>;
}

/**
 * Unified Message Bus:
 * Combines fan-out events and point-to-point commands.
 * Implemented by brokers supporting both paradigms (RabbitMQEventBus).
 */
export interface IMessageBus<TEventOptions = any, TCmdOptions = any> extends IEventBus<TEventOptions>, ICommandBus<TCmdOptions> { }

export const EVENT_BUS = 'EVENT_BUS';
