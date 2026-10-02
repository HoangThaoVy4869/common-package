import { IMessageBus } from '../../core/interfaces';
import { RabbitMQConnection } from './RabbitMQConnection';
import { InMemoryMessageInbox } from './rabbitmq/in-memory-message.inbox';
import { RabbitMQConsumer } from './rabbitmq/rabbitmq.consumer';
import { createEnvelope, publishEnvelope, sendEnvelope } from './rabbitmq/rabbitmq.message';
import { MessageHandler, MessageInbox, PublishOptions, RabbitMQEventBusOptions, SubscriptionOptions } from './rabbitmq/rabbitmq.types';

export * from './RabbitMQConnection';
export * from './rabbitmq/rabbitmq.types';
export * from './rabbitmq/in-memory-message.inbox';

export class RabbitMQEventBus implements IMessageBus {
    private readonly inbox: MessageInbox;
    private readonly drainTimeoutMs: number;
    private readonly consumers: RabbitMQConsumer[] = [];
    private closed = false;

    constructor(
        readonly connection: RabbitMQConnection,
        options: RabbitMQEventBusOptions = {}
    ) {
        this.inbox = options.inbox ?? new InMemoryMessageInbox();
        this.drainTimeoutMs = options.drainTimeoutMs ?? 30_000;
    }

    async publishEvent<T>(eventName: string, payload: T, options: PublishOptions = {}): Promise<void> {
        this.requireReady();
        await this.connection.declareExchange(eventName);
        await publishEnvelope(this.connection.publisher, eventName, '', createEnvelope('event', eventName, payload, options), {
            persistent: options.persistent ?? true
        });
    }

    async sendCommand<T>(queueName: string, payload: T, options: PublishOptions = {}): Promise<void> {
        this.requireReady();
        await this.connection.declareQueue(queueName);
        await sendEnvelope(this.connection.publisher, queueName, createEnvelope('command', queueName, payload, options), {
            persistent: options.persistent ?? true
        });
    }

    async subscribeEvent<T>(eventName: string, handler: MessageHandler<T>, options: SubscriptionOptions = {}): Promise<void> {
        await this.startConsumer('event', eventName, handler as MessageHandler, options);
    }

    async handleCommand<T>(queueName: string, handler: MessageHandler<T>, options: SubscriptionOptions = {}): Promise<void> {
        await this.startConsumer('command', queueName, handler as MessageHandler, {
            ...options,
            queueName
        });
    }

    private async startConsumer(kind: 'event' | 'command', name: string, handler: MessageHandler, options: SubscriptionOptions): Promise<void> {
        this.requireReady();
        const consumer = new RabbitMQConsumer(this.connection, this.inbox, kind, name, handler, options, this.consumers.length + 1);
        await consumer.start();
        this.consumers.push(consumer);
    }

    async close(options: { drainTimeoutMs?: number } = {}): Promise<void> {
        if (this.closed) return;
        this.closed = true;
        const timeout = options.drainTimeoutMs ?? this.drainTimeoutMs;
        await Promise.all(this.consumers.map((consumer) => consumer.stop(timeout)));
        this.consumers.length = 0;
        console.info('[MQ] Event bus closed');
    }

    private requireReady(): void {
        if (this.closed || !this.connection.isReady) {
            throw new Error('RabbitMQ is not ready; connect the shared RabbitMQConnection first');
        }
    }
}
