import { AmqpConnectionManager, ChannelWrapper, CreateChannelOpts, SetupFunc, connect } from 'amqp-connection-manager';
import { RabbitMQConnectionOptions } from './rabbitmq/rabbitmq.types';

export type { RabbitMQConnectionOptions, AlternateExchangeOptions } from './rabbitmq/rabbitmq.types';

export const DEFAULT_ALTERNATE_EXCHANGE = 'unrouted.events.exchange';
export const DEFAULT_UNROUTED_EVENTS_QUEUE = 'unrouted.events.q';

export class RabbitMQConnection {
    private readonly manager: AmqpConnectionManager;
    private readonly connectTimeoutMs: number;
    private readonly topology = new Map<string, { setup: SetupFunc; promise: Promise<void> }>();
    private closed = false;

    readonly publisher: ChannelWrapper;
    readonly alternateExchangeConfig: {
        enabled: boolean;
        exchangeName: string;
        queueName: string;
    };

    constructor(options: RabbitMQConnectionOptions = {}) {
        const urls = options.urls?.length ? options.urls : [options.url || process.env.RABBITMQ_URL || 'amqp://localhost'];
        this.connectTimeoutMs = options.connectTimeoutMs ?? 30_000;
        this.alternateExchangeConfig = {
            enabled: options.alternateExchange?.enabled ?? true,
            exchangeName: options.alternateExchange?.exchangeName || DEFAULT_ALTERNATE_EXCHANGE,
            queueName: options.alternateExchange?.queueName || DEFAULT_UNROUTED_EVENTS_QUEUE
        };

        this.manager = connect(urls, {
            heartbeatIntervalInSeconds: options.heartbeatIntervalInSeconds ?? 5,
            reconnectTimeInSeconds: options.reconnectTimeInSeconds
        });
        this.publisher = this.manager.createChannel({
            name: 'rabbitmq-publisher',
            confirm: true,
            publishTimeout: options.publishTimeoutMs ?? 30_000
        });

        if (this.alternateExchangeConfig.enabled) {
            this.publisher.addSetup(async (channel: any) => {
                const { exchangeName, queueName } = this.alternateExchangeConfig;
                await channel.assertExchange(exchangeName, 'fanout', { durable: true });
                await channel.assertQueue(queueName, { durable: true });
                await channel.bindQueue(queueName, exchangeName, '');
            });
        }

        this.manager.on('connect', () => console.info('[MQ] Connected'));
        this.manager.on('connectFailed', ({ err }) => console.error('[MQ] Connection attempt failed', err));
        this.manager.on('disconnect', ({ err }) => console.warn('[MQ] Disconnected; reconnecting', err));
        this.manager.on('blocked', ({ reason }) => console.warn(`[MQ] Connection blocked: ${reason}`));
        this.manager.on('unblocked', () => console.info('[MQ] Connection unblocked'));
        this.publisher.on('error', (error) => console.error('[MQ] Publisher channel error', error));
    }

    get isReady(): boolean {
        return !this.closed && this.manager.isConnected();
    }

    get channelCount(): number {
        return this.manager.channelCount;
    }

    async connect(timeoutMs = this.connectTimeoutMs): Promise<void> {
        if (this.closed) throw new Error('RabbitMQ connection is closed');
        await this.manager.connect({ timeout: timeoutMs });
        await this.publisher.waitForConnect();
    }

    createChannel(options: CreateChannelOpts): ChannelWrapper {
        if (this.closed) throw new Error('RabbitMQ connection is closed');
        return this.manager.createChannel(options);
    }

    async declareExchange(name: string, type = 'fanout'): Promise<void> {
        await this.declare(`exchange:${type}:${name}`, async (channel) => {
            const isAe = this.alternateExchangeConfig.enabled && name !== this.alternateExchangeConfig.exchangeName;
            await channel.assertExchange(name, type, {
                durable: true,
                ...(isAe && { alternateExchange: this.alternateExchangeConfig.exchangeName })
            });
        });
    }

    async declareQueue(name: string): Promise<void> {
        await this.declare(`queue:${name}`, async (channel) => {
            await channel.assertQueue(name, { durable: true });
        });
    }

    private async declare(key: string, setup: SetupFunc): Promise<void> {
        const existing = this.topology.get(key);
        if (existing) return existing.promise;

        const promise = this.publisher.addSetup(setup);
        this.topology.set(key, { setup, promise });
        try {
            await promise;
        } catch (error) {
            this.topology.delete(key);
            await this.publisher.removeSetup(setup).catch(() => undefined);
            throw error;
        }
    }

    async close(): Promise<void> {
        if (this.closed) return;
        this.closed = true;
        this.topology.clear();
        await this.publisher.close();
        await this.manager.close();
    }
}
