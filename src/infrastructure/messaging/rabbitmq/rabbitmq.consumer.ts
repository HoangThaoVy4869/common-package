import { ConfirmChannel, Message } from 'amqplib';
import { ChannelWrapper, SetupFunc } from 'amqp-connection-manager';
import { RabbitMQConnection } from '@rabbitmq/RabbitMQConnection';
import { confirmedRawSendToQueue, declareSubscriptionTopology, parseEnvelope, SubscriptionTopology } from '@rabbitmq/rabbitmq.codec';
import { isPermanentError, type MessageEnvelope, type MessageHandler, type MessageKind, type SubscriptionOptions } from '@messaging-common';

const RETRY_HEADER = 'x-event-bus-retry-count';

export class RabbitMQConsumer {
    private readonly channel: ChannelWrapper;
    private currentChannel: ConfirmChannel | null = null;
    private consumerTag: string | null = null;
    private inFlight = 0;
    private drainWaiters: Array<() => void> = [];

    constructor(
        private readonly connection: RabbitMQConnection,
        private readonly kind: MessageKind,
        private readonly name: string,
        private readonly handler: MessageHandler,
        private readonly options: SubscriptionOptions,
        id: number
    ) {
        this.channel = connection.createChannel({
            name: `rabbitmq-${kind}-${name}-${id}`,
            confirm: true
        });
        this.channel.on('error', (error) => {
            console.error(`[MQ] Consumer channel error ${kind}=${name}`, error);
        });
    }

    async start(): Promise<void> {
        const setup: SetupFunc = async (channel) => {
            const consumerChannel = channel as ConfirmChannel;
            await consumerChannel.prefetch(this.options.prefetch ?? 1);
            const aeName = this.connection.alternateExchangeConfig.enabled && this.name !== this.connection.alternateExchangeConfig.exchangeName
                ? this.connection.alternateExchangeConfig.exchangeName
                : undefined;
            const topology = await declareSubscriptionTopology(consumerChannel, {
                kind: this.kind,
                name: this.name,
                options: this.options,
                alternateExchange: aeName
            });
            const reply = await consumerChannel.consume(
                topology.queue,
                (message) => {
                    if (message) {
                        void this.consume(topology, consumerChannel, message).catch((error) => {
                            console.error(`[MQ] Consumer callback failed ${this.kind}=${this.name}`, error);
                        });
                    }
                },
                { noAck: false }
            );
            this.currentChannel = consumerChannel;
            this.consumerTag = reply.consumerTag;
        };

        try {
            await this.channel.addSetup(setup);
            await this.channel.waitForConnect();
        } catch (error) {
            await this.channel.close().catch(() => undefined);
            throw error;
        }
    }

    async stop(drainTimeoutMs: number): Promise<void> {
        if (this.currentChannel && this.consumerTag) {
            await this.currentChannel.cancel(this.consumerTag).catch((error) => {
                console.warn(`[MQ] Could not cancel consumer ${this.kind}=${this.name}`, error);
            });
        }
        await this.waitForDrain(drainTimeoutMs);
        await this.channel.close();
    }

    private async consume(topology: SubscriptionTopology, channel: ConfirmChannel, message: Message): Promise<void> {
        this.inFlight++;
        try {
            const envelope = parseEnvelope(message, this.kind, this.name);
            await this.handler(envelope.payload, message, envelope);
            channel.ack(message);
        } catch (error) {
            console.error(`[MQ] Handler failed for ${this.kind}=${this.name}`, error);
            await this.retryOrDeadLetter(topology, channel, message, error);
        } finally {
            this.inFlight--;
            if (this.inFlight === 0) {
                for (const resolve of this.drainWaiters.splice(0)) resolve();
            }
        }
    }

    private async retryOrDeadLetter(topology: SubscriptionTopology, channel: ConfirmChannel, message: Message, error?: unknown): Promise<void> {
        const isPermanent = isPermanentError(error);
        const maxRetries = this.options.retry?.maxRetries ?? 3;
        const headers = message.properties.headers ?? {};
        const retryCount = Number(headers[RETRY_HEADER] ?? 0);

        let targetQueue: string;
        let nextRetryCount = retryCount;
        let dlqReason: string | undefined;

        if (isPermanent) {
            targetQueue = topology.deadLetterQueue;
            dlqReason = 'permanent-failure';
            console.warn(`[MQ] Permanent error on ${this.kind}=${this.name}, forwarding directly to DLQ ${targetQueue}`);
        } else if (retryCount < maxRetries && topology.retryQueue) {
            targetQueue = topology.retryQueue;
            nextRetryCount = retryCount + 1;
        } else {
            targetQueue = topology.deadLetterQueue;
            dlqReason = 'max-retries-exceeded';
        }

        if (!targetQueue) {
            channel.nack(message, false, false);
            return;
        }

        try {
            await confirmedRawSendToQueue(channel, targetQueue, message.content, {
                persistent: true,
                contentType: message.properties.contentType,
                contentEncoding: message.properties.contentEncoding,
                messageId: message.properties.messageId,
                correlationId: message.properties.correlationId,
                headers: {
                    ...headers,
                    [RETRY_HEADER]: nextRetryCount,
                    ...(dlqReason ? { 'x-dlq-reason': dlqReason } : {})
                }
            });
            channel.ack(message);
        } catch (err) {
            console.error(`[MQ] Could not route failed message to ${targetQueue}`, err);
            channel.nack(message, false, true);
        }
    }

    private async waitForDrain(timeoutMs: number): Promise<void> {
        if (this.inFlight === 0) return;
        let timeout: NodeJS.Timeout | undefined;
        try {
            await Promise.race([
                new Promise<void>((resolve) => this.drainWaiters.push(resolve)),
                new Promise<void>((_, reject) => {
                    timeout = setTimeout(() => reject(new Error(`Consumer drain timed out after ${timeoutMs}ms`)), timeoutMs);
                })
            ]);
        } catch (error) {
            console.warn(`[MQ] Closing ${this.kind}=${this.name} with handlers in flight`, error);
        } finally {
            if (timeout) clearTimeout(timeout);
        }
    }
}
