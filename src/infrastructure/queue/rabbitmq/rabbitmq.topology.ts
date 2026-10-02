import { ConfirmChannel } from 'amqplib';
import { MessageKind, SubscriptionOptions } from './rabbitmq.types';

export interface SubscriptionTopology {
    queue: string;
    retryQueue: string;
    deadLetterQueue: string;
}

interface TopologySubscription {
    kind: MessageKind;
    name: string;
    options: SubscriptionOptions;
    alternateExchange?: string;
}

export async function declareSubscriptionTopology(channel: ConfirmChannel, subscription: TopologySubscription): Promise<SubscriptionTopology> {
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
            console.warn(`[MQ] Retry/DLQ disabled for anonymous subscription ${subscription.name}; set queueName to enable it`);
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
