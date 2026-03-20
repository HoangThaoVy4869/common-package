import { IEventBus } from "../../core/interfaces";
import { connect, ChannelModel, ConfirmChannel, Channel, Message } from 'amqplib';

type Handler = (payload: any, rawMsg?: Message) => Promise<void>;
export class RabbitMQEventBus implements IEventBus {
    private url: string;
    private conn: ChannelModel | null = null;
    private pubChannel: ConfirmChannel | null = null;
    private subChannel: Channel | null = null;


    private reconnectAttempts = 0;
    private reconnectTimer?: NodeJS.Timeout;
    private isClosing = false;

    constructor(url?: string) {
        this.url = url || process.env.RABBITMQ_URL || "amqp://localhost";
        process.on("SIGINT", () => this.close().then(() => process.exit(0)));
        process.on("SIGTERM", () => this.close().then(() => process.exit(0)));
    }

    private sleep(ms: number) {
        return new Promise((res) => setTimeout(res, ms));
    }

    async connect() {
        if (this.conn) return;
        await this.doConnect();
    }

    private async doConnect() {
        try {
            this.conn = await connect(this.url);
            this.reconnectAttempts = 0;
            console.info("[MQ] Connected");

            this.conn.on("error", (err) => {
                console.error("[MQ] Connection error", err);
            });

            this.conn.on("close", () => {
                console.warn("[MQ] Connection closed. Scheduling reconnect...");
                if (!this.isClosing) this.scheduleReconnect();
            });

            // Create separate channels: one confirm channel for publishing, one regular for consuming
            this.pubChannel = await this.conn.createConfirmChannel();
            this.pubChannel.on("error", (err) => console.error("[MQ] pubChannel error", err));
            this.pubChannel.on("close", () => {
                console.warn("[MQ] pubChannel closed");
                if (!this.isClosing) this.scheduleReconnect();
            });

            this.subChannel = await this.conn.createChannel();
            this.subChannel.on("error", (err) => console.error("[MQ] subChannel error", err));
            this.subChannel.on("close", () => {
                console.warn("[MQ] subChannel closed");
                if (!this.isClosing) this.scheduleReconnect();
            });
        } catch (err) {
            console.error("[MQ] Connect failed:", err);
            this.scheduleReconnect();
        }
    }

    private scheduleReconnect() {
        if (this.reconnectTimer || this.isClosing) return;
        this.reconnectAttempts++;
        const delay = Math.min(1000 * 2 ** (this.reconnectAttempts - 1), 30000);
        console.info(`[MQ] Reconnect attempt ${this.reconnectAttempts} in ${delay}ms`);
        this.reconnectTimer = setTimeout(async () => {
            this.reconnectTimer = undefined;
            await this.doConnect();
        }, delay);
    }

    async publish(exchange: string, payload: any, options: { persistent?: boolean } = {}) {
        if (!this.pubChannel) throw new Error("Not connected to RabbitMQ (pubChannel missing)");
        await this.pubChannel.assertExchange(exchange, "fanout", { durable: true });
        const buffer = Buffer.from(JSON.stringify(payload));
        return new Promise<void>((resolve, reject) => {
            this.pubChannel!.publish(
                exchange,
                "", // routing key for fanout
                buffer,
                { persistent: options.persistent ?? true },
                (err) => {
                    if (err) {
                        console.error(`[MQ] publish error exchange=${exchange}`, err);
                        return reject(err);
                    }
                    resolve();
                }
            );
        });
    }

    async subscribe(eventName: string, handler: Handler, opts?: { queueName?: string; prefetch?: number }) {
        if (!this.subChannel) throw new Error("Not connected to RabbitMQ (subChannel missing)");

        await this.subChannel.assertExchange(eventName, "fanout", { durable: true });

        const queueOptions = opts?.queueName ? { durable: true } : { exclusive: true, autoDelete: true };

        const q = await this.subChannel.assertQueue(opts?.queueName || "", queueOptions);

        await this.subChannel.bindQueue(q.queue, eventName, "");

        const prefetch = opts?.prefetch ?? 1;
        this.subChannel.prefetch(prefetch);

        this.subChannel.consume(
            q.queue,
            async (msg) => {
                if (!msg) return;
                try {
                    let payload;
                    try {
                        payload = JSON.parse(msg.content.toString());
                    } catch (err) {
                        console.error("[MQ] JSON parse error:", err);
                        this.subChannel!.ack(msg);
                        return;
                    }

                    await handler(payload, msg);
                    this.subChannel!.ack(msg);
                } catch (err) {
                    console.error(`[MQ] handler failed for exchange=${eventName}`, err);
                    const redelivered = msg.fields.redelivered;
                    if (!redelivered) {
                        this.subChannel!.nack(msg, false, true);
                    } else {
                        this.subChannel!.nack(msg, false, false);
                    }
                }
            },
            { noAck: false }
        );
    }

    async close() {
        this.isClosing = true;
        if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
        try {
            await this.pubChannel?.close();
            await this.subChannel?.close();
            await this.conn?.close();
            this.conn = null;
            console.info("[MQ] Closed connection and channels");
        } catch (err) {
            console.warn("[MQ] Error during close", err);
        }
    }
}
