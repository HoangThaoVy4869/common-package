export const EVENT_BUS = 'EVENT_BUS';
export interface IMessageBus {
    /** Publish a fan-out event. */
    publishEvent(eventName: string, payload: unknown): Promise<void>;
    /** Send a point-to-point command. */
    sendCommand(queueName: string, payload: unknown): Promise<void>;
    /** Subscribe to a fan-out event. */
    subscribeEvent(eventName: string, handler: (payload: unknown) => Promise<void>): Promise<void>;
    /** Handle a point-to-point command. */
    handleCommand(queueName: string, handler: (payload: unknown) => Promise<void>): Promise<void>;
}
