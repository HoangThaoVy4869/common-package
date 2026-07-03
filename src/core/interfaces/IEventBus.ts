export const EVENT_BUS = 'EVENT_BUS';

export interface IEventBus {
    publish(eventName: string, payload: any): Promise<void>;
    emit(eventName: string, payload: any): Promise<void>;
    subscribe(eventName: string, handler: (payload: any) => Promise<void>): void;
}
