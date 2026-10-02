import { MessageInbox } from './rabbitmq.types';

export class InMemoryMessageInbox implements MessageInbox {
    private readonly entries = new Map<string, 'processing' | 'completed'>();
    private readonly completedIds: string[] = [];

    constructor(private readonly maxCompletedEntries = 10_000) {}

    async claim(messageId: string): Promise<boolean> {
        if (this.entries.has(messageId)) return false;
        this.entries.set(messageId, 'processing');
        return true;
    }

    async complete(messageId: string): Promise<void> {
        this.entries.set(messageId, 'completed');
        this.completedIds.push(messageId);
        while (this.completedIds.length > this.maxCompletedEntries) {
            const oldest = this.completedIds.shift();
            if (!oldest) break;
            if (this.entries.get(oldest) === 'completed') this.entries.delete(oldest);
        }
    }

    async release(messageId: string): Promise<void> {
        if (this.entries.get(messageId) === 'processing') this.entries.delete(messageId);
    }
}
