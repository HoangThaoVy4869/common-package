import { randomBytes, randomUUID } from 'crypto';
import { uuidv7 } from 'uuidv7';

export function generateId(): string {
    return uuidv7();
}
export const generateProductId = generateId;
export const generateVariantId = generateId;
export const generateOrderId = generateId;
export const generatePaymentId = generateId;

export function generateApiKey(clientId: string): string {
    return `${clientId}_${randomUUID()}`;
}

export function generateSecret(): string {
    return randomBytes(32).toString('hex');
}
