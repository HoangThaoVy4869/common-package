export interface RequestContext {
    requestId?: string;
    userId?: string;
    merchantId?: string;
    roles?: string[];
    scopes?: string[];
    sessionId?: string;
    clientIp?: string;
    language?: string;
    currencyCode?: string;
    regionCode?: string;
    idempotencyKey?: string;
}

export const REQUEST_METADATA = {
    requestId: 'x-request-id',
    userId: 'x-user-id',
    merchantId: 'x-merchant-id',
    roles: 'x-roles',
    scopes: 'x-scopes',
    sessionId: 'x-session-id',
    clientIp: 'x-client-ip',
    language: 'x-accept-language',
    currencyCode: 'x-currency-code',
    regionCode: 'x-region-code',
    idempotencyKey: 'x-idempotency-key'
} as const;
