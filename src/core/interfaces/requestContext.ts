export interface RequestContext {
    userId?: string;
    merchantId?: string;
    role?: string;
    language?: string;
    requestId?: string;
    currencyCode?: string;
    regionCode?: string;
}
