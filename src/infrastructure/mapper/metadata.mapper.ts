import { Metadata } from '@grpc/grpc-js';
import { REQUEST_METADATA, RequestContext } from '../../core/interfaces';

export class MetadataMapper {
    static toRequestContext(metadata: Metadata): RequestContext {
        return {
            requestId: metadata.get(REQUEST_METADATA.requestId)?.[0]?.toString(),
            userId: metadata.get(REQUEST_METADATA.userId)?.[0]?.toString(),
            merchantId: metadata.get(REQUEST_METADATA.merchantId)?.[0]?.toString(),
            roles: metadata.get(REQUEST_METADATA.roles)?.[0]?.toString().split(','),
            scopes: metadata.get(REQUEST_METADATA.scopes)?.[0]?.toString().split(','),
            sessionId: metadata.get(REQUEST_METADATA.sessionId)?.[0]?.toString(),
            clientIp: metadata.get(REQUEST_METADATA.clientIp)?.[0]?.toString() ?? metadata.get(REQUEST_METADATA.clientIp)?.[0]?.toString(),
            language: metadata.get(REQUEST_METADATA.language)?.[0]?.toString() ?? 'vi',
            currencyCode: metadata.get(REQUEST_METADATA.currencyCode)?.[0]?.toString() ?? 'VND',
            regionCode: metadata.get(REQUEST_METADATA.regionCode)?.[0]?.toString() ?? 'VN',
            idempotencyKey: metadata.get(REQUEST_METADATA.idempotencyKey)?.[0]?.toString() ?? metadata.get(REQUEST_METADATA.idempotencyKey)?.[0]?.toString(),
        };
    }
    static fromRequestContext(context: RequestContext): Metadata {
        const metadata = new Metadata();

        if (context.requestId) {
            metadata.set(REQUEST_METADATA.requestId, context.requestId);
        }
        if (context.userId) {
            metadata.set(REQUEST_METADATA.userId, context.userId);
        }
        if (context.merchantId) {
            metadata.set(REQUEST_METADATA.merchantId, context.merchantId);
        }
        if (context.roles) {
            metadata.set(REQUEST_METADATA.roles, context.roles.join(','));
        }
        if (context.scopes) {
            metadata.set(REQUEST_METADATA.scopes, context.scopes.join(','));
        }
        if (context.sessionId) {
            metadata.set(REQUEST_METADATA.sessionId, context.sessionId);
        }
        if (context.clientIp) {
            metadata.set(REQUEST_METADATA.clientIp, context.clientIp);
        }
        if (context.language) {
            metadata.set(REQUEST_METADATA.language, context.language);
        }
        if (context.regionCode) {
            metadata.set(REQUEST_METADATA.regionCode, context.regionCode);
        }
        if (context.currencyCode) {
            metadata.set(REQUEST_METADATA.currencyCode, context.currencyCode);
        }
        if (context.idempotencyKey) {
            metadata.set(REQUEST_METADATA.idempotencyKey, context.idempotencyKey);
        }

        return metadata;
    }
}
