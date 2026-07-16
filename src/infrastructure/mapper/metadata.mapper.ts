import { Metadata } from '@grpc/grpc-js';
import { RequestContext } from '../../core/interfaces';

export class MetadataMapper {
    static toRequestContext(metadata: Metadata): RequestContext {
        return {
            userId: metadata.get('x-user-id')?.[0]?.toString(),
            merchantId: metadata.get('x-merchant-id')?.[0]?.toString(),
            role: metadata.get('x-role')?.[0]?.toString(),
            requestId: metadata.get('x-request-id')?.[0]?.toString(),
            language: metadata.get('x-accept-language')?.[0]?.toString() ?? 'vi',
            currencyCode: metadata.get('x-currency-code')?.[0]?.toString() ?? 'VND',
            regionCode: metadata.get('x-region-code')?.[0]?.toString() ?? 'VN'
        };
    }
    static fromRequestContext(context: RequestContext): Metadata {
        const metadata = new Metadata();

        if (context.userId) {
            metadata.set('x-user-id', context.userId);
        }
        if (context.merchantId) {
            metadata.set('x-merchant-id', context.merchantId);
        }
        if (context.role) {
            metadata.set('x-role', context.role);
        }
        if (context.requestId) {
            metadata.set('x-request-id', context.requestId);
        }
        if (context.language) {
            metadata.set('x-accept-language', context.language);
        }
        if (context.regionCode) {
            metadata.set('x-region-code', context.regionCode);
        }

        return metadata;
    }
}
