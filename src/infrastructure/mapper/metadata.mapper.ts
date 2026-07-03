import { Metadata } from '@grpc/grpc-js';
import { RequestContext } from '../../core/interfaces';

export class MetadataMapper {
    static toRequestContext(metadata: Metadata): RequestContext {
        return {
            userId: metadata.get('x-user-id')?.[0]?.toString(),
            role: metadata.get('x-role')?.[0]?.toString(),
            requestId: metadata.get('x-request-id')?.[0]?.toString(),
            language: metadata.get('accept-language')?.[0]?.toString() ?? 'en'
        };
    }
}
