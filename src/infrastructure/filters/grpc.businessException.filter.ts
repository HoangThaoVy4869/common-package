import { Catch, RpcExceptionFilter, ArgumentsHost, Optional } from '@nestjs/common';
import { throwError, Observable } from 'rxjs';
import { BusinessError } from '../../core/errors';

export type ErrorStatusMapper = (code: number) => number;
export type BusinessErrorMapper = (error: BusinessError) => number;

@Catch(BusinessError)
export class GrpcBusinessExceptionFilter implements RpcExceptionFilter<BusinessError> {
    constructor(@Optional() private readonly mapper?: ErrorStatusMapper | BusinessErrorMapper) {}

    catch(exception: BusinessError, _host: ArgumentsHost): Observable<never> {
        let mappedCode = 13; // default gRPC status.INTERNAL = 13

        if (this.mapper) {
            try {
                // Try passing exception first (for BusinessErrorMapper)
                let res: any;
                try {
                    res = (this.mapper as any)(exception);
                } catch {
                    res = undefined;
                }

                // If not a number or returned default 13, try passing exception.code (for ErrorStatusMapper)
                if (typeof res !== 'number' || res === 13) {
                    try {
                        const codeRes = (this.mapper as any)(exception.code);
                        if (typeof codeRes === 'number' && codeRes !== 13) {
                            res = codeRes;
                        }
                    } catch {
                        // ignore
                    }
                }

                if (typeof res === 'number') {
                    mappedCode = res;
                }
            } catch {
                mappedCode = 13;
            }
        }

        // Try dynamically loading Metadata from @grpc/grpc-js if installed
        let metadata: any = undefined;
        try {
            const runtimeRequire = eval('require') as NodeJS.Require;
            const grpc = runtimeRequire('@grpc/grpc-js');
            if (grpc && grpc.Metadata) {
                metadata = new grpc.Metadata();
                metadata.set('error-code', String(exception.code));
            }
        } catch {
            // grpc-js not available or optional
        }

        const errorPayload: Record<string, any> = {
            code: mappedCode,
            details: exception.message
        };

        if (metadata) {
            errorPayload.metadata = metadata;
        }

        return throwError(() => errorPayload);
    }
}
