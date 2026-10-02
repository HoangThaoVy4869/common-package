import { Catch, RpcExceptionFilter, ArgumentsHost } from '@nestjs/common';
import { throwError, Observable } from 'rxjs';
import { BusinessError } from '../../core/errors';

export type ErrorStatusMapper = (code: number) => number;
@Catch(BusinessError)
export class GrpcBusinessExceptionFilter implements RpcExceptionFilter<BusinessError> {
    constructor(private readonly mapper: ErrorStatusMapper) {}

    catch(exception: BusinessError, host: ArgumentsHost): Observable<any> {
        const mappedCode = this.mapper(exception.code);

        return throwError(() => ({
            code: mappedCode,
            details: exception.message
        }));
    }
}
