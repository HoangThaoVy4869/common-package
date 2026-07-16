import { RpcException } from '@nestjs/microservices';
import { Catch, RpcExceptionFilter, ArgumentsHost } from '@nestjs/common';
import { throwError, Observable } from 'rxjs';
import { BusinessError } from '../../core/errors';

export type ErrorStatusMapper = (code: number) => number;

@Catch(BusinessError)
export class GrpcBusinessExceptionFilter implements RpcExceptionFilter<BusinessError> {
    constructor(private readonly mapper: ErrorStatusMapper) {}

    catch(exception: BusinessError, host: ArgumentsHost): Observable<any> {
        return throwError(
            () =>
                new RpcException({
                    code: this.mapper(exception.code),
                    message: exception.message
                })
        );
    }
}
