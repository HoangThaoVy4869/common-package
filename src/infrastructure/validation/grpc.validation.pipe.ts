import { Injectable, Optional } from '@nestjs/common';
import type { ZodType } from 'zod';
import {
    ValidationErrorDetail,
    formatZodIssues,
    type ZodSchemaMap,
    GrpcValidationError,
    GrpcValidationException
} from './base.validation';

export { GrpcValidationError, GrpcValidationException };

export interface GrpcValidationPipeOptions {
    defaultErrorCode?: number;
    schemas?: ZodSchemaMap;
}

@Injectable()
export class GrpcValidationPipe {
    protected readonly defaultErrorCode: number;
    protected readonly schemas: ZodSchemaMap;

    constructor(@Optional() optionsOrSchemas?: ZodSchemaMap | GrpcValidationPipeOptions) {
        if (optionsOrSchemas && ('schemas' in optionsOrSchemas || 'defaultErrorCode' in optionsOrSchemas)) {
            const opts = optionsOrSchemas as GrpcValidationPipeOptions;
            this.defaultErrorCode = opts.defaultErrorCode ?? 3;
            this.schemas = opts.schemas ?? {};
        } else {
            this.defaultErrorCode = 3;
            this.schemas = (optionsOrSchemas as ZodSchemaMap) ?? {};
        }
    }

    /**
     * Validates data against a Zod schema or registered method name.
     * Returns the validated and parsed data on success.
     * Throws GrpcValidationException on failure.
     */
    async validate<T>(schema: ZodType<T>, data: unknown): Promise<T>;
    async validate<T = unknown>(method: string, data: unknown, overrideSchemas?: ZodSchemaMap): Promise<T>;
    async validate<T = unknown>(
        schemaOrMethod: string | ZodType<T>,
        data: unknown,
        overrideSchemas?: ZodSchemaMap
    ): Promise<T> {
        let schema: ZodType<T> | undefined;

        if (typeof schemaOrMethod === 'string') {
            schema = (overrideSchemas?.[schemaOrMethod] || this.schemas[schemaOrMethod]) as ZodType<T> | undefined;
            if (!schema) {
                return data as T;
            }
        } else {
            schema = schemaOrMethod;
        }

        const result = await schema.safeParseAsync(data);
        if (!result.success) {
            const details = formatZodIssues(result.error);
            const message = `Validation failed: ${details.map((e) => e.message).join(', ')}`;
            throw new GrpcValidationException(message, details, this.defaultErrorCode);
        }

        return result.data;
    }

    /**
     * Synchronous validation against a Zod schema or registered method name.
     */
    validateSync<T>(schema: ZodType<T>, data: unknown): T;
    validateSync<T = unknown>(method: string, data: unknown, overrideSchemas?: ZodSchemaMap): T;
    validateSync<T = unknown>(
        schemaOrMethod: string | ZodType<T>,
        data: unknown,
        overrideSchemas?: ZodSchemaMap
    ): T {
        let schema: ZodType<T> | undefined;

        if (typeof schemaOrMethod === 'string') {
            schema = (overrideSchemas?.[schemaOrMethod] || this.schemas[schemaOrMethod]) as ZodType<T> | undefined;
            if (!schema) {
                return data as T;
            }
        } else {
            schema = schemaOrMethod;
        }

        const result = schema.safeParse(data);
        if (!result.success) {
            const details = formatZodIssues(result.error);
            const message = `Validation failed: ${details.map((e) => e.message).join(', ')}`;
            throw new GrpcValidationException(message, details, this.defaultErrorCode);
        }

        return result.data;
    }
}
