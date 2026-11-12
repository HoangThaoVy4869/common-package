import { z, type ZodType, ZodError } from 'zod';

export { z, type ZodType, ZodError };
export type ZodSchemaMap = Record<string, ZodType>;

export interface ValidationErrorDetail {
    field: string;
    message: string;
    code?: string;
    value?: unknown;
}

export class GrpcValidationError extends Error {
    public code: number;
    public errors?: ValidationErrorDetail[];
    readonly error: { code: number; message: string; details: ValidationErrorDetail[] };

    constructor(
        message: string,
        detailsOrCode?: ValidationErrorDetail[] | number,
        codeOrDetails?: number | ValidationErrorDetail[]
    ) {
        super(message);
        this.name = 'RpcException';

        let code = 3;
        let details: ValidationErrorDetail[] = [];

        if (typeof detailsOrCode === 'number') {
            code = detailsOrCode;
            if (Array.isArray(codeOrDetails)) {
                details = codeOrDetails;
            }
        } else if (Array.isArray(detailsOrCode)) {
            details = detailsOrCode;
            if (typeof codeOrDetails === 'number') {
                code = codeOrDetails;
            }
        }

        this.code = code;
        this.errors = details;
        this.error = { code, message, details };
    }

    getError() {
        return this.error;
    }
}

/** Alias / subclass for GrpcValidationError to maintain compatibility across validation modules */
export class GrpcValidationException extends GrpcValidationError {}

export type ZodIssueLike = {
    readonly path?: readonly (string | number)[];
    readonly message: string;
    readonly code?: string;
};

/**
 * Formats Zod issues into structured ValidationErrorDetail array without any casting.
 */
export function formatZodIssues(
    errorOrIssues: ZodError | readonly ZodIssueLike[]
): ValidationErrorDetail[] {
    const issues = Array.isArray(errorOrIssues)
        ? errorOrIssues
        : (errorOrIssues as ZodError).issues ?? [];

    return issues.map((issue) => {
        const field = Array.isArray(issue.path) ? issue.path.join('.') : String(issue.path ?? '');
        return {
            field: field || 'payload',
            message: issue.message,
            code: issue.code
        };
    });
}

/**
 * Validates data against a Zod schema asynchronously.
 * Returns the parsed and typed data.
 * Throws GrpcValidationError if validation fails.
 */
export async function validateZod<T>(schema: ZodType<T>, input: unknown): Promise<T> {
    const result = await schema.safeParseAsync(input);
    if (!result.success) {
        const details = formatZodIssues(result.error);
        const message = `Validation failed: ${details.map((d) => d.message).join(', ')}`;
        throw new GrpcValidationError(message, 3, details);
    }
    return result.data;
}

/**
 * Validates data against a Zod schema synchronously.
 * Returns the parsed and typed data.
 * Throws GrpcValidationError if validation fails.
 */
export function validateZodSync<T>(schema: ZodType<T>, input: unknown): T {
    const result = schema.safeParse(input);
    if (!result.success) {
        const details = formatZodIssues(result.error);
        const message = `Validation failed: ${details.map((d) => d.message).join(', ')}`;
        throw new GrpcValidationError(message, 3, details);
    }
    return result.data;
}
