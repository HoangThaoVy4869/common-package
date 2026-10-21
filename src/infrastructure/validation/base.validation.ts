export class GrpcValidationError extends Error {
    public code: number;
    public errors?: ValidationErrorDetail[];

    constructor(message: string, code = 400, errors?: ValidationErrorDetail[]) {
        super(message);
        this.name = 'GrpcValidationError';
        this.code = code;
        this.errors = errors;
    }
}

export interface ValidationErrorDetail {
    field: string;
    message: string;
    value?: unknown;
}

export interface FieldRule {
    required?: boolean;
    type?: 'string' | 'number' | 'boolean' | 'array' | 'object';
    trim?: boolean;
    minLength?: number;
    maxLength?: number;
    min?: number;
    max?: number;
    integer?: boolean;
    positive?: boolean;
    nonNegative?: boolean;
    pattern?: RegExp;
    enum?: (string | number)[];
    message?: string;
    fields?: ValidationSchema;
    minItems?: number;
    maxItems?: number;
    itemSchema?: ValidationSchema;
    uniqueBy?: string;
    custom?: (value: any, rootObj?: any) => string | boolean | undefined;
}

export interface ValidationSchema {
    [field: string]: FieldRule;
}

export type TypedSchema<T> = {
    [K in keyof T]?: FieldRule;
};

export function validate<T>(schema: ValidationSchema, input: T): void {
    if (!input || typeof input !== 'object') {
        throw new GrpcValidationError('Request body must be an object');
    }

    for (const [field, rules] of Object.entries(schema)) {
        let value = (input as any)[field];

        if (rules.type === 'string' && typeof value === 'string' && rules.trim) {
            value = value.trim();
        }

        if (rules.required && (value === undefined || value === null || value === '')) {
            throw new GrpcValidationError(rules.message || `${field} is required`);
        }

        if (value === undefined || value === null) continue;

        if (rules.type) {
            const actualType = Array.isArray(value) ? 'array' : typeof value;
            if (actualType !== rules.type) {
                throw new GrpcValidationError(rules.message || `${field} must be of type ${rules.type}`);
            }
        }

        if (typeof value === 'string') {
            if (rules.minLength !== undefined && value.length < rules.minLength) {
                throw new GrpcValidationError(rules.message || `${field} must be at least ${rules.minLength} characters`);
            }
            if (rules.maxLength !== undefined && value.length > rules.maxLength) {
                throw new GrpcValidationError(rules.message || `${field} must be at most ${rules.maxLength} characters`);
            }
            if (rules.pattern && !rules.pattern.test(value)) {
                throw new GrpcValidationError(rules.message || `${field} format is invalid`);
            }
            if (rules.enum && !rules.enum.includes(value)) {
                throw new GrpcValidationError(rules.message || `${field} must be one of: ${rules.enum.join(', ')}`);
            }
        }

        if (typeof value === 'number') {
            if (rules.integer && !Number.isSafeInteger(value)) {
                throw new GrpcValidationError(rules.message || `${field} must be an integer`);
            }
            if (rules.positive && value <= 0) {
                throw new GrpcValidationError(rules.message || `${field} must be a positive number`);
            }
            if (rules.nonNegative && value < 0) {
                throw new GrpcValidationError(rules.message || `${field} must be >= 0`);
            }
            if (rules.min !== undefined && value < rules.min) {
                throw new GrpcValidationError(rules.message || `${field} must be >= ${rules.min}`);
            }
            if (rules.max !== undefined && value > rules.max) {
                throw new GrpcValidationError(rules.message || `${field} must be <= ${rules.max}`);
            }
        }

        if (Array.isArray(value)) {
            if (rules.minItems !== undefined && value.length < rules.minItems) {
                throw new GrpcValidationError(rules.message || `${field} must have at least ${rules.minItems} items`);
            }
            if (rules.maxItems !== undefined && value.length > rules.maxItems) {
                throw new GrpcValidationError(rules.message || `${field} must have at most ${rules.maxItems} items`);
            }
            if (rules.uniqueBy) {
                const seen = new Set();
                for (const item of value) {
                    const val = item?.[rules.uniqueBy];
                    if (val !== undefined && seen.has(val)) {
                        throw new GrpcValidationError(`Duplicate ${rules.uniqueBy} in ${field}`);
                    }
                    seen.add(val);
                }
            }
            if (rules.itemSchema) {
                for (const item of value) {
                    validate(rules.itemSchema, item);
                }
            }
        }

        if (rules.fields && typeof value === 'object' && !Array.isArray(value)) {
            validate(rules.fields, value);
        }

        if (rules.custom) {
            const customResult = rules.custom(value, input);
            if (typeof customResult === 'string') {
                throw new GrpcValidationError(customResult);
            } else if (customResult === false) {
                throw new GrpcValidationError(rules.message || `${field} is invalid`);
            }
        }
    }
}

