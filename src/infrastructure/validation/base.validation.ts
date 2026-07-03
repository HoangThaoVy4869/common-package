export class GrpcValidationError extends Error {
    public code: number;

    constructor(message: string, code = 400) {
        super(message);
        this.name = 'GrpcValidationError';
        this.code = code;
    }
}

export interface FieldRule {
    required?: boolean;
    type?: 'string' | 'number' | 'boolean' | 'array' | 'object';
    minLength?: number;
    maxLength?: number;
    min?: number;
    max?: number;
    pattern?: RegExp;
    enum?: string[];
    message?: string;
    fields?: ValidationSchema;
}

export interface ValidationSchema {
    [field: string]: FieldRule;
}

export function validate<T>(schema: ValidationSchema, input: T): void {
    if (!input || typeof input !== 'object') {
        throw new GrpcValidationError('Request body must be an object');
    }

    for (const [field, rules] of Object.entries(schema)) {
        const value = (input as any)[field];

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
            if (rules.min !== undefined && value < rules.min) {
                throw new GrpcValidationError(rules.message || `${field} must be >= ${rules.min}`);
            }
            if (rules.max !== undefined && value > rules.max) {
                throw new GrpcValidationError(rules.message || `${field} must be <= ${rules.max}`);
            }
        }

        if (rules.fields && typeof value === 'object' && !Array.isArray(value)) {
            validate(rules.fields, value);
        }
    }
}
