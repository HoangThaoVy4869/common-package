import { Injectable, Optional } from '@nestjs/common';
import { ValidationSchema, ValidationErrorDetail } from './base.validation';

export interface GrpcValidationPipeOptions {
    defaultErrorCode?: number;
    schemas?: Record<string, ValidationSchema>;
}

export class GrpcValidationException extends Error {
    readonly error: { code: number; message: string; details?: ValidationErrorDetail[] };

    constructor(
        message: string,
        details: ValidationErrorDetail[] = [],
        code = 3 // status.INVALID_ARGUMENT
    ) {
        super(message);
        this.name = 'RpcException';
        this.error = { code, message, details };
    }

    getError() {
        return this.error;
    }
}

@Injectable()
export class GrpcValidationPipe {
    private readonly defaultErrorCode: number;
    private readonly schemas: Record<string, ValidationSchema>;

    constructor(@Optional() optionsOrSchemas?: Record<string, ValidationSchema> | GrpcValidationPipeOptions) {
        const anyOpts = optionsOrSchemas as any;
        if (anyOpts && (anyOpts.schemas !== undefined || typeof anyOpts.defaultErrorCode === 'number')) {
            this.defaultErrorCode = anyOpts.defaultErrorCode ?? 3;
            this.schemas = anyOpts.schemas ?? {};
        } else {
            this.defaultErrorCode = 3;
            this.schemas = anyOpts ?? {};
        }
    }

    async validate(
        method: string,
        data: any,
        overrideSchemas?: Record<string, ValidationSchema>
    ): Promise<void> {
        const schema = overrideSchemas?.[method] || this.schemas[method];
        if (!schema) {
            return;
        }

        const errors = this.validateObjectDetails(data, schema);
        if (errors.length > 0) {
            const message = `Validation failed: ${errors.map((e) => e.message).join(', ')}`;
            throw new GrpcValidationException(message, errors, this.defaultErrorCode);
        }
    }

    public validateObject(obj: any, schema: ValidationSchema): string[] {
        return this.validateObjectDetails(obj, schema).map((e) => e.message);
    }

    public validateObjectDetails(obj: any, schema: ValidationSchema, prefix = ''): ValidationErrorDetail[] {
        const errors: ValidationErrorDetail[] = [];

        if (!obj || typeof obj !== 'object') {
            errors.push({
                field: prefix || 'payload',
                message: 'Request payload must be an object'
            });
            return errors;
        }

        for (const [field, rules] of Object.entries(schema)) {
            const fieldPath = prefix ? `${prefix}.${field}` : field;
            let value = obj[field];

            if (rules.type === 'string' && typeof value === 'string' && rules.trim) {
                value = value.trim();
            }

            if (rules.required && (value === undefined || value === null || value === '')) {
                errors.push({
                    field: fieldPath,
                    message: rules.message || `${fieldPath} is required`
                });
                continue;
            }

            if (value === undefined || value === null) {
                continue;
            }

            if (rules.type) {
                if (!this.validateType(value, rules.type)) {
                    errors.push({
                        field: fieldPath,
                        message: rules.message || `${fieldPath} must be of type ${rules.type}`
                    });
                    continue;
                }
            }

            if (rules.type === 'string' && typeof value === 'string') {
                if (rules.minLength !== undefined && value.length < rules.minLength) {
                    errors.push({
                        field: fieldPath,
                        message: rules.message || `${fieldPath} must be at least ${rules.minLength} characters`
                    });
                }
                if (rules.maxLength !== undefined && value.length > rules.maxLength) {
                    errors.push({
                        field: fieldPath,
                        message: rules.message || `${fieldPath} must be at most ${rules.maxLength} characters`
                    });
                }
                if (rules.pattern && !rules.pattern.test(value)) {
                    errors.push({
                        field: fieldPath,
                        message: rules.message || `${fieldPath} format is invalid`
                    });
                }
                if (rules.enum && !rules.enum.includes(value)) {
                    errors.push({
                        field: fieldPath,
                        message: rules.message || `${fieldPath} must be one of: ${rules.enum.join(', ')}`
                    });
                }
            }

            if (rules.type === 'number' && typeof value === 'number') {
                if (rules.integer && !Number.isSafeInteger(value)) {
                    errors.push({
                        field: fieldPath,
                        message: rules.message || `${fieldPath} must be an integer`
                    });
                }
                if (rules.positive && value <= 0) {
                    errors.push({
                        field: fieldPath,
                        message: rules.message || `${fieldPath} must be a positive number`
                    });
                }
                if (rules.nonNegative && value < 0) {
                    errors.push({
                        field: fieldPath,
                        message: rules.message || `${fieldPath} must be >= 0`
                    });
                }
                if (rules.min !== undefined && value < rules.min) {
                    errors.push({
                        field: fieldPath,
                        message: rules.message || `${fieldPath} must be >= ${rules.min}`
                    });
                }
                if (rules.max !== undefined && value > rules.max) {
                    errors.push({
                        field: fieldPath,
                        message: rules.message || `${fieldPath} must be <= ${rules.max}`
                    });
                }
            }

            if (rules.type === 'array' && Array.isArray(value)) {
                if (rules.minItems !== undefined && value.length < rules.minItems) {
                    errors.push({
                        field: fieldPath,
                        message: rules.message || `${fieldPath} must have at least ${rules.minItems} items`
                    });
                }
                if (rules.maxItems !== undefined && value.length > rules.maxItems) {
                    errors.push({
                        field: fieldPath,
                        message: rules.message || `${fieldPath} must have at most ${rules.maxItems} items`
                    });
                }
                if (rules.uniqueBy) {
                    const uniqueProp = rules.uniqueBy;
                    const seen = new Set();
                    for (const item of value) {
                        const val = item?.[uniqueProp];
                        if (val !== undefined && seen.has(val)) {
                            errors.push({
                                field: fieldPath,
                                message: `Duplicate ${uniqueProp} in ${fieldPath}`
                            });
                            break;
                        }
                        seen.add(val);
                    }
                }
                if (rules.itemSchema) {
                    value.forEach((item, index) => {
                        const itemErrors = this.validateObjectDetails(item, rules.itemSchema!, `${fieldPath}[${index}]`);
                        errors.push(...itemErrors);
                    });
                }
            }

            if (rules.fields && typeof value === 'object' && !Array.isArray(value)) {
                const nestedErrors = this.validateObjectDetails(value, rules.fields, fieldPath);
                errors.push(...nestedErrors);
            }

            if (rules.custom) {
                const customResult = rules.custom(value, obj);
                if (typeof customResult === 'string') {
                    errors.push({ field: fieldPath, message: customResult });
                } else if (customResult === false) {
                    errors.push({ field: fieldPath, message: rules.message || `${fieldPath} is invalid` });
                }
            }
        }

        return errors;
    }

    private validateType(value: any, expectedType: string): boolean {
        switch (expectedType) {
            case 'string':
                return typeof value === 'string';
            case 'number':
                return typeof value === 'number' && !isNaN(value);
            case 'boolean':
                return typeof value === 'boolean';
            case 'array':
                return Array.isArray(value);
            case 'object':
                return typeof value === 'object' && value !== null && !Array.isArray(value);
            default:
                return true;
        }
    }
}
