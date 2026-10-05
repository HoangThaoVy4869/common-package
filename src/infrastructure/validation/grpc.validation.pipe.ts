import { Injectable, Optional } from '@nestjs/common';
import { BusinessError } from '../../core/errors';
import { ValidationSchema } from './base.validation';

export interface GrpcValidationPipeOptions {
    defaultErrorCode?: number;
    schemas?: Record<string, ValidationSchema>;
}

@Injectable()
export class GrpcValidationPipe {
    private readonly defaultErrorCode: number;
    private readonly schemas: Record<string, ValidationSchema>;

    constructor(@Optional() optionsOrSchemas?: Record<string, ValidationSchema> | GrpcValidationPipeOptions) {
        const anyOpts = optionsOrSchemas as any;
        if (anyOpts && (anyOpts.schemas !== undefined || typeof anyOpts.defaultErrorCode === 'number')) {
            this.defaultErrorCode = anyOpts.defaultErrorCode ?? 4000;
            this.schemas = anyOpts.schemas ?? {};
        } else {
            this.defaultErrorCode = 4000;
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

        const errors = this.validateObject(data, schema);
        if (errors.length > 0) {
            throw new BusinessError(this.defaultErrorCode, `Validation failed: ${errors.join(', ')}`, { errors });
        }
    }

    public validateObject(obj: any, schema: ValidationSchema): string[] {
        const errors: string[] = [];

        if (!obj || typeof obj !== 'object') {
            errors.push('Request payload must be an object');
            return errors;
        }

        for (const [field, rules] of Object.entries(schema)) {
            const value = obj[field];

            if (rules.required && (value === undefined || value === null || value === '')) {
                errors.push(rules.message || `${field} is required`);
                continue;
            }

            if (value === undefined || value === null) {
                continue;
            }

            if (rules.type) {
                if (!this.validateType(value, rules.type)) {
                    errors.push(rules.message || `${field} must be of type ${rules.type}`);
                    continue;
                }
            }

            if (rules.type === 'string' && typeof value === 'string') {
                if (rules.minLength !== undefined && value.length < rules.minLength) {
                    errors.push(rules.message || `${field} must be at least ${rules.minLength} characters`);
                }
                if (rules.maxLength !== undefined && value.length > rules.maxLength) {
                    errors.push(rules.message || `${field} must be at most ${rules.maxLength} characters`);
                }
                if (rules.pattern && !rules.pattern.test(value)) {
                    errors.push(rules.message || `${field} format is invalid`);
                }
            }

            if (rules.enum && !rules.enum.includes(value)) {
                errors.push(rules.message || `${field} must be one of: ${rules.enum.join(', ')}`);
            }

            if (rules.type === 'number' && typeof value === 'number') {
                if (rules.min !== undefined && value < rules.min) {
                    errors.push(rules.message || `${field} must be >= ${rules.min}`);
                }
                if (rules.max !== undefined && value > rules.max) {
                    errors.push(rules.message || `${field} must be <= ${rules.max}`);
                }
            }

            if (rules.type === 'array' && Array.isArray(value)) {
                const anyRules = rules as any;
                if (anyRules.minItems !== undefined && value.length < anyRules.minItems) {
                    errors.push(rules.message || `${field} must have at least ${anyRules.minItems} items`);
                }
                if (anyRules.maxItems !== undefined && value.length > anyRules.maxItems) {
                    errors.push(rules.message || `${field} must have at most ${anyRules.maxItems} items`);
                }
            }

            if (rules.fields && typeof value === 'object' && !Array.isArray(value)) {
                const nestedErrors = this.validateObject(value, rules.fields);
                errors.push(...nestedErrors.map((err) => `${field}.${err}`));
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
