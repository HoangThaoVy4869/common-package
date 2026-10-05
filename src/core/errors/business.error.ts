export class BusinessError extends Error {
    public readonly code: number;
    public readonly params?: Record<string, unknown>;

    constructor(code: number, message: string, params?: Record<string, unknown>) {
        super(message);
        this.name = 'BusinessError';
        this.code = code;
        this.params = params;
    }
}

export class NotFoundError extends BusinessError {
    constructor(message = 'Resource not found', code = 4040, params?: Record<string, unknown>) {
        super(code, message, params);
        this.name = 'NotFoundError';
    }
}

export class DuplicateError extends BusinessError {
    constructor(message = 'Resource already exists', code = 4090, params?: Record<string, unknown>) {
        super(code, message, params);
        this.name = 'DuplicateError';
    }
}

export class ConflictError extends BusinessError {
    constructor(message = 'Resource conflict', code = 4090, params?: Record<string, unknown>) {
        super(code, message, params);
        this.name = 'ConflictError';
    }
}

export class UnauthorizedError extends BusinessError {
    constructor(message = 'Unauthorized', code = 4010, params?: Record<string, unknown>) {
        super(code, message, params);
        this.name = 'UnauthorizedError';
    }
}

export class ForbiddenError extends BusinessError {
    constructor(message = 'Forbidden', code = 4030, params?: Record<string, unknown>) {
        super(code, message, params);
        this.name = 'ForbiddenError';
    }
}
