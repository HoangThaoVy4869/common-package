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
