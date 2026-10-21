export interface ILogger {
    info(message: string, meta?: Record<string, any>): void;
    warn(message: string, meta?: Record<string, any>): void;
    error(message: string | Error, meta?: Record<string, any>): void;
    debug(message: string, meta?: Record<string, any>): void;
    child?(bindings: Record<string, any>): ILogger;
}
