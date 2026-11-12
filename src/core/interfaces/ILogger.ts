export interface ILogger {
    info(message: string, meta?: Record<string, any>): void;
    warn(message: string, meta?: Record<string, any>): void;
    error(message: string | Error, meta?: Record<string, any>): void;
    debug(message: string, meta?: Record<string, any>): void;
    child?(bindings: Record<string, any>): ILogger;
}

export const defaultConsoleLogger: ILogger = {
    info: (msg, meta) => (meta ? console.info(msg, meta) : console.info(msg)),
    warn: (msg, meta) => (meta ? console.warn(msg, meta) : console.warn(msg)),
    error: (msg, meta) => (meta ? console.error(msg, meta) : console.error(msg)),
    debug: (msg, meta) => (meta ? console.debug(msg, meta) : console.debug(msg))
};
