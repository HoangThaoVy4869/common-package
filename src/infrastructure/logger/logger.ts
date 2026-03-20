import { ILogger } from "../../core/interfaces";

export class PinoLogger implements ILogger {
    constructor(private readonly logger: any) {}

    info(message: string, meta?: Record<string, any>) {
        this.logger.info(message, meta);
    }
    warn(message: string, meta?: Record<string, any>) {
        this.logger.warn(message, meta);
    }
    error(message: string, meta?: Record<string, any>) {
        this.logger.error(message, meta);
    }
    debug(message: string, meta?: Record<string, any>) {
        this.logger.debug(message, meta);
    }
}
