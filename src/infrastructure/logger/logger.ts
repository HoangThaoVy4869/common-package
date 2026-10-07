import { ILogger } from "../../core/interfaces";

export class PinoLogger implements ILogger {
    constructor(private readonly logger: any) {}

    info(message: string, meta?: Record<string, any>): void {
        if (meta) {
            this.logger.info(meta, message);
        } else {
            this.logger.info(message);
        }
    }

    warn(message: string, meta?: Record<string, any>): void {
        if (meta) {
            this.logger.warn(meta, message);
        } else {
            this.logger.warn(message);
        }
    }

    error(message: string | Error, meta?: Record<string, any>): void {
        if (message instanceof Error) {
            this.logger.error({ err: message, ...meta }, message.message);
        } else if (meta) {
            this.logger.error(meta, message);
        } else {
            this.logger.error(message);
        }
    }

    debug(message: string, meta?: Record<string, any>): void {
        if (meta) {
            this.logger.debug(meta, message);
        } else {
            this.logger.debug(message);
        }
    }
}

