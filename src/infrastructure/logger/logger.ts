import util from 'util';
import { ILogger } from '@core/interfaces';
import { pinoElastic, PinoElasticOptions } from './elastic.transport';

export interface LoggerOptions {
    /** Logical service name (e.g. 'order-service', 'auth-service'). */
    serviceName?: string;
    /** Minimum logging level. Defaults to 'info' ('debug' in non-production). */
    level?: 'debug' | 'info' | 'warn' | 'error';
    /** Colorized and human-readable terminal output. Defaults to true in non-production. */
    pretty?: boolean;
    /** Optional Elasticsearch shipping configuration. */
    elastic?: PinoElasticOptions;
    /** Default static metadata attached to every log record (e.g. env, version). */
    bindings?: Record<string, any>;
}

const LEVEL_COLORS: Record<string, { label: string; color: string }> = {
    debug: { label: '[DEBUG]', color: '\x1b[35m' }, // Magenta
    info: { label: '[INFO ]', color: '\x1b[32m' }, // Green
    warn: { label: '[WARN ]', color: '\x1b[33m' }, // Yellow
    error: { label: '[ERROR]', color: '\x1b[31m' }  // Red
};

const RESET = '\x1b[0m';
const DIM = '\x1b[90m';
const CYAN = '\x1b[36m';
const BOLD = '\x1b[1m';

function formatTimestamp(): string {
    const now = new Date();
    const hh = String(now.getHours()).padStart(2, '0');
    const mm = String(now.getMinutes()).padStart(2, '0');
    const ss = String(now.getSeconds()).padStart(2, '0');
    const ms = String(now.getMilliseconds()).padStart(3, '0');
    return `${hh}:${mm}:${ss}.${ms}`;
}

/**
 * Universal Logger implementation for Microservices.
 * Supports beautiful, colorized terminal logs in development
 * and structured JSON / Elasticsearch log streaming in production.
 */
export class PinoLogger implements ILogger {
    private readonly serviceName?: string;
    private readonly bindings: Record<string, any>;
    private readonly isPretty: boolean;
    private readonly underlyingLogger?: any;
    private readonly elasticStream?: any;

    constructor(
        underlyingOrOptions?: any | LoggerOptions,
        bindings: Record<string, any> = {}
    ) {
        if (underlyingOrOptions && typeof underlyingOrOptions.info === 'function') {
            this.underlyingLogger = underlyingOrOptions;
            this.isPretty = false;
            this.bindings = bindings;
        } else {
            const opts: LoggerOptions = underlyingOrOptions || {};
            this.serviceName = opts.serviceName;
            this.isPretty = opts.pretty ?? (process.env.NODE_ENV !== 'production');
            this.bindings = { ...(opts.bindings || {}), ...bindings };

            if (opts.elastic) {
                try {
                    this.elasticStream = pinoElastic(opts.elastic);
                } catch (error) {
                    console.error('[PinoLogger] Failed to initialize Elasticsearch log stream:', error);
                }
            }
        }
    }

    info(message: string, meta?: Record<string, any>): void {
        this.log('info', message, meta);
    }

    warn(message: string, meta?: Record<string, any>): void {
        this.log('warn', message, meta);
    }

    error(message: string | Error, meta?: Record<string, any>): void {
        if (message instanceof Error) {
            this.log('error', message.message, { stack: message.stack, ...(meta || {}) });
        } else {
            this.log('error', message, meta);
        }
    }

    debug(message: string, meta?: Record<string, any>): void {
        this.log('debug', message, meta);
    }

    /**
     * Creates a scoped child logger with bound metadata (e.g. correlationId, traceId, userId).
     */
    child(bindings: Record<string, any>): PinoLogger {
        if (this.underlyingLogger && typeof this.underlyingLogger.child === 'function') {
            return new PinoLogger(this.underlyingLogger.child(bindings), { ...this.bindings, ...bindings });
        }
        return new PinoLogger({
            serviceName: this.serviceName,
            pretty: this.isPretty,
            bindings: { ...this.bindings, ...bindings }
        });
    }

    private log(level: 'debug' | 'info' | 'warn' | 'error', message: string, meta?: Record<string, any>): void {
        const mergedMeta = { ...this.bindings, ...(meta || {}) };

        // 1. Forward to Elasticsearch stream if enabled
        if (this.elasticStream) {
            try {
                const esDoc = {
                    '@timestamp': new Date().toISOString(),
                    level,
                    service: this.serviceName,
                    message,
                    ...mergedMeta
                };
                this.elasticStream.write(JSON.stringify(esDoc) + '\n');
            } catch {
                // Ignore transient stream buffer issues
            }
        }

        // 2. Delegate to underlying external logger if provided
        if (this.underlyingLogger) {
            if (Object.keys(mergedMeta).length > 0) {
                this.underlyingLogger[level](mergedMeta, message);
            } else {
                this.underlyingLogger[level](message);
            }
            return;
        }

        const trace = mergedMeta.correlationId || mergedMeta.traceId || mergedMeta.id;

        // 3. Terminal output
        if (this.isPretty) {
            const timeStr = `${DIM}${formatTimestamp()}${RESET}`;
            const { label, color } = LEVEL_COLORS[level] || LEVEL_COLORS.info;
            const levelBadge = `${color}${BOLD}${label}${RESET}`;
            const serviceBadge = this.serviceName ? `${CYAN}[${this.serviceName}]${RESET} ` : '';
            const traceBadge = trace ? `${DIM}(trace:${String(trace).slice(0, 8)})${RESET} ` : '';

            // Clean meta for display
            const cleanMeta = { ...mergedMeta };
            delete cleanMeta.correlationId;
            delete cleanMeta.traceId;
            const stack = cleanMeta.stack;
            delete cleanMeta.stack;

            let metaStr = '';
            if (Object.keys(cleanMeta).length > 0) {
                metaStr = ` ${DIM}${util.inspect(cleanMeta, { depth: 3, colors: true, breakLength: 120 })}${RESET}`;
            }

            const line = `${timeStr} ${levelBadge} ${serviceBadge}${traceBadge}${message}${metaStr}`;
            if (level === 'error') {
                console.error(line);
                if (stack) console.error(`${DIM}${stack}${RESET}`);
            } else if (level === 'warn') {
                console.warn(line);
            } else if (level === 'debug') {
                console.debug(line);
            } else {
                console.log(line);
            }
        } else {
            // Production JSON output
            const payload = {
                timestamp: new Date().toISOString(),
                level,
                service: this.serviceName,
                message,
                ...mergedMeta
            };
            const json = JSON.stringify(payload);
            if (level === 'error') console.error(json);
            else if (level === 'warn') console.warn(json);
            else console.log(json);
        }
    }
}

/**
 * Creates a configured, high-performance logger.
 * - In Development: Emits clean, readable colorized terminal logs.
 * - In Production: Emits structured JSON and optional Elasticsearch shipping.
 */
export function createLogger(options: LoggerOptions = {}): PinoLogger {
    return new PinoLogger(options);
}
