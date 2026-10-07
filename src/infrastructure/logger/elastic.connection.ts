import type { Client as ElasticClient, estypes, Connection } from "@elastic/elasticsearch";
import split2 from 'split2';
import { Transform } from 'stream';

const runtimeRequire = eval("require") as NodeJS.Require;

export interface PinoElasticOptions {
    node: string;
    auth?: { username: string; password: string } | { apiKey: string };
    cloud?: { id: string };
    rejectUnauthorized?: boolean;
    Connection?: new (...args: any[]) => Connection;
    index?: string | ((time: string) => string);
    type?: string;
    op_type?: estypes.OpType;
    'es-version'?: number;
    esVersion?: number;
    'flush-bytes'?: number;
    flushBytes?: number;
    'flush-interval'?: number;
    flushInterval?: number;
    'bulk-size'?: number; // deprecated
    refreshOnCompletion?: boolean | string;
}

interface LogDocument {
    time?: string;
    '@timestamp'?: string;
    [key: string]: any;
}

export function pinoElastic(opts: PinoElasticOptions): Transform {
    let Client: typeof ElasticClient;
    try {
        const esModule = runtimeRequire("@elastic/elasticsearch");
        Client = esModule.Client;
    } catch {
        throw new Error(
            "[pinoElastic] Package '@elastic/elasticsearch' is required but not installed. Please install it with: npm install @elastic/elasticsearch"
        );
    }

    const esVersion = Number(opts.esVersion ?? opts['es-version']) || 9;
    const indexOpt = opts.index || 'pino';
    const buildIndexName = typeof indexOpt === 'function' ? indexOpt : null;
    const type = esVersion >= 7 ? undefined : opts.type || 'log';
    const opType = esVersion >= 7 ? opts.op_type : undefined;

    if (opts['bulk-size']) {
        process.emitWarning('The "bulk-size" option has been deprecated, use "flush-bytes" (or "flushBytes") instead');
        delete opts['bulk-size'];
    }

    const getIndexName = (time = new Date().toISOString()): string => {
        if (buildIndexName) return buildIndexName(time);
        return (indexOpt as string).replace('%{DATE}', time.substring(0, 10));
    };

    const setDateTimeString = (value: any): string => {
        if (typeof value === 'object' && value?.time !== undefined) {
            if (
                (typeof value.time === 'string' && value.time.length > 0) ||
                (typeof value.time === 'number' && value.time >= 0)
            ) {
                return new Date(value.time).toISOString();
            }
        }
        return new Date().toISOString();
    };

    const splitter = split2(
        function (this: Transform, line: string) {
            let value: any;

            try {
                value = JSON.parse(line);
            } catch (error) {
                if (this.listenerCount('unknown') > 0) {
                    this.emit('unknown', line, error);
                }
                return;
            }

            if (typeof value === 'boolean') {
                if (this.listenerCount('unknown') > 0) {
                    this.emit('unknown', line, 'Boolean value ignored');
                }
                return;
            }
            if (value === null) {
                if (this.listenerCount('unknown') > 0) {
                    this.emit('unknown', line, 'Null value ignored');
                }
                return;
            }
            if (typeof value !== 'object') {
                value = { data: value, time: setDateTimeString(value) };
            } else if (value['@timestamp'] === undefined) {
                value.time = setDateTimeString(value);
            }

            return value;
        },
        { autoDestroy: true }
    );

    const client = new Client({
        node: opts.node,
        auth: opts.auth,
        cloud: opts.cloud,
        tls: { rejectUnauthorized: opts.rejectUnauthorized },
    });

    const flushBytes = opts.flushBytes ?? opts['flush-bytes'] ?? 1000;
    const flushInterval = opts.flushInterval ?? opts['flush-interval'] ?? 30000;
    const refreshOnCompletion = opts.refreshOnCompletion !== undefined ? opts.refreshOnCompletion : getIndexName();

    const bulkPromise = client.helpers.bulk<LogDocument>({
        datasource: splitter,
        flushBytes,
        flushInterval,
        refreshOnCompletion,
        onDocument(doc) {
            const date = doc.time || doc['@timestamp'] || new Date().toISOString();
            if (!doc['@timestamp']) {
                doc['@timestamp'] = date;
            }

            return {
                index: {
                    _index: getIndexName(date),
                    _type: type,
                    op_type: opType
                }
            };
        },
        onDrop(doc) {
            const error = new Error('Dropped document');
            (error as any).document = doc;
            if (splitter.listenerCount('insertError') > 0) {
                splitter.emit('insertError', error);
            } else {
                console.error('[pinoElastic] Dropped log document:', error);
            }
        }
    });

    bulkPromise.then(
        (stats) => {
            if (splitter.listenerCount('insert') > 0) {
                splitter.emit('insert', stats);
            }
        },
        (err) => {
            if (splitter.listenerCount('error') > 0) {
                splitter.emit('error', err);
            } else {
                console.error('[pinoElastic] Elasticsearch connection/indexing error:', err);
            }
        }
    );

    // Clean up when stream is destroyed
    (splitter as any)._destroy = function (err: Error | null, cb: (err?: Error | null) => void) {
        bulkPromise.then(
            () => cb(err),
            (e2) => cb(e2 || err)
        );
    };

    return splitter;
}
