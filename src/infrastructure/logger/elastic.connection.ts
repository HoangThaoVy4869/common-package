import type { estypes, Connection } from "@elastic/elasticsearch";
import split2 from 'split2';
import { Transform } from 'stream';

const runtimeRequire = eval("require") as NodeJS.Require;

interface PinoElasticOptions {
    node: string;
    auth?: { username: string; password: string } | { apiKey: string };
    cloud?: { id: string };
    rejectUnauthorized?: boolean;
    Connection?: new (...args: any[]) => Connection;
    index?: string | ((time: string) => string);
    type?: string;
    op_type?: estypes.OpType;
    'es-version'?: number;
    'flush-bytes'?: number;
    'flush-interval'?: number;
    'bulk-size'?: number; // deprecated
}

interface LogDocument {
    time?: string;
    '@timestamp'?: string;
    [key: string]: any;
}

export function pinoElastic(opts: PinoElasticOptions): Transform {
    const { Client } = runtimeRequire("@elastic/elasticsearch") as typeof import("@elastic/elasticsearch");
    const esVersion = Number(opts['es-version']) || 9;
    const indexOpt = opts.index || 'pino';
    const buildIndexName = typeof indexOpt === 'function' ? indexOpt : null;
    const type = esVersion >= 7 ? undefined : opts.type || 'log';
    const opType = esVersion >= 7 ? opts.op_type : undefined;

    if (opts['bulk-size']) {
        process.emitWarning('The "bulk-size" option has been deprecated, use "flush-bytes" instead');
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
                this.emit('unknown', line, error);
                return;
            }

            if (typeof value === 'boolean') {
                this.emit('unknown', line, 'Boolean value ignored');
                return;
            }
            if (value === null) {
                this.emit('unknown', line, 'Null value ignored');
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

    const bulkPromise = client.helpers.bulk<LogDocument>({
        datasource: splitter,
        flushBytes: opts['flush-bytes'] || 1000,
        flushInterval: opts['flush-interval'] || 30000,
        refreshOnCompletion: getIndexName(),
        onDocument(doc) {
            const date = doc.time || doc['@timestamp'] || new Date().toISOString();
            if (opType === 'create') {
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
            console.error('Dropped log document:', error);
            (error as any).document = doc;
            splitter.emit('insertError', error);
        }
    });

    bulkPromise.then(
        (stats) => splitter.emit('insert', stats),
        (err) => splitter.emit('error', err)
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
