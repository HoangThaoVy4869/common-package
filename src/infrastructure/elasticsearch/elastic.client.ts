import { Client as ElasticClient, ClientOptions } from "@elastic/elasticsearch";

export interface ElasticConfigOptions extends ClientOptions {
    node?: string;
    url?: string;
}

let sharedClient: ElasticClient | null = null;

/**
 * Creates a dedicated Elasticsearch Client instance.
 */
export function createElasticClient(options?: ElasticConfigOptions): ElasticClient {
    const node = options?.node || options?.url || process.env.ELASTICSEARCH_NODE || process.env.ELASTICSEARCH_URL || 'http://localhost:9200';
    return new ElasticClient({
        node,
        ...options
    });
}

/**
 * Returns a process-level shared Elasticsearch Client instance.
 */
export function getSharedElasticClient(options?: ElasticConfigOptions): ElasticClient {
    if (!sharedClient) {
        sharedClient = createElasticClient(options);
    }
    return sharedClient;
}
