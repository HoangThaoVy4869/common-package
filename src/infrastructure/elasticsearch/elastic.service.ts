import type { Client as ElasticClient } from "@elastic/elasticsearch";
import type { IElasticSearchClient } from "@core/interfaces/IElasticClient";
import { createElasticClient, ElasticConfigOptions } from "./elastic.client";

/**
 * High-level Elasticsearch service for document indexing, searching, and management.
 */
export class ElasticSearchService implements IElasticSearchClient {
    readonly client: ElasticClient;

    constructor(clientOrOptions?: ElasticClient | ElasticConfigOptions) {
        if (clientOrOptions && typeof (clientOrOptions as any).search === 'function') {
            this.client = clientOrOptions as ElasticClient;
        } else {
            this.client = createElasticClient(clientOrOptions as ElasticConfigOptions);
        }
    }

    /**
     * Index or overwrite a document.
     */
    async index<TDocument = unknown>(params: { index: string; id?: string; document: TDocument }): Promise<void> {
        await this.client.index({
            index: params.index,
            id: params.id,
            document: params.document
        });
    }

    /**
     * Partially update an existing document.
     */
    async update<TPartial = unknown>(params: { index: string; id: string; doc: Partial<TPartial> }): Promise<void> {
        await this.client.update({
            index: params.index,
            id: params.id,
            doc: params.doc
        });
    }

    /**
     * Retrieve a document by ID. Returns null if not found.
     */
    async get<TDocument = unknown>(params: { index: string; id: string }): Promise<TDocument | null> {
        try {
            const res = await this.client.get<TDocument>({
                index: params.index,
                id: params.id
            });
            return (res._source as TDocument) ?? null;
        } catch (error: any) {
            if (error?.meta?.statusCode === 404 || error?.statusCode === 404) {
                return null;
            }
            throw error;
        }
    }

    /**
     * Execute a search query against an index.
     */
    async search<TDocument = unknown>(params: {
        index: string;
        body?: Record<string, any>;
        query?: Record<string, any>;
        size?: number;
        from?: number;
    }): Promise<TDocument[]> {
        const res = await this.client.search<TDocument>({
            index: params.index,
            size: params.size,
            from: params.from,
            query: params.query,
            ...params.body
        });

        const hits = res.hits?.hits ?? [];
        return hits.map((hit) => hit._source as TDocument).filter(Boolean);
    }

    /**
     * Delete a document by ID. Returns true if deleted, false if not found.
     */
    async delete(params: { index: string; id: string }): Promise<boolean> {
        try {
            const res = await this.client.delete({
                index: params.index,
                id: params.id
            });
            return res.result === 'deleted';
        } catch (error: any) {
            if (error?.meta?.statusCode === 404 || error?.statusCode === 404) {
                return false;
            }
            throw error;
        }
    }

    /**
     * Count matching documents.
     */
    async count(params: { index: string; query?: Record<string, any> }): Promise<number> {
        const res = await this.client.count({
            index: params.index,
            query: params.query
        });
        return res.count;
    }

    /**
     * Ping cluster health.
     */
    async ping(): Promise<boolean> {
        try {
            return await this.client.ping();
        } catch {
            return false;
        }
    }
}
