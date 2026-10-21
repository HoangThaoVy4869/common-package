export interface IElasticSearchClient {
    /** Index or replace a document in Elasticsearch. */
    index<TDocument = unknown>(params: { index: string; id?: string; document: TDocument }): Promise<void>;
    /** Partially update an existing document. */
    update<TPartial = unknown>(params: { index: string; id: string; doc: Partial<TPartial> }): Promise<void>;
    /** Retrieve a document by ID. Returns null if not found. */
    get<TDocument = unknown>(params: { index: string; id: string }): Promise<TDocument | null>;
    /** Search documents matching a query. */
    search<TDocument = unknown>(params: {
        index: string;
        body?: Record<string, any>;
        query?: Record<string, any>;
        size?: number;
        from?: number;
    }): Promise<TDocument[]>;
    /** Delete a document by ID. Returns true if deleted, false if not found. */
    delete(params: { index: string; id: string }): Promise<boolean>;
    /** Count documents matching a query. */
    count(params: { index: string; query?: Record<string, any> }): Promise<number>;
    /** Health check ping to the Elasticsearch cluster. */
    ping(): Promise<boolean>;
}

export type IElasticClient = IElasticSearchClient;
