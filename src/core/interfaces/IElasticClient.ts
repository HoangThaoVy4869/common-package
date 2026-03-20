export interface IElasticSearchClient {
    index<TDocument>(params: { index: string; id: string; document: TDocument; }): Promise<void>;
    update<TPartial>(params: { index: string; id: string; doc: Partial<TPartial>; }): Promise<void>;
    get<TDocument>(params: { index: string; id: string; }): Promise<TDocument | null>;
    search<TDocument>(params: { index: string; body: Record<string, any>; }): Promise<TDocument[]>;
}
