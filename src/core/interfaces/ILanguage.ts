export interface ILanguageService {
    translate(key: number | string, lang?: string, params?: Record<string, any>): Promise<string>;
}