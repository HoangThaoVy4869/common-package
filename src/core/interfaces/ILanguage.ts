export interface ILanguageService {
    translate(key: number, lang?: string, params?: Record<string, any>): Promise<string>;
}