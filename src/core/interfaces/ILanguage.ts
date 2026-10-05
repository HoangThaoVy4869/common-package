export interface TranslateOptions {
    lang?: string;
    args?: Record<string, any>;
    fallback?: string;
    defaultValue?: string;
}

export interface ILanguageService {
    translate(
        key: number | string,
        langOrOptions?: string | TranslateOptions,
        params?: Record<string, any>
    ): Promise<string> | string;
}