import { ILanguageService, TranslateOptions } from '../../core/interfaces/ILanguage';

export interface II18nTranslator {
    translate(key: string, options?: { lang?: string; args?: Record<string, any>; defaultValue?: string }): any;
}

export class LanguageService implements ILanguageService {
    constructor(
        private readonly i18n?: II18nTranslator,
        private readonly defaultLanguage: string = 'en'
    ) {}

    async translate(
        key: number | string,
        langOrOptions?: string | TranslateOptions,
        params?: Record<string, any>
    ): Promise<string> {
        return this.translateSync(key, langOrOptions, params);
    }

    translateSync(
        key: number | string,
        langOrOptions?: string | TranslateOptions,
        params?: Record<string, any>
    ): string {
        const stringKey = String(key);
        let lang: string | undefined;
        let args: Record<string, any> | undefined;
        let fallback: string | undefined;

        if (typeof langOrOptions === 'string') {
            lang = langOrOptions;
            args = params;
        } else if (langOrOptions && typeof langOrOptions === 'object') {
            lang = langOrOptions.lang;
            args = langOrOptions.args || params;
            fallback = langOrOptions.fallback ?? langOrOptions.defaultValue;
        }

        const targetLang = lang || this.defaultLanguage;

        if (!this.i18n) {
            return fallback !== undefined ? fallback : stringKey;
        }

        try {
            const result = this.i18n.translate(stringKey, {
                lang: targetLang,
                args,
                defaultValue: fallback
            });

            const translated = result != null ? String(result) : undefined;
            if (!translated || translated === stringKey) {
                return fallback !== undefined ? fallback : stringKey;
            }
            return translated;
        } catch {
            return fallback !== undefined ? fallback : stringKey;
        }
    }
}
