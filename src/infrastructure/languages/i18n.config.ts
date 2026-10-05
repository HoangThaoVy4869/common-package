export interface I18nConfigOptions {
    /**
     * Absolute or relative path to the locales folder (e.g. path.join(process.cwd(), 'account/locales'))
     */
    localesPath: string;
    /**
     * Fallback language if not resolved. Default: 'en'
     */
    fallbackLanguage?: string;
    /**
     * Watch files for hot reload in dev. Default: true (or false in production)
     */
    watch?: boolean;
    /**
     * Query parameter keys to resolve language from. Default: ['lang', 'locale', 'l']
     */
    queryKeys?: string[];
    /**
     * Header keys to resolve language from. Default: ['accept-language', 'x-custom-lang']
     */
    headerKeys?: string[];
    /**
     * Cookie keys to resolve language from. Default: ['lang', 'locale']
     */
    cookieKeys?: string[];
    /**
     * Custom resolvers if you want to override default resolvers
     */
    resolvers?: any[];
}

/**
 * Creates options object compatible with I18nModule.forRoot() or forRootAsync()
 */
export function createI18nOptions(options: I18nConfigOptions) {
    const {
        localesPath,
        fallbackLanguage = 'en',
        watch = process.env.NODE_ENV !== 'production',
        queryKeys = ['lang', 'locale', 'l'],
        headerKeys = ['accept-language', 'x-custom-lang'],
        cookieKeys = ['lang', 'locale'],
        resolvers
    } = options;

    return {
        fallbackLanguage,
        loaderOptions: {
            path: localesPath,
            watch
        },
        resolvers: resolvers || [
            { use: 'query', options: { keys: queryKeys } },
            { use: 'header', options: { keys: headerKeys } },
            { use: 'cookie', options: { keys: cookieKeys } }
        ]
    };
}

/**
 * Creates async options for I18nModule.forRootAsync(i18nConfig.i18n)
 */
export function createI18nAsyncConfig(options: I18nConfigOptions) {
    return {
        i18n: {
            useFactory: () => createI18nOptions(options)
        }
    };
}
