import { ServiceSchema } from 'moleculer';

type Messages = Record<string, string>;
type LocaleStore = Record<string, Messages>;

const messages: LocaleStore = {};

function mergeMessages(target: Messages = {}, src: Messages = {}) {
    return Object.assign(target, src);
}

const I18nService: ServiceSchema = {
    name: 'i18n',

    settings: {
        defaultLocale: 'en'
    },

    actions: {
        addLocale: {
            params: {
                locale: 'string',
                messages: 'object',
                override: { type: 'boolean', optional: true }
            },
            handler(ctx) {
                const {
                    locale,
                    messages: newMsgs,
                    override = true
                } = ctx.params as {
                    locale: string;
                    messages: Record<string, string>;
                    override?: boolean;
                };

                messages[locale] = messages[locale] || {};

                if (override) {
                    messages[locale] = mergeMessages(messages[locale], newMsgs);
                } else {
                    Object.keys(newMsgs).forEach((k) => {
                        if (messages[locale][k] === undefined) {
                            messages[locale][k] = newMsgs[k];
                        }
                    });
                }

                return { ok: true, locale, added: Object.keys(newMsgs).length };
            }
        },
        translate: {
            params: {
                key: 'string',
                locale: { type: 'string', optional: true },
                params: { type: 'object', optional: true },
                fallback: { type: 'string', optional: true }
            },
            handler(ctx) {
                const {
                    key,
                    locale,
                    params = {},
                    fallback
                } = ctx.params as {
                    key: string;
                    locale?: string;
                    params?: Record<string, any>;
                    fallback?: string;
                };

                const loc = locale || this.settings.defaultLocale || 'en';
                const dict = messages[loc] || {};

                let text = dict[key] ?? fallback ?? key;

                Object.keys(params).forEach((p) => {
                    text = text.replace(new RegExp(`{${p}}`, 'g'), String(params[p]));
                });

                return text;
            }
        }
    }
};

export default I18nService;
