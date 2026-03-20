import type { ServiceBroker } from "moleculer";
import { ILanguageService } from "../../core/interfaces/ILanguage";

export class LanguageService implements ILanguageService {
    constructor(private readonly broker: ServiceBroker) { }

    async translate(key: number, locale?: string, params?: Record<string, any>, fallback?: string): Promise<string> {
        return this.broker.call("i18n.translate", { key, locale, params, fallback }, { timeout: 5000 });
    }
}
