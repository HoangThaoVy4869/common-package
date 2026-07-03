import fs from 'fs';
import path from 'path';
import type { ServiceBroker } from 'moleculer';

export interface LoadLocalesOptions {
    localesDir: string;
    override?: boolean;
    flagKey?: string;
}

export async function loadLocales(broker: ServiceBroker, options: LoadLocalesOptions) {
    const { localesDir, override = true, flagKey = 'i18nLocalesLoaded' } = options;

    if (broker.locals?.[flagKey]) return;

    if (!fs.existsSync(localesDir)) {
        broker.logger?.info?.(`Locales folder not found: ${localesDir}, skipping`);
        return;
    }

    await broker.waitForServices(['i18n']);

    const files = fs.readdirSync(localesDir).filter((f) => f.endsWith('.json'));
    for (const file of files) {
        const locale = path.basename(file, '.json');
        const payload = JSON.parse(fs.readFileSync(path.join(localesDir, file), 'utf8'));
        await broker.call('i18n.addLocale', {
            locale,
            messages: payload,
            override
        });
    }

    broker.locals = { ...broker.locals, [flagKey]: true };
}
