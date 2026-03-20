import { v4 as uuidv4 } from 'uuid';

export function generateShortId(): string {
    return uuidv4().replace(/-/g, '').slice(0, 18);
}

export function generateProductId() {
    return `prd_${generateShortId()}`;
}

export function generateVariantId() {
    return `var_${generateShortId()}`;
}

export function generatePriceId() {
    return `prc_${generateShortId()}`;
}

export function generateInventoryId() {
    return `inv_${generateShortId()}`;
}
