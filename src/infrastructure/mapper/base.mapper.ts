export abstract class BaseMapper<TEntity, TPersistence> {
    abstract toEntity(doc: TPersistence | null): TEntity | null;
    abstract toPersistence(entity: TEntity): TPersistence;

    toPartialPersistence(entity: any): any {
        if (!entity.getChanges) return this.toPersistence(entity);

        const changes = entity.getChanges();
        const persistence: any = {};

        for (const field of changes) {
            const getter = `get${field.charAt(0).toUpperCase() + field.slice(1)}`;
            if (typeof entity[getter] === 'function') {
                const value = entity[getter]();
                persistence[field] = value?.getValue ? value.getValue() : value;
            } else {
                persistence[field] = (entity as any)[field];
            }
        }
        return persistence;
    }

    getIdentityFilter(entity: TEntity): Record<string, any> {
        return { _id: (entity as any).id };
    }
}
