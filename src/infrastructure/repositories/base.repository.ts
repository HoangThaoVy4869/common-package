import { Model, QueryFilter, UpdateQuery } from 'mongoose';

export interface IRepositoryMapper<TDomain, TPersistence> {
    toEntity(persistence: TPersistence | null): TDomain | null;
    toPersistence(domain: TDomain): TPersistence;
    toPartialPersistence?(domain: TDomain): Partial<TPersistence>;
}
export abstract class BaseRepository<TDomain, TPersistence> {
    protected constructor(
        protected readonly model: Model<TPersistence>,
        protected readonly mapper: IRepositoryMapper<TDomain, TPersistence>
    ) { }

    // DOMAIN DRIVEN - DOMAIN FACING QUERY
    async findById(id: string): Promise<TDomain | null> {
        const doc = await this.model
            .findOne({ id } as QueryFilter<TPersistence>)
            .lean<TPersistence>()
            .exec();

        return this.mapper.toEntity(doc);
    }

    async create(entity: TDomain): Promise<TDomain> {
        const persistence = this.mapper.toPersistence(entity);
        const created = await this.model.create(persistence);
        return this.mapper.toEntity(created.toObject() as TPersistence)!;
    }

    async save(filter: QueryFilter<TPersistence>, entity: TDomain): Promise<TDomain> {
        const updateData = this.mapper.toPartialPersistence
            ? this.mapper.toPartialPersistence(entity)
            : this.mapper.toPersistence(entity);

        if (!updateData || Object.keys(updateData).length === 0) {
            return entity;
        }

        const updated = await this.model.findOneAndUpdate(
            filter,
            { $set: updateData } as UpdateQuery<TPersistence>,
            {
                new: true,
                upsert: false
            }
        )
            .lean<TPersistence>()
            .exec();

        if (!updated) {
            throw new Error('Entity not found for update');
        }
        return this.mapper.toEntity(updated)!;
    }


    async deleteById(id: string): Promise<boolean> {
        const result = await this.model.deleteOne({ id } as QueryFilter<TPersistence>).exec();
        return (result.deletedCount ?? 0) > 0;
    }

    // DATA DRIVENT - RAW QUERY
    async findOneByFilter(filter: QueryFilter<TPersistence>,
        options?: { sort?: Record<string, 1 | -1>; }
    ): Promise<TDomain | null> {
        const query = this.model.findOne(filter);

        if (options?.sort) {
            query.sort(options.sort);
        }

        const doc = await query.lean<TPersistence>().exec();
        return this.mapper.toEntity(doc);
    }

    async findManyByFilter(
        filter: QueryFilter<TPersistence> = {},
        options?: { limit?: number; sort?: Record<string, 1 | -1>; }
    ): Promise<TDomain[]> {
        const query = this.model.find(filter);

        if (options?.sort) {
            query.sort(options.sort);
        }

        if (options?.limit) {
            query.limit(options.limit);
        }

        const docs = await query.lean<TPersistence[]>().exec();

        return docs
            .map((d) => this.mapper.toEntity(d))
            .filter((e): e is TDomain => e !== null);
    }

    async updateOneByFilter(filter: QueryFilter<TPersistence>, fields: Partial<TPersistence>): Promise<TDomain | null> {
        const updated = await this.model.findOneAndUpdate(
            filter,
            { $set: fields } as UpdateQuery<TPersistence>,
            { new: true }
        )
            .lean<TPersistence>()
            .exec();

        return updated ? this.mapper.toEntity(updated) : null;
    }

    async updateManyByFilter(filter: QueryFilter<TPersistence>, fields: Partial<TPersistence>): Promise<number> {
        const result = await this.model
            .updateMany(
                filter,
                { $set: fields } as UpdateQuery<TPersistence>
            )
            .exec();

        return result.modifiedCount ?? 0;
    }

    async existsByFilter(filter: QueryFilter<TPersistence>): Promise<boolean> {
        return !!(await this.model.exists(filter));
    }

    async countByFilter(filter: QueryFilter<TPersistence> = {}): Promise<number> {
        return this.model.countDocuments(filter).exec();
    }

    async deleteOneByFilter(filter: QueryFilter<TPersistence>): Promise<boolean> {
        const result = await this.model.deleteOne(filter).exec();
        return (result.deletedCount ?? 0) > 0;
    }
}
