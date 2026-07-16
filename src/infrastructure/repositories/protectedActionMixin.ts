import type { Context, Service, ServiceSchema } from "moleculer";
import _ from "lodash";

interface MongooseService extends Service {
    adapter: {
        model: any;
    };
}
/**
 * ProtectedDbMixin
 * --------------------------
 * - Thay thế toàn bộ CRUD mặc định của moleculer-db
 * - Dùng handler mongoose custom
 * - Tự động set visibility = "protected"
 */
const ProtectedDbMixin: ServiceSchema = {
    name: "protected-db-mixin",
    actions: {},
};

const mongooseActions = {
    aggregate: {
        rest: false,
        async handler(this: MongooseService, ctx: Context) {
            return this.adapter.model.aggregate(...(ctx.params as any[]));
        },
    },
    aggregateWithHint: {
        rest: false,
        async handler(this: MongooseService, ctx: Context) {
            const [pipeline, options = {}] = ctx.params as any[];
            if (options.hint) {
                return this.adapter.model.aggregate(pipeline).hint(options.hint);
            }
            return this.adapter.model.aggregate(pipeline);
        },
    },
    create: {
        rest: false,
        async handler(this: MongooseService, ctx: Context) {
            const doc = new this.adapter.model(ctx.params);
            return doc.save();
        },
    },
    findOne: {
        rest: false,
        async handler(this: MongooseService, ctx: Context) {
            return this.adapter.model.findOne(...(ctx.params as any[])).lean();
        },
    },
    findMany: {
        rest: false,
        async handler(this: MongooseService, ctx: Context) {
            return this.adapter.model.find(...(ctx.params as any[])).lean();
        },
    },
    findOneAndUpdate: {
        rest: false,
        async handler(this: MongooseService, ctx: Context) {
            return this.adapter.model.findOneAndUpdate(...(ctx.params as any[]));
        },
    },
    delete: {
        rest: false,
        async handler(this: MongooseService, ctx: Context) {
            return this.adapter.model.deleteOne(...(ctx.params as any[]));
        },
    },
    deleteMany: {
        rest: false,
        async handler(this: MongooseService, ctx: Context) {
            return this.adapter.model.deleteMany(...(ctx.params as any[]));
        },
    },
    updateOne: {
        rest: false,
        async handler(this: MongooseService, ctx: Context) {
            return this.adapter.model.updateOne(...(ctx.params as any[]));
        },
    },
    update: {
        rest: false,
        async handler(this: MongooseService, ctx: Context) {
            return this.adapter.model.updateOne(...(ctx.params as any[]));
        },
    },
    updateMany: {
        rest: false,
        async handler(this: MongooseService, ctx: Context) {
            return this.adapter.model.updateMany(...(ctx.params as any[]));
        },
    },
    findOneSecondary: {
        async handler(this: MongooseService, ctx: Context) {
            return this.adapter.model.findOne(...(ctx.params as any[])).read("secondary").lean();
        },
    },
    findManySecondary: {
        async handler(this: MongooseService, ctx: Context) {
            return this.adapter.model.find(...(ctx.params as any[])).read("secondary").lean();
        },
    },
    aggregateSecondary: {
        async handler(this: MongooseService, ctx: Context) {
            return this.adapter.model.aggregate(...(ctx.params as any[])).read("secondary");
        },
    },
    countRow: {
        rest: false,
        async handler(this: MongooseService, ctx: Context) {
            return this.adapter.model.countDocuments(...(ctx.params as any[]));
        },
    },
    distinct: {
        rest: false,
        async handler(this: MongooseService, ctx: Context) {
            const [field, filter] = ctx.params as any[];
            return this.adapter.model.distinct(field, filter);
        },
    },
    bulkWrite: {
        rest: false,
        async handler(this: MongooseService, ctx: Context) {
            return this.adapter.model.bulkWrite(ctx.params);
        },
    },
    findOneAndDelete: {
        rest: false,
        async handler(this: MongooseService, ctx: Context) {
            return this.adapter.model.findOneAndDelete(...(ctx.params as any[]));
        },
    },
    replaceOne: {
        rest: false,
        async handler(this: MongooseService, ctx: Context) {
            return this.adapter.model.replaceOne(...(ctx.params as any[]));
        },
    },
};


_.forEach(mongooseActions, (v, k) => {
    ProtectedDbMixin.actions![k] = {
        visibility: "protected",
        ...(v as any),
    };
});

export default ProtectedDbMixin;
