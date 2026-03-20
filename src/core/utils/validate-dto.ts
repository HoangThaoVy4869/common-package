import type { ClassConstructor } from "class-transformer";

const runtimeRequire = eval("require") as NodeJS.Require;

export async function validateDto<T>(
    cls: ClassConstructor<T>,
    payload: unknown,
): Promise<T> {
    const { plainToInstance } = runtimeRequire("class-transformer") as typeof import("class-transformer");
    const { validateOrReject } = runtimeRequire("class-validator") as typeof import("class-validator");
    const { BadRequestException } = runtimeRequire("@nestjs/common") as typeof import("@nestjs/common");
    const instance = plainToInstance(cls, payload);
    try {
        await validateOrReject(instance as object, {
            whitelist: true,
            forbidNonWhitelisted: true,
        });
        return instance;
    } catch (error) {
        throw new BadRequestException({
            message: `Validation failed: ${cls.name}`,
            errors: error,
        });
    }
}
