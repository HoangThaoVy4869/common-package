import { plainToInstance, ClassConstructor } from "class-transformer";
import { validateOrReject } from "class-validator";
import { BadRequestException } from "@nestjs/common";

export async function validateDto<T>(
    cls: ClassConstructor<T>,
    payload: unknown,
): Promise<T> {
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