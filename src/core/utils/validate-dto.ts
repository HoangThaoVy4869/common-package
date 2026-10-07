import type { ClassConstructor } from "class-transformer";

const runtimeRequire = eval("require") as NodeJS.Require;

export async function validateDto<T>(
    cls: ClassConstructor<T>,
    payload: unknown,
): Promise<T> {
    let plainToInstance: any;
    let validateOrReject: any;
    let BadRequestException: any;

    try {
        plainToInstance = (runtimeRequire("class-transformer") as typeof import("class-transformer")).plainToInstance;
        validateOrReject = (runtimeRequire("class-validator") as typeof import("class-validator")).validateOrReject;
        BadRequestException = (runtimeRequire("@nestjs/common") as typeof import("@nestjs/common")).BadRequestException;
    } catch {
        throw new Error(
            "[validateDto] Peer dependencies 'class-transformer', 'class-validator', and '@nestjs/common' are required to use validateDto(). Please install them with: npm install class-transformer class-validator @nestjs/common"
        );
    }

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
