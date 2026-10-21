import type { ClassConstructor } from "class-transformer";

const runtimeRequire = eval("require") as NodeJS.Require;

export interface ValidateDtoOptions {
    whitelist?: boolean;
    forbidNonWhitelisted?: boolean;
    errorFactory?: (errors: any, className: string) => Error;
}

export async function validateDto<T>(
    cls: ClassConstructor<T>,
    payload: unknown,
    options: ValidateDtoOptions = {}
): Promise<T> {
    let plainToInstance: any;
    let validateOrReject: any;
    let BadRequestException: any;

    try {
        plainToInstance = (runtimeRequire("class-transformer") as typeof import("class-transformer")).plainToInstance;
        validateOrReject = (runtimeRequire("class-validator") as typeof import("class-validator")).validateOrReject;
    } catch {
        throw new Error(
            "[validateDto] Peer dependencies 'class-transformer' and 'class-validator' are required to use validateDto(). Please install them with: npm install class-transformer class-validator"
        );
    }

    try {
        BadRequestException = (runtimeRequire("@nestjs/common") as typeof import("@nestjs/common")).BadRequestException;
    } catch {
        BadRequestException = class HttpBadRequestError extends Error {
            constructor(public readonly response: any) {
                super(typeof response === "string" ? response : response?.message || "Bad Request");
                this.name = "BadRequestException";
            }
        };
    }

    const instance = plainToInstance(cls, payload);
    try {
        await validateOrReject(instance as object, {
            whitelist: options.whitelist ?? true,
            forbidNonWhitelisted: options.forbidNonWhitelisted ?? true,
        });
        return instance;
    } catch (error) {
        if (options.errorFactory) {
            throw options.errorFactory(error, cls.name);
        }
        throw new BadRequestException({
            message: `Validation failed: ${cls.name}`,
            errors: error,
        });
    }
}
