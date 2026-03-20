export interface BaseResponse<T = any> {
    success: boolean;
    code: number;
    message: string;
    data?: T;
}

export class ResponseHandler {
    static success<T>(code: number, message: string, data?: T): BaseResponse<T> {
        return {
            success: true,
            code,
            message,
            data,
        };
    }

    static error<T>(code: number, message: string, data?: T): BaseResponse<T> {
        return {
            success: false,
            code,
            message,
            data,
        };
    }
}