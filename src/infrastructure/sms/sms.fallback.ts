import { ISmsService } from "../../core/interfaces";

export class SmsServiceWithFallback implements ISmsService {
    private primary: ISmsService;
    private fallback: ISmsService;

    constructor(primary: ISmsService, fallback: ISmsService) {
        this.primary = primary;
        this.fallback = fallback;
    }

    async sendOTP(to: string, otp: string): Promise<void> {
        try {
            await this.primary.sendOTP(to, otp);
        } catch (error) {
            await this.fallback.sendOTP(to, otp);
            throw Error(`Primary SMS service failed, fallback used. Error: ${error}`);
        }
    }
}
