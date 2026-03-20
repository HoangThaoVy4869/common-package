import { ISmsService } from "../../core/interfaces";
import { TelegramSmsService, TwilioService } from "../sms";

class TwilioSmsAdapter implements ISmsService {
    private twilio: TwilioService;

    constructor(twilio: TwilioService) {
        this.twilio = twilio;
    }

    async sendOTP(to: string, otp: string): Promise<void> {
        await this.twilio.sendOTP(to, "sms", `Your OTP is: ${otp}`);
    }
}
export class SmsServiceFactory {
    static create(): ISmsService {
        const env = process.env.SMS_PROVIDER || "telegram";

        switch (env) {
            case "twilio":
                return new TwilioSmsAdapter(
                    new TwilioService({
                        accountSid: process.env.TWILIO_ACCOUNT_SID!,
                        authToken: process.env.TWILIO_AUTH_TOKEN!,
                        from: process.env.FROM_TWILIO!,
                    })
                );
            default:
            case "telegram":
                return new TelegramSmsService(
                    process.env.TELEGRAM_BOT_TOKEN!,
                    process.env.TELEGRAM_CHAT_ID!
                );
        }
    }
}
