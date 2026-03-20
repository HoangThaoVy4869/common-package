import twilio, { Twilio } from "twilio";
import { ITwilioService } from "../../core/interfaces";

export class TwilioService implements ITwilioService {
    private client: Twilio;
    private from: string;

    constructor(config: { accountSid: string; authToken: string; from: string }) {
        this.client = twilio(config.accountSid, config.authToken);
        this.from = config.from;
    }

    async sendOTP(to: string, channel: "sms" | "call", body: string): Promise<void> {
        try {
            if (channel === "sms") {
                await this.client.messages.create({
                    body,
                    from: this.from,
                    to,
                });
            } else if (channel === "call") {
                await this.client.calls.create({
                    url: "http://demo.twilio.com/docs/voice.xml",
                    from: this.from,
                    to,
                });
            }
        } catch (error: any) {
            throw Error(`Failed to send OTP via Twilio: ${error.message}`);
        }
    }
}
