import axios from 'axios';
import { ISmsService } from "../../core/interfaces";

export class TelegramSmsService implements ISmsService {
    private readonly botToken: string;
    private readonly chatId: string;

    constructor(botToken: string, chatId: string) {
        this.botToken = botToken;
        this.chatId = chatId;
    }

    async sendOTP(to: string, otp: string): Promise<void> {
        try {
            const message = `📲 OTP for ${to}: ${otp}`;
            await axios.post(
                `https://api.telegram.org/bot${this.botToken}/sendMessage`,
                {
                    chat_id: this.chatId,
                    text: message,
                }
            );
        } catch (error) {
            throw Error(`Failed to send OTP via Telegram: ${error}`);
        }
    }
}
