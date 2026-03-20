export interface ITwilioService {
    sendOTP(to: string, channel: 'sms' | 'call', body: string): Promise<void | string>;
}