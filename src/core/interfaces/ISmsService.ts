export interface ISmsService {
    sendOTP(to: string, otp: string): Promise<void>;
}