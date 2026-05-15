import crypto from 'crypto';
import {env} from '../../lib/env'

const SECRET_KEY = env.ENCRYPTION_KEY;
const ALGORITHM = 'aes-256-gcm';

export function hashApiKey(text: string) {
  return crypto
    .createHmac('sha256', Buffer.from(SECRET_KEY))
    .update(text.trim(), 'utf8')
    .digest('hex');
}

export function encrypt(text: string) {
  const iv = crypto.randomBytes(16);
  const cipher = crypto.createCipheriv(ALGORITHM, Buffer.from(SECRET_KEY), iv);
  
  let encrypted = cipher.update(text, 'utf8', 'hex');
  encrypted += cipher.final('hex');
  const authTag = cipher.getAuthTag().toString('hex');

  return {
    iv: iv.toString('hex'),
    encryptedKey: `${encrypted}:${authTag}`
  };
}

// เตรียมไว้ใช้ตอน Chatbot ดึงไปใช้งาน
export function decrypt(encryptedText: string, ivHex: string) {
  const [encrypted, authTag] = encryptedText.split(':');
  const iv = Buffer.from(ivHex, 'hex');
  const decipher = crypto.createDecipheriv(ALGORITHM, Buffer.from(SECRET_KEY), iv);
  decipher.setAuthTag(Buffer.from(authTag, 'hex'));
  
  let decrypted = decipher.update(encrypted, 'hex', 'utf8');
  decrypted += decipher.final('utf8');
  return decrypted;
}
