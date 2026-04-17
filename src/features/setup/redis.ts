
import Redis from 'ioredis';
import { env } from '../../lib/env';

// สร้าง Instance ของ Redis 
export const redis = new Redis(env.REDIS_URL);

// ใส่ Event Listener ไ (เอาไว้ Debug)
redis.on('connect', () => {
    console.log(' Redis connected successfully!');
});

redis.on('error', (err) => {
    console.error(' Redis connection error:', err);
});