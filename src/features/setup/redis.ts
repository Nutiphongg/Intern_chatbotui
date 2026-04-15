
import Redis from 'ioredis';

// ดึง URL ของ Redis จากไฟล์ .env 
const REDIS_URL = process.env.REDIS_URL || 'redis://localhost:6379';

// สร้าง Instance ของ Redis 
export const redis = new Redis(REDIS_URL);

// ใส่ Event Listener ไ (เอาไว้ Debug)
redis.on('connect', () => {
    console.log('✅ Redis connected successfully!');
});

redis.on('error', (err) => {
    console.error('❌ Redis connection error:', err);
});