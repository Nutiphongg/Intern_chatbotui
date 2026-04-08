import { Pool } from 'pg'; 
import { PrismaPg } from '@prisma/adapter-pg'; 
import { PrismaClient } from '@prisma/client'; 

// ดึง URL จากไฟล์ .env ของคุณ
const connectionString = process.env.DATABASE_URL;

// 1. สร้างท่อเชื่อมต่อ (Connection Pool)
const pool = new Pool({ connectionString });

// 2. เอาท่อเชื่อมต่อ ไปสวมเข้ากับหัวแปลงของ Prisma (Adapter)
const adapter = new PrismaPg(pool);

// 3. ยัด Adapter เข้าไปใน PrismaClient (นี่แหละครับสิ่งที่ Prisma 7 ร้องขอ!)
export const prisma = new PrismaClient({ adapter });