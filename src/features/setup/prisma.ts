import { Pool } from 'pg'; 
import { PrismaPg } from '@prisma/adapter-pg'; 
import { PrismaClient } from '@prisma/client'; 

// ดึง URL จากไฟล์ .env 
const connectionString = process.env.DATABASE_URL;

// 1. เชื่อมต่อ (Connection Pool)
const pool = new Pool({ connectionString });

// 2. เชื่อมต่อเข้าแปลงของ Prisma (Adapter)
const adapter = new PrismaPg(pool);

// 3. Adapter เข้าไปใน PrismaClient 
export const prisma = new PrismaClient({ adapter });