import jwt from 'jsonwebtoken'
import { env } from '../../lib/env'
import { PrismaClient } from '@prisma/client'
import {Pool} from 'pg';
import { PrismaPg} from '@prisma/adapter-pg'

const pool = new Pool({ connectionString: env.DATABASE_URL as string })

// 3. ห่อด้วย Prisma Adapter
const adapter = new PrismaPg(pool)

// 4. สร้าง PrismaClient โดยยัด Adapter เข้าไป (ตรงนี้แหละที่มันเรียกร้องหาอยู่)
const prisma = new PrismaClient({ adapter })

const ACCESS_SECRET = env.ACCESS_SECRET as string
const REFRESH_SECRET = env.REFRESH_SECRET as string

export let jwtConfig = {} as {
    userAccess: string;
    guestAccess: string;
    refresh: string;
};

export const loadJwtConfig = async () => {
    const settings = await prisma.systemSettings.findUnique({ where: { id: 1 } })
    
    if (settings) {
        jwtConfig.userAccess = settings.userAccessExpiry
        jwtConfig.guestAccess = settings.guestAccessExpiry
        jwtConfig.refresh = settings.refreshExpiry
    } else {
        console.error('time not found in Database');
    }
}
export const signAccessToken = (payload: any) => {
    const expiry = payload.role === 'guest' ? jwtConfig.guestAccess : jwtConfig.userAccess
    return jwt.sign(payload, ACCESS_SECRET, { expiresIn: expiry as any})
}

export const signRefreshToken = (payload: any) => 
    jwt.sign(payload, REFRESH_SECRET, { expiresIn: jwtConfig.refresh as any })


export const verifyAccessToken = (token: string) =>
    jwt.verify(token, ACCESS_SECRET)

export const verifyRefreshToken = (token: string) =>
    jwt.verify(token, REFRESH_SECRET)

export const getUserIdFromToken = (token: string) => (verifyRefreshToken(token) as any).userId;