import jwt from 'jsonwebtoken'
import { env } from '../../lib/env'
import { prisma } from '../setup/prisma'

const ACCESS_SECRET = env.ACCESS_SECRET as string
const REFRESH_SECRET = env.REFRESH_SECRET as string

export const jwtConfig = {} as {
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
