import jwt from 'jsonwebtoken'
import { env } from '../../lib/env'

const ACCESS_SECRET = env.ACCESS_SECRET
const REFRESH_SECRET = env.REFRESH_SECRET

export const signAccessToken = (payload: any) => {
    const expiry = payload.role === 'guest' ? '2m': '10m'
    return jwt.sign(payload,ACCESS_SECRET,{expiresIn: expiry})
}
export const signRefreshToken = (payload: any) => 
    jwt.sign(payload,REFRESH_SECRET,{expiresIn: '7d'})

export const verifyAccessToken = (token: string) =>
    jwt.verify(token,ACCESS_SECRET)

export const verifyRefreshToken = (token: string) =>
    jwt.verify(token,REFRESH_SECRET)

export const getUserIdFromToken = (token: string) => (verifyRefreshToken(token)as any).userId;