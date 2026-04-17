import jwt from 'jsonwebtoken'
import { env } from '../../lib/env'

const ACCESS_SECRET = env.ACCESS_SECRET
const REFRESH_SECRET = env.REFRESH_SECRET

export const signAccessToken = (payload: any) => 
    jwt.sign(payload,ACCESS_SECRET,{expiresIn: '10m'})

export const signRefreshToken = (payload: any) => 
    jwt.sign(payload,REFRESH_SECRET,{expiresIn: '7d'})

export const verifyAccessToken = (token: string) =>
    jwt.verify(token,ACCESS_SECRET)

export const verifyRefreshToken = (token: string) =>
    jwt.verify(token,REFRESH_SECRET)

export const getUserIdFromToken = (token: string) => (verifyRefreshToken(token)as any).userId;