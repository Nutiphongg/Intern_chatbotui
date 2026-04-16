
import { Elysia } from 'elysia';
import { Errors } from '../lib/errors';
import { verifyAccessToken } from '../features/auth/jwt'; 


export const authPlugin =  (app: Elysia) =>
    //  ใช้ .derive() เพื่อสร้างตัวแปรและส่งต่อ Type
    app.derive(async ({ headers }) => {
        // 1. ดึง Token จาก Header
        const auth = headers.authorization;
        if (!auth || !auth.startsWith('Bearer ')) {
            throw Errors.missingToken(); 
        }

        const token = auth.split(' ')[1];

        try {
            // 2. ถอดรหัส Token 
            const payload = verifyAccessToken(token) as { userId: string };

          
            return {
                user: {
                    id: payload.userId
                }
            };
        } catch (error) {
            throw Errors.invalidToken();
        }
    });