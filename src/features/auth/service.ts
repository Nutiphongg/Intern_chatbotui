import bcrypt from 'bcrypt';
import { RegisterBody ,LoginBody} from './types';
import { signAccessToken,signRefreshToken,verifyRefreshToken } from './jwt';
import { prisma } from '../setup/prisma'
import { HttpError } from '../../lib/problem';
import { Errors} from '../../lib/errors';
import { ulid } from 'ulid'
import { redis } from "../setup/redis"
import { GuestUser } from './interface';
//ระบบลงทะเบียนผู้ใช้ใหม่
export const Register = async (data: RegisterBody) => {
    //ตรวจสอบ Email หรือ Username ในระบบ
    const existingUser = await prisma.users.findFirst({
        where: {
            OR: [
                { email: data.email },
                { username: data.username }
            ]
        }
    });

    //   ถ้าเจอว่ามีข้อมูลซ้ำ โยน Error 
    if (existingUser) {
        throw Errors.userAlreadyExists(); 
    }
    // 1. ทำการ hash password
    const salt = await bcrypt.genSalt(10);
    const passwordHash = await bcrypt.hash(data.password, salt);

        const newUserId = ulid();
    // 2. ใช้ prisma สร้าง user ใหม่
        const newUser = await prisma.users.create({
            // ข้อมูลที่ต้องบันทึก
            data: {
                id: newUserId,
                email: data.email,
                username: data.username,
                password_hash: passwordHash,
            },
            //ส่งข้อมูลกลับ
            select: {
                id:true,
                email:true,
                username: true,
            }
        });
        return newUser;
};
//ระบบเข้าสู่ระบบ (Login)
export const Login = async (body: LoginBody ,userAgent:string ,ip:string ) => {
    const { email,password } = body;
    // 1. ค้นหา User จาก Email
    const user = await prisma.users.findFirst({
        where: {email: email }
    });
    // ไม่พบ User ให้โยน Error
    if(!user){
         throw Errors.invalidCredentials()
    
    }
    // 2. ตรวจสอบรหัสผ่าน
    const ok = await bcrypt.compare(password,user.password_hash)

    if(!ok){
        throw Errors.invalidCredentials()
    }
    // 3. จัดการจำนวน Session (จำกัดการ Login ไว้ไม่เกิน 3 เครื่อง)
    const sessions = await prisma.sessions.findMany({
        where:{
            user_id: user.id,
            expires_at:{gt: new Date()}// เอาเฉพาะที่ยังไม่หมดอายุ
        },
        orderBy: { created_at:'asc'}
    });
    //login เกินให้ลบแบบ(FIFO)
    if (sessions.length >= 3) {
        const oldest = sessions[0];
        await prisma.sessions.delete({
            where: { id: oldest.id }
        });
    }

    //4. สร้าง Access Token และ Refresh Token
    const accessToken = signAccessToken({userId: user.id})
    const refreshToken = signRefreshToken({userId: user.id})

    // 5. บันทึก Session ใหม่ลง Database
    const expiresAt = new Date();
    expiresAt.setDate(expiresAt.getDate() + 7);
    const newSessionId = ulid();

    await prisma.sessions.create({
        data: {
            id: newSessionId,
            user_id: user.id,
            refresh_token: refreshToken,
            user_agent: userAgent,
            ip_address: ip,
            expires_at: expiresAt
        }
    });
    // ส่งข้อมูล Token และข้อมูล User
    return {
        accessToken,
        refreshToken,
        user: {
            id: user.id,
            email: user.email,
            username: user.username
        }
    }
} 

//ระบบต่ออายุ Token (Refresh Token Rotation)

export const Refresh = async (refreshToken: string | undefined,userAgent:string ,ip:string) => { 
    // ตรวจสอบว่ามี Token
    if(!refreshToken) {
         throw Errors.missingToken();
    }

    // ตรวจสอบความถูกต้อง JWT
    let payload: any;
    try {
        payload = verifyRefreshToken(refreshToken);
    } catch (error:any){
        // ลบ token ออกจาก db
        await prisma.sessions.deleteMany({
            where: { refresh_token: refreshToken }
        });
         throw Errors.invalidToken()
    }

    // 2. เช็คว่า Session นี้ยังมีอยู่ใน Database
   const session = await prisma.sessions.findFirst({
        where: { refresh_token: refreshToken }
    });

    if (!session) {
        throw Errors.sessionNotFound();
    }

    const userId = payload.userId;

    // 3. ทำการ Token Rotation (สร้างคู่ใหม่)
    const newAccessToken = signAccessToken({ userId });
    const newRefreshToken = signRefreshToken({ userId });

    // 4. ลบ refreshtoken เก่าออก
    await prisma.sessions.deleteMany({
        where: { refresh_token: refreshToken }
    });

    // 5. บันทึก Refresh Token เพิ่มเข้ามาใหม่
    const expiresAt = new Date();
    expiresAt.setDate(expiresAt.getDate() + 7);

    await prisma.sessions.create({
        data: {
            id: ulid(),
            user_id: userId,
            refresh_token: newRefreshToken,
            user_agent: userAgent,
            ip_address: ip,
            expires_at: expiresAt
        }
    });

    // ส่ง Token คู่ใหม่กลับไปให้ Client
    return {
        newAccessToken:newAccessToken,
        newRefreshToken: newRefreshToken
    };
};

//ดึงข้อมูลอุปกรณ์ที่กำลังใช้งานอยู่ (Active Sessions)
export const getActiveDevices = async (userId: string) => {
  const result = await prisma.sessions.groupBy({
    by: ['user_agent'],// จัดกลุ่มตามประเภทอุปกรณ์ (Browser/OS)
    where: {
      user_id: userId,
      expires_at: {
        gt: new Date(),// เอาเฉพาะที่ยังไม่หมดอายุ
      },
    },
  });

  return {user: result.length,// จำนวนอุปกรณ์ที่ออนไลน์อยู่
         devices:result// รายการอุปกรณ์
         }
};

//ระบบออกจากระบบ (Logout)
export const Logout = async (refreshToken: string | undefined) => {
    // ถ้าไม่มี Token ส่งมา ให้แจ้ง Error
    if (!refreshToken) {
        throw Errors.missingToken();
    }

    await prisma.sessions.deleteMany({
        where: { refresh_token: refreshToken }
    });

    return {
        message: 'logout success'
    };
};

const GUEST_SESSION_TTL = 3600; // 1 ชั่วโมง (วินาที)

export const createGuestUser = async (): Promise<GuestUser> => {
  const guestId = `guest_${ulid()}`;
  
  const guestUser: GuestUser = {
    id: guestId,
    isGuest: true,
    createdAt: new Date()
  };

  await redis.setex(
    `guest:${guestId}`, 
    GUEST_SESSION_TTL, 
    JSON.stringify(guestUser)
  );

  return guestUser;
};

export const verifyGuestUser = async (guestId: string): Promise<GuestUser | null> => {
  const data = await redis.get(`guest:${guestId}`);
  return data ? JSON.parse(data) : null;
};
