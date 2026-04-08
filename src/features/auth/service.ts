import bcrypt from 'bcrypt';
import { db } from '../setup/db';
import { RegisterBody ,LoginBody} from './types';
import { signAccessToken,signRefreshToken,verifyRefreshToken } from './jwt';
import { prisma } from '../setup/prisma'
import { HttpError } from '../../lib/problem';
import { Errors} from '../../lib/errors';
export const Register = async (data: RegisterBody) => {
    // 1. ทำการ hash password
    const salt = await bcrypt.genSalt(10);
    const passwordHash = await bcrypt.hash(data.password, salt);

    // 2. บันทึกลงใน db (ใช้ pg Pool ต้องเขียน SQL สด)
    const query = `
        INSERT INTO users (email, username, password_hash)
        VALUES ($1, $2, $3)
        RETURNING id, email, username;
    `;
    const values = [data.email, data.username, passwordHash];
    
    // สั่งรัน SQL
    const result = await db.query(query, values);
    
    // ข้อมูลที่เรา Return กลับมาจาก SQL จะอยู่ใน result.rows[0]
    const newUser = result.rows[0];

    // 3. ส่งข้อมูลไป Route
    return {
        id: newUser.id,
        email: newUser.email,
        username: newUser.username,
    };
}

export const Login = async (body: LoginBody ,userAgent:string ,ip:string ) => {
    const { email,password } = body;

    const result = await db.query(
        'SELECT * FROM users WHERE email = $1',
        [email]
    )

    const user = result.rows[0]

    if(!user){
         throw Errors.invalidCredentials()
    
    }

    const ok = await bcrypt.compare(password,user.password_hash)

    if(!ok){
        throw Errors.invalidCredentials()
    }
    const sessions = await db.query(
    `SELECT id FROM sessions 
     WHERE user_id = $1 
     AND expires_at > NOW()
     ORDER BY created_at ASC`,
    [user.id]
    )
    if (sessions.rows.length >= 3) {
    const oldest = sessions.rows[0]

    await db.query(
      `DELETE FROM sessions WHERE id = $1`,
      [oldest.id]
    )
  }
    // สร้าง token
    const accessToken = signAccessToken({userId: user.id})
    const refreshToken = signRefreshToken({userId: user.id})

    // บันทึก token
    await db.query(
         `INSERT INTO sessions (user_id, refresh_token,user_agent, ip_address, expires_at)
         VALUES ($1, $2, $3, $4, NOW() + INTERVAL '7 days')`,
         [user.id, refreshToken,userAgent,ip]
    )

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

// 1. รับค่าเป็น String ธรรมดา ไม่รับ Object Cookie แล้ว
export const Refresh = async (refreshToken: string | undefined) => { 
    if(!refreshToken) {
         throw Errors.missingToken()
    }

    // ตรวจสอบ JWT
    let payload: any;
    try {
        payload = verifyRefreshToken(refreshToken);
    } catch (error:any){
        // ลบ token ออกจาก db
        await db.query(`DELETE FROM sessions WHERE refresh_token = $1`, [refreshToken]);
        // ลบเสร็จ กลับไปที่ route
         throw Errors.invalidToken()
    }

    // เช็คใน db
    const session = await db.query(
        `SELECT * FROM sessions WHERE refresh_token = $1`, [refreshToken]
    );

    if(session.rows.length === 0){
        throw Errors.sessionNotFound()

    }

    const userId = payload.userId;

    // Rotate token (สร้างคู่ใหม่)
    const newAccessToken = signAccessToken({ userId });
    const newRefreshToken = signRefreshToken({ userId });

    // ลบ refreshtoken เก่าออก
    await db.query(
        `DELETE FROM sessions WHERE refresh_token = $1`, [refreshToken]
    );

    // เพิ่มเข้ามาใหม่
    await db.query(
        `INSERT INTO sessions (user_id, refresh_token, expires_at) 
         VALUES ($1, $2, NOW() + INTERVAL '7 days')`, 
         [userId, newRefreshToken]
    );

    // 2. คืนค่า Token คู่ใหม่กลับไปให้ Route จัดการต่อ
    return {
        newAccessToken,
        newRefreshToken
    };
};

export const getActiveDevices = async (userId: string) => {
  const result = await prisma.sessions.groupBy({
    by: ['user_agent'],
    where: {
      user_id: userId,
      expires_at: {
        gt: new Date(),
      },
    },
  });

  return {count: result.length,
         devices:result
         }
};

export const Logout = async(cookie: any) => {
    const refreshToken = cookie.refresh_token?.value

    if(!refreshToken){
        throw Errors.missingToken()
    }

    await db.query(
        `DELETE FROM sessions WHERE refresh_token = $1`,[refreshToken]
    )

    cookie.refresh_token.remove()

    return {
        message: 'logout success'
    }

}
