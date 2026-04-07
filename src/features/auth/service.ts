import bcrypt from 'bcrypt';
import { db } from '../setup/db';
import { RegisterBody ,LoginBody} from './types';
import { signAccessToken,signRefreshToken,verifyRefreshToken } from './jwt';

export const registerUser = async (data: RegisterBody) => {
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

export const loginUser = async (body: LoginBody) => {
    const { email,password } = body;

    const result = await db.query(
        'SELECT * FROM users WHERE email = $1',
        [email]
    )

    const user = result.rows[0]

    if(!user){
        throw new Error('ข้อมูลไม่ถูกต้อง')
    }

    const ok = await bcrypt.compare(password,user.password_hash)

    if(!ok){
        throw new Error('ข้อมูลไม่ถูกต้อง')
    }
    // สร้าง token
    const accessToken = signAccessToken({userId: user.id})
    const refreshToken = signRefreshToken({userId: user.id})

    // บันทึก token
    await db.query(
         `INSERT INTO sessions (user_id, refresh_token, expires_at)
         VALUES ($1, $2, NOW() + INTERVAL '7 days')`,
         [user.id, refreshToken]
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
export const refreshUser = async (refreshToken: string | undefined) => { 
    if(!refreshToken) {
        throw new Error('ไม่มี Refresh Token ในระบบ');
    }

    // ตรวจสอบ JWT
    let payload: any;
    try {
        payload = verifyRefreshToken(refreshToken);
    } catch (error:any){
        // ลบ token ออกจาก db
        await db.query(`DELETE FROM sessions WHERE refresh_token = $1`, [refreshToken]);
        // ลบเสร็จ กลับไปที่ route
        throw new Error('Refresh Token ไม่ถูกต้องหรือหมดอายุ กรุณาล็อกอินใหม่');
    }

    // เช็คใน db
    const session = await db.query(
        `SELECT * FROM sessions WHERE refresh_token = $1`, [refreshToken]
    );

    if(session.rows.length === 0){
        throw new Error('ไม่พบ Session หรือ Session ถูกยกเลิกไปแล้ว');
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