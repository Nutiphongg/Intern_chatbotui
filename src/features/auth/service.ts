import bcrypt from 'bcrypt';
import { db } from '../setup/db';
import { RegisterBody } from './types';

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