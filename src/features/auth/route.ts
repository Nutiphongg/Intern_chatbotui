import {Elysia, t} from "elysia";
import { registerSchema,loginSchema } from "./types";
import { registerUser,loginUser,refreshUser } from "./service";

export const authRoutes = new Elysia({prefix: '/auth'})

.post('/register',async ({body,set}) => {
  try{
    const user = await registerUser(body);
    set.status = 201;
    return{
        success: true,
        message: "สมัครสมาชิกสำเร็จ",
        data: user,
    };
  }catch (error: any){
    set.status = 400;
    return{
        success: false,
        message: "ไม่สามารถสมัครสมาชิกได้",
        error: error.message
    };
  }

},{
   body: registerSchema
})

.post('/login', async ({ body, cookie: { refresh_token }, set }) => {
  try {
    // 1. รับค่าที่ Service โยนกลับมา (มี accessToken, refreshToken, user)
    const { accessToken, refreshToken, user } = await loginUser(body);

    // 2. ให้ Route เป็นคนฝัง Cookie
    refresh_token.set({
      value: refreshToken,
      httpOnly: true,       
      maxAge: 7 * 86400,    // 7 วัน
      path: "/",
    });

    set.status = 200;
    return {
      success: true,
      message: "เข้าสู่ระบบสำเร็จ",
      data: { user, accessToken } // ส่งให้ Frontend แค่ Access Token กับข้อมูล User
    };

  } catch (error: any) {
    set.status = 401;
    return {
      success: false,
      message: "เข้าสู่ระบบไม่สำเร็จ",
      error: error.message
    };
  }
}, {
  body: loginSchema
})
.post('/refresh', async ({ cookie: { refresh_token }, set }) => {
  try {
    // 1. ดึงแค่ String ยาวๆ ส่งไปให้ Service เช็ค
    const { newAccessToken, newRefreshToken } = await refreshUser(refresh_token.value as string | undefined);

    // 2. เอา Token ตัวใหม่ที่ Service สร้างให้ มาฝังลง Cookie ทับของเดิม
    refresh_token.set({
      value: newRefreshToken,
      httpOnly: true,
      maxAge: 7 * 86400, // 7 วัน
      path: "/",
      //ใช้ test ต้องลบออก
      sameSite: "none",//อนุญาตให้ส่งคุกกี้ข้ามโดเมน
      secure: true,//ยิงผ่าน api https

    });

    // 3. ตอบกลับ Frontend (ส่งไปแค่ Access Token ส่วน Refresh อยู่ใน Cookie แล้ว)
    set.status = 200;
    return {
      success: true,
      message: 'refresh สำเร็จ',
      data: {
        accessToken: newAccessToken
      }
    };

  } catch (error: any) {
    set.status = 401;
    return {
      success: false,
      message: 'refresh ไม่สำเร็จ กรุณาล็อกอินใหม่',
      error: error.message
    };
  }
})