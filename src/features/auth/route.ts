import {Elysia, t} from "elysia";
import { registerSchema,loginSchema } from "./types";
import { Register,Login,Refresh,getActiveDevices,Logout } from "./service";
import { getUserIdFromToken } from "./jwt";
import { success } from "../../lib/response";
import { Errors } from "../../lib/errors";

export const authRoutes = new Elysia({prefix: '/auth'})

.post('/register',async ({body,set}) => {
    const user = await Register(body);
    set.status = 201;
    return success(user,"สมัครสมาชิกสำเร็จ")

},{
   body: registerSchema
})

.post('/login', async ({ body, cookie: { refresh_token },headers,request, set }) => {
  
     //ดึง  user-agent
    const userAgent = headers['user-agent'] || 'unknown';
    //ngrok proxy แก้ไขถ้าใช้ตัวอื่น
    const ip = request.headers.get('x-forwarded-for') || 'unknown'
    // 1. รับค่าที่ Service โยนกลับมา (มี accessToken, refreshToken, user)
    const { accessToken, refreshToken, user } = await Login(body,userAgent,ip);


    // 2. ให้ Route เป็นคนฝัง Cookie
    refresh_token.set({
      value: refreshToken,
      httpOnly: true,       
      maxAge: 7 * 86400,    // 7 วัน
      path: "/",
    });

    
    return success({user,accessToken}, "เข้าสู่ระบบสำเร็จ")
}, {
  body: loginSchema
})
.post('/refresh', async ({ cookie: { refresh_token }, set }) => {
  
    // 1. ดึงแค่ String ยาวๆ ส่งไปให้ Service เช็ค
    const { newAccessToken, newRefreshToken } = await Refresh(refresh_token.value as string | undefined);

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
     return success(
    { accessToken: newAccessToken },
    "refresh สำเร็จ"
  )
})

.get('/sessions/count', async({cookie: {refresh_token} }) => {
  
    if (!refresh_token.value) {
      throw new Error("ไม่พบ Token ");
    }
    // 2. ใช้เครื่องมือถอดรหัส เพื่อแกะเอา userId ออกมา
    const userId = await getUserIdFromToken(refresh_token.value as string);
    // 3. ส่ง userId ไปให้ Service นับจำนวนจาก Database
    const devices = await getActiveDevices(userId);

    return success(devices, "ดึงข้อมูลสำเร็จ") 
})
.post('/logout',async ({cookie, set}) => {
  
    const result = await Logout(cookie)

    return success(null, result.message)
})

