import {Elysia, t} from "elysia";
import { registerSchema,loginSchema } from "./types";
import { Register,Login,Refresh,getActiveDevices,Logout,createGuestUser } from "./service";
import { getUserIdFromToken,signAccessToken } from "./jwt";
import { success } from "../../lib/response";
import { Errors } from "../../lib/errors";
import { env } from "../../lib/env";
import { migrateGuestChatToUser } from "../chatbot/service";


export const authRoutes = new Elysia({prefix: '/auth'})

  .post('/register',async ({body,set}) => {
      const {guest_id, ...registerData } = body;
      const user = await Register(registerData);

      let redirecConversationId = null;

      if(guest_id) {
        redirecConversationId = await migrateGuestChatToUser(user.id, guest_id);
      }
      set.status = 201;
      return success({user,redirecConversationId:redirecConversationId},"สมัครสมาชิกสำเร็จ")

  },{
    body: registerSchema
  })

  .post('/sessions', async ({ body, cookie: { refresh_token },headers,request, set }) => {
      const {guest_id, ...loginData } = body;
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
        //ใช้ test ต้องลบออก
        sameSite: "none",//อนุญาตให้ส่งคุกกี้ข้ามโดเมน
        secure: true,//ยิงผ่าน api https
      });

      let redirectConversationId = null;

      if(guest_id) {
        redirectConversationId = await migrateGuestChatToUser(user.id, guest_id);
      }

      
      return success({user,accessToken,redirectConversationId}, "เข้าสู่ระบบสำเร็จ")
  }, {
    body: loginSchema
  })
  .put('/sessions', async ({ cookie: { refresh_token },headers,request, set }) => {

      const userAgent = headers['user-agent'] || 'unknown';
    //ngrok proxy แก้ไขถ้าใช้ตัวอื่น
      const ip = request.headers.get('x-forwarded-for') || 'unknown'
    
      // 1. ดึงแค่ String ยาวๆ ส่งไปให้ Service เช็ค
      const { newAccessToken, newRefreshToken } = await Refresh(refresh_token.value as string | undefined,userAgent,ip);

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

  .get('/sessions', async({cookie: {refresh_token} }) => {
    
      if (!refresh_token.value) {
        throw Errors.missingToken();
      }
      // 2. ใช้เครื่องมือถอดรหัส เพื่อแกะเอา userId ออกมา
      const userId = await getUserIdFromToken(refresh_token.value as string);
      // 3. ส่ง userId ไปให้ Service นับจำนวนจาก Database
      const devices = await getActiveDevices(userId);

      return success(devices, "ดึงข้อมูลสำเร็จ") 
  })
  .delete('/sessions', async ({ cookie: { refresh_token }, set }) => {
    
      // 1. ส่งแค่ string ไปให้ Service ลบข้อมูลใน Database
      const result = await Logout(refresh_token.value as string | undefined);

      // 2.  สั่งเคลียร์ Cookie ที่ฝั่ง Browser 
      refresh_token.set({
          value: "",          
          httpOnly: true,
          path: "/",
          maxAge: 0,          
          sameSite: "none",   
          secure: true,       
      });

      return success(null, result.message);
  })
  
  .post('/guests', async ({  set }) => {
      // 1. สร้าง Guest ID ใน Redis
      const guest = await createGuestUser();
      
      // 2. เอา Guest ID มา Sign เป็น JWT
      const token = signAccessToken({
        userId: guest.id,
        role: 'guest'
        
      });

      set.status = 201;
      return success(
        { 
          accessToken: token, 
          guestId: guest.id 
        }, 
        'สร้าง Guest Session สำเร็จ'
      );
  })
    