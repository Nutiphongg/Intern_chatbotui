import {Elysia, t} from "elysia";
import { registerSchema } from "./types";
import { registerUser } from "./service";

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
});


