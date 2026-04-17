import { t, Static } from "elysia";
import { AuthUser, DeviceInfo, JwtPayloadShape } from "./interface";

// 1. สร้างตัวดักจับ Request 
export const registerSchema = t.Object({
  email: t.String({ format: "email" }),
  username: t.String(),
  password: t.String()
});

export const loginSchema = t.Object({
  email: t.String(),
  password: t.String()
});

// 2. แปลง Schema ด้านบนให้กลายเป็น Interface 
export type RegisterBody = Static<typeof registerSchema>;
export type LoginBody = Static<typeof loginSchema>;
// COMMON TYPES
export type User = AuthUser;
export type Device = DeviceInfo;

//response type
// success wrapper (ตามที่ใช้ success())
export type ApiResponse<T> = {
  success: boolean;
  message: string;
  data: T;
};

// register
export type RegisterResponse = ApiResponse<User>;

// login
export type LoginResponse = ApiResponse<{
  user: User;
  accessToken: string;
}>;

// refresh
export type RefreshResponse = ApiResponse<{
  accessToken: string;
}>;

// devices
export type DevicesResponse = ApiResponse<{
  user: number;
  devices: Device[];
}>;

// logout
export type LogoutResponse = ApiResponse<null>;
//  JWT TYPE
export type JwtPayload = JwtPayloadShape;