import { t, Static } from "elysia";

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