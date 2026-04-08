import { Elysia } from "elysia";
import { authRoutes } from "./features/auth/route";
import { swagger } from "@elysiajs/swagger";
import { cookie } from "@elysiajs/cookie";
import { cors } from "@elysiajs/cors";

const app = new Elysia()
//ใช้ test
.use(cors({
  origin:'http://localhost:3000',
  credentials:true //ส่ง cookie
}))
.use(swagger())
.use(cookie())
.get("/", () => "welcome to auth api")

.use(authRoutes)
.listen(3000);

console.log(
  `🦊 Elysia is running at ${app.server?.hostname}:${app.server?.port}/swagger`
);

