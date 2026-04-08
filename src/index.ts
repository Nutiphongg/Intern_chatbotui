import { Elysia } from "elysia";
import { authRoutes } from "./features/auth/route";
import { swagger } from "@elysiajs/swagger";
import { cookie } from "@elysiajs/cookie";
import { cors } from "@elysiajs/cors";
import { problem,HttpError } from "./lib/problem";

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
app.onError(({ error, set, request }) => {

  if (error instanceof HttpError) {
    set.status = error.status

    return problem({
      type: error.type,
      title: error.message,
      status: error.status,
      detail: error.message,
      instance: request.url
    })
  }

  // 🔥 fallback (แก้ตรงนี้)
  set.status = 500
  return problem({
    title: 'Internal Server Error',
    status: 500,
    detail: error instanceof Error ? error.message : String(error),
    instance: request.url
  })
})
.listen(3000);


console.log(
  `🦊 Elysia is running at ${app.server?.hostname}:${app.server?.port}/swagger`
);

