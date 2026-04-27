import { Elysia } from "elysia";
import { authRoutes } from "./features/auth/route";
import { chatRoutes } from "./features/chatbot/route";
import { swagger } from "@elysiajs/swagger";
import { cookie } from "@elysiajs/cookie";
import { cors } from "@elysiajs/cors";
import { problem,HttpError } from "./lib/problem";
import { env } from "./lib/env";

const app = new Elysia()
//ใช้ test
.use(cors({
  origin:'http://localhost:3000',
  methods: "GET,HEAD,PUT,POST,DELETE,OPTIONS",
  credentials:true, //ส่ง cookie
  exposeHeaders: ['X-Conversation-Id', 'conversation_id']
}))
app.use(
  swagger({
    documentation: {
      components: {
        securitySchemes: {
          bearerAuth: {
            type: 'http',
            scheme: 'bearer',
            bearerFormat: 'JWT'
          }
        }
      },
      security: [
        {
          bearerAuth: []
        }
      ]
    }
  })
)
.use(cookie())
app.onError(({ error, set, request }) => {
  set.headers['Content-Type'] = 'application/json'

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

  //  fallback
  set.status = 500
  //สร้างตัวแปรเพื่อแสดงรายละเอียด error
  const isProduction = env.NODE_ENV === 'production';
  return problem({
    type: 'about:blank',
    title: 'Internal Server Error',
    status: 500,
    detail: isProduction 
        ? "ระบบเกิดข้อผิดพลาด กรุณาติดต่อผู้ดูแลระบบ" 
        : (error instanceof Error ? error.message : String(error)),
    instance: request.url
  })
})
.get("/", () => "welcome to auth api")
//.use(authRoutes)
.use(authRoutes)
//.use(chatRoutes)
.use(chatRoutes)
.listen(3000);


console.log(
  `🦊 Elysia is running at ${app.server?.hostname}:${app.server?.port}/swagger`
);

