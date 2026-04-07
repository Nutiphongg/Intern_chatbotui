import { Elysia } from "elysia";
import { authRoutes } from "./features/auth/route";
import { swagger } from "@elysiajs/swagger";

const app = new Elysia()
.use(swagger())
.get("/", () => "welcome to auth api")
.use(authRoutes)
.listen(3000);

console.log(
  `🦊 Elysia is running at ${app.server?.hostname}:${app.server?.port}`
);
