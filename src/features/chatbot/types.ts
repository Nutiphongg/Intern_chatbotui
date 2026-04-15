import { t, Static } from "elysia";

export const chatRequestSchema = t.Object({
  // Optional เพราะถ้าเป็นการกด New Chat จะยังไม่มี ID
  conversationId: t.Optional(t.String()), 
  message: t.String({ minLength: 1, error: "ข้อความห้ามว่างเปล่า" })
});

// สร้าง Interface ไว้ให้ TypeScript รู้จัก
export type ChatRequestBody = Static<typeof chatRequestSchema>;