import { t, Static } from "elysia";
import {
  ChatConversationSummary,
  ChatDoneEvent,
  ChatMessage,
  DeleteConversationResult,
  ChatErrorEvent,
  ChatMetaEvent,
  ChatPingEvent,
  ChatResponsePayload,
  ChatStreamPayload,
  ChatTokenEvent,
  PaginationMetadata
} from "./interface";

export const chatRequestSchema = t.Object({
  // Optional เพราะถ้าเป็นการกด New Chat จะยังไม่มี ID
  conversationId: t.Optional(t.String()), 
  message: t.String({ minLength: 1, error: "ข้อความห้ามว่างเปล่า" })
}, {
  // ปิดรับ field อื่นที่ไม่กำหนดใน schema  userId
  additionalProperties: false
});

// สร้าง Interface ไว้ให้ TypeScript รู้จัก
export type ChatRequestBody = Static<typeof chatRequestSchema>;

export const conversationParamsSchema = t.Object({
  conversationId: t.String()
});
export type ConversationParams = Static<typeof conversationParamsSchema>;

export const chatHistoryQuerySchema = t.Object({
  page: t.Optional(t.Numeric()),
  limit: t.Optional(t.Numeric())
});
export type ChatHistoryQuery = Static<typeof chatHistoryQuerySchema>;

export type ChatResponse = ChatResponsePayload;
export type ChatStreamResponse = ChatStreamPayload;

export type ChatStreamEventName = "meta" | "token" | "ping" | "done" | "error";
export type ChatStreamEventData =
  | ChatMetaEvent
  | ChatTokenEvent
  | ChatPingEvent
  | ChatDoneEvent
  | ChatErrorEvent;


export type DeleteConversationResponse = DeleteConversationResult;


export type ChatHistoryResponse = {
  data: ChatMessage[];
  pagination: PaginationMetadata;
};

export type UserConversationsResponse = {
  data: ChatConversationSummary[];
  pagination: PaginationMetadata;
};

export const editMessageParamsSchema = t.Object({
  messageId: t.String({ error: "ต้องระบุ messageId ใน URL" })
});
export type EditMessageParams = Static<typeof editMessageParamsSchema>;

export const editMessageBodySchema = t.Object({
  newContent: t.String({ minLength: 1, error: "ข้อความห้ามว่างเปล่า" })
});
export type EditMessageBody = Static<typeof editMessageBodySchema>;

// (Optional) Type สำหรับ Response ที่ส่งกลับไป
export type EditMessageResponse = {
  message: string;
  data: ChatMessage; 
};