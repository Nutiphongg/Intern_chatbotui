import { t, Static } from "elysia";
import {
  ChatConversationSummary,
  ChatDoneEvent,
  ChatMessage,
  DeleteConversationResult,
  ChatErrorEvent,
  ChatMapEvent,
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
  message: t.String({ error: "ข้อความห้ามว่างเปล่า" }),
  model: t.Optional(t.String({ minLength: 1, error: "modle is" })),
  feeling: t.Optional(t.String({default:"normal"})),
  is_silent_retry: t.Optional(t.Boolean({default: false})),
  mapSelection: t.Optional(t.Any())
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

export type ChatStreamEventName = "meta" | "token" | "ping" | "done" | "error" | "map" | "map_error" | "map_access" | "map_options" | "option_info";
export type ChatStreamEventData =
  | ChatMetaEvent
  | ChatTokenEvent
  | ChatPingEvent
  | ChatDoneEvent
  | ChatErrorEvent
  | ChatMapEvent;


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
  newContent: t.String({ minLength: 1, error: "ข้อความห้ามว่างเปล่า" }),
  is_generate: t.Boolean({ error: " is_generate is boolean" }),
});
export type EditMessageBody = Static<typeof editMessageBodySchema>;

// (Optional) Type สำหรับ Response ที่ส่งกลับไป
export type EditMessageResponse = {
  data: ChatMessage; 
};
// Schema สำหรับ Validate Params (รับ ID)
export const updateConvTitleParamsSchema = t.Object({
    conversationId: t.String()
});

// Schema สำหรับ Validate Body (รับ Title ใหม่)
export const updateConveTitleBodySchema = t.Object({
    title: t.String({ 
        minLength: 1, 
        maxLength: 255, 
    }) 
});
