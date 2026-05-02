export interface ChatMessage {
  role: string;
  content: string;
  metadata?: unknown;
  is_silent_retry?: boolean;
}

export interface ChatReply {
  role: "assistant";
  content: string;
}

export interface ChatResponsePayload {
  conversationId: string;
  reply: ChatReply;
}

export interface ChatStreamPayload {
  conversationId: string;
  stream: ReadableStream<Uint8Array>;
}

export interface ChatMetaEvent {
  conversationId: string;
}

export interface ChatTokenEvent {
  text: string;
}

export interface ChatPingEvent {
  ts: number;
}

export interface ChatDoneEvent {
  conversationId: string;
  tokenUsage: number;
}

export interface ChatErrorEvent {
  message: string;
}

export interface ChatMapEvent {
  event: "hotspots" | "disaster";
  query: unknown;
  geojsonUrl?: string;
  layerId: string;
  source: string;
  data: unknown;
  mapAction: unknown;
}
export interface ChatConversationSummary {
  id: string;
  title: string | null;
  created_at: Date;
  updated_at: Date;
  last_message_at: Date | null;
}

export interface DeleteConversationResult {
  message: string;
}
export interface PaginationMetadata {
  currentPage: number;
  pageSize: number;
  totalItems: number;
  totalPages: number;
}

//POST/chat
export interface SendChatRequest {
  conversationId?: string; // ว่างไว้ถ้าสร้างห้องใหม่ / ส่ง ID 
  message: string;         // ข้อความที่พิมพ์
  model?: string;
  feeling?: string;
  is_silent_retry?: boolean;
}
