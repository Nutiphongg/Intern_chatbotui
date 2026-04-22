// src/features/chat/service.ts
import { prisma } from '../setup/prisma';
import { redis } from '../setup/redis';
import { Errors } from '../../lib/errors';
import { ChatRequestBody } from './types';
import { ulid } from 'ulid';
import { env } from '../../lib/env';


const OLLAMA_URL = env.OLLAMA_URL;
const MAX_HISTORY = 10; // จำแค่ 10 ประโยคล่าสุด
const REDIS_TTL = 3600; // ให้ Redis จำไว้ 1 ชั่วโมง

export const processChatMessageStream = (userId: string,role:string, body: ChatRequestBody) => {
    if (!body || !body.message?.trim()) {
        throw Errors.badRequest('no message data found');
    }

    const isGuest = role === 'guest';
    const message = body.message.trim();
    const isNewConv = !body.conversationId;
    const convId = body.conversationId || ulid();
    const redisKey = isGuest? `guest_chat:${convId}`:`chat:${convId}`;
    const encoder = new TextEncoder();
    
    const writeSse = (controller: ReadableStreamDefaultController<Uint8Array>, event: string, data: unknown) => {
        controller.enqueue(encoder.encode(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`));
    };

    let heartbeat: Timer | null = null;
    let isClosed = false;

    // เปิด stream ทันที แล้วค่อยทำงานหนักใน background
    const stream = new ReadableStream<Uint8Array>({
        start(controller) {
            const closeSafely = () => {
                if (isClosed) return;
                isClosed = true;
                if (heartbeat) {
                    clearInterval(heartbeat);
                    heartbeat = null;
                }
                controller.close();
            };
            //แจ้ง metadata กลับทันที เพื่อให้ frontend เข้าสู่โหมด stream เร็วที่สุด
            writeSse(controller, 'meta', { conversationId: convId });
            const run = async () => {
                try {
                     
                    // ส่ง heartbeat กัน proxy/ngrok ตัด connection ตอนโมเดลยังไม่ตอบ token แรก
                    heartbeat = setInterval(() => {
                        if (!isClosed) {
                            writeSse(controller, 'ping', { ts: Date.now() });
                        }
                    }, 10000);
                    if (!isGuest) {
                    // จัดการ/ตรวจสอบห้องแชทก่อนบันทึกข้อความ
                        if (isNewConv) {
                            await prisma.conversations.create({
                                data: {
                                    id: convId,
                                    user_id: userId,
                                    title: message.substring(0, 30),
                                    last_message_at: new Date()
                                } as any
                            });
                        } else {
                            const updated = await prisma.conversations.updateMany({
                                where: { id: convId, user_id: userId },
                                data: { last_message_at: new Date() }
                            });
                            if (updated.count === 0) {
                                writeSse(controller, 'error', { message: 'conversation_not_found' });
                                closeSafely();
                                return;
                            }
                        }
                        // บันทึกข้อความ user ลงฐานข้อมูล
                        await prisma.messages.create({
                            data: {
                                id: ulid(),
                                conversation_id: convId,
                                role: 'user',
                                content: message
                            }
                        });
                    }
                    // เตรียม history ให้ LLM: ใช้ Redis ก่อน ถ้าไม่มีค่อย fallback ไป DB
                    let messagesForLLM: Array<{ role: string, content: string }> = [];
                    if (isNewConv) {
                        messagesForLLM = [{ role: 'user', content: message }];
                        await redis.rpush(redisKey, JSON.stringify(messagesForLLM[0]));
                    } else {
                        const cached = await redis.lrange(redisKey, 0, -1);
                        if (cached.length > 0) {
                            messagesForLLM = cached.map(msg => JSON.parse(msg));
                            const newMessage = { role: 'user', content: message };
                            messagesForLLM.push(newMessage);
                            await redis.rpush(redisKey, JSON.stringify(newMessage));
                        } else {
                            if (!isGuest) {
                                const dbHistory = await prisma.messages.findMany({
                                    where: { conversation_id: convId },
                                    orderBy: { created_at: 'desc' },
                                    take: MAX_HISTORY,
                                    select: { role: true, content: true }
                                });

                                messagesForLLM = dbHistory.reverse();
                                const newMessage = { role: 'user', content: message };
                                messagesForLLM.push(newMessage);

                                const pipeline = redis.pipeline();
                                messagesForLLM.forEach(msg => pipeline.rpush(redisKey, JSON.stringify(msg)));
                                await pipeline.exec();
                            
                            } else {
                                const newMessage = { role: 'user', content: message, created_at: new Date() };
                            messagesForLLM.push(newMessage);
                            await redis.rpush(redisKey, JSON.stringify(newMessage));
                            }
                        }
                    }

                    // เรียก Ollama แบบ stream เพื่อรับ token ทีละส่วน
                    const startTime = Date.now();
                    const ollamaResponse = await fetch(OLLAMA_URL, {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({
                            model: 'llama3',
                            messages: messagesForLLM,
                            stream: true
                        })
                    });

                    if (!ollamaResponse.ok || !ollamaResponse.body) {
                        writeSse(controller, 'error', { message: 'ollama_request_failed' });
                        closeSafely();
                        return;
                    }

                    // อ่าน stream จาก Ollama แล้วแปลงเป็น SSE token ส่งต่อให้ frontend
                    let assistantReply = '';
                    let tokenUsage = 0;
                    const decoder = new TextDecoder();
                    const reader = ollamaResponse.body.getReader();
                    let buffer = '';

                    while (!isClosed) {
                        const { done, value } = await reader.read();

                        if (done) {
                            const lastLine = buffer.trim();
                            if (lastLine) {
                                const chunk = JSON.parse(lastLine);
                                const textPart = chunk?.message?.content || '';
                                if (textPart) {
                                    assistantReply += textPart;
                                    writeSse(controller, 'token', { text: textPart });
                                }
                                if (chunk?.done && typeof chunk?.eval_count === 'number') {
                                    tokenUsage = chunk.eval_count;
                                }
                            }
                            break;
                        }

                        buffer += decoder.decode(value, { stream: true });
                        const lines = buffer.split('\n');
                        buffer = lines.pop() || '';

                        for (const line of lines) {
                            const trimmed = line.trim();
                            if (!trimmed) continue;

                            const chunk = JSON.parse(trimmed);
                            const textPart = chunk?.message?.content || '';
                            if (textPart) {
                                assistantReply += textPart;
                                writeSse(controller, 'token', { text: textPart });
                            }
                            if (chunk?.done && typeof chunk?.eval_count === 'number') {
                                tokenUsage = chunk.eval_count;
                            }
                        }
                    }

                    // สรุปผลลัพธ์แล้วบันทึกฝั่ง assistant ลง DB/Redis
                    const responseTimeMs = Date.now() - startTime;
                    if (assistantReply) {
                        const botMessage = {role: 'assistant', content: assistantReply, created_at: new Date() };
                        if(! isGuest) {
                            await prisma.messages.create({
                                data: {
                                    id: ulid(),
                                    conversation_id: convId,
                                    role: 'assistant',
                                    content: assistantReply,
                                    model: 'llama3',
                                    response_time: responseTimeMs,
                                    token_usage: tokenUsage
                                }
                           }) 
                        };

                        await redis.rpush(redisKey, JSON.stringify(botMessage));
                        await redis.ltrim(redisKey, -MAX_HISTORY, -1);
                        await redis.expire(redisKey, REDIS_TTL);
                    }

                    // แจ้งจบ stream ให้ frontend ปิด loading/state
                    writeSse(controller, 'done', { tokenUsage });
                    closeSafely();
                } catch (error) {
                    console.error('LLM Stream Error:', error);
                    // ส่ง error event แทนการปล่อย connection ตายเงียบ
                    writeSse(controller, 'error', { message: 'stream_failed' });
                    closeSafely();
                }
            };

            void run();
        },
        cancel() {
            isClosed = true;
            if (heartbeat) {
                clearInterval(heartbeat);
                heartbeat = null;
            }
        }
    });

    return { conversationId: convId, stream };
};

export const getChatHistory = async (userId: string, role: string, conversationId: string, page: number = 1, limit: number = 5) => {
    const isGuest = role === 'guest';
    const skip = (page - 1) * limit;

    if (isGuest) {
    const redisKey = `guest_chat:${conversationId}`;
    const cached = await redis.lrange(redisKey, 0, -1);

    if (cached.length === 0) {
        return { data: [], pagination: { currentPage: page, pageSize: limit, totalItems: 0, totalPages: 0 } };
    }

   
    const totalCount = cached.length;
    const start = totalCount - (page * limit);
    const end = totalCount -((page - 1) * limit);

    const safeStart = Math.max(0,start);
    const safeEnd = Math.max(0, end);
    
    const pageItems = cached.slice(safeStart, safeEnd);
    
    
    const messages = pageItems.map((msg,index) => {
        const parsed = JSON.parse(msg);
        return {
            id:`guest_msg_${safeStart + index}`,
            role: parsed.role,
            content: parsed.content,
            created_at: parsed.created_at || null
        };
    });
       

    return {
        data: messages,
        pagination: {
            currentPage: page,
            pageSize: limit,
            totalItems: totalCount,
            totalPages: Math.ceil(totalCount / limit)
        }
    };
}

    // Registered User
    const conversation = await prisma.conversations.findFirst({
        where: { id: conversationId, is_deleted: false, user_id: userId }
    });

    if (!conversation) {
        throw Errors.badRequest('ไม่พบห้องแชท หรือคุณไม่มีสิทธิ์เข้าถึงข้อมูลนี้');
    }

    const [dbMessages, totalCount] = await Promise.all([
        prisma.messages.findMany({
            where: { conversation_id: conversationId },
            orderBy: { created_at: 'desc' }, // 
            skip: skip,
            take: limit,
            select: { id: true, role: true, content: true, created_at: true }
        }),
        prisma.messages.count({
            where: { conversation_id: conversationId }
        })
    ]);

    // reverse ให้ภายใน page เรียง เก่า→ใหม่ (บนลงล่าง)
    const messages = dbMessages.reverse();

    return {
        data: messages,
        pagination: {
            currentPage: page,
            pageSize: limit,
            totalItems: totalCount,
            totalPages: Math.ceil(totalCount / limit)
        }
    };
};

export const getUserConversations = async (userId: string, role:string, page: number = 1, limit: number = 10) => {
    const isGuest = role === 'guest';

    if (isGuest) { 
        // ฝั่ง Guest: จำลองข้อมูลว่ามี 1 ห้องแชทที่เป็นเซสชันปัจจุบัน
        const mockGuestConversation = {
            id: userId, // หรือจะรับค่ามาก็ได้
            title: 'Guest Session',
            created_at: new Date(),
            updated_at: new Date(),
            last_message_at: new Date()
        };

        return {
            data: [mockGuestConversation],
            pagination: {
                currentPage: 1,
                pageSize: limit,
                totalItems: 1,
                totalPages: 1
            }
        };
    }
    const skip = (page - 1) * limit;

    // ใช้ Promise.all ดึงข้อมูลและนับจำนวนไปพร้อมๆ กัน
    const [items, totalCount] = await Promise.all([
        prisma.conversations.findMany({
            where: { user_id: userId, is_deleted: false },
            orderBy: { last_message_at: 'desc' },
            skip: skip, 
            take: limit, 
            select: {
                id: true,
                title: true,
                created_at: true,
                updated_at: true,
                last_message_at: true
            }
        }),
        prisma.conversations.count({
            where: { user_id: userId, is_deleted: false }
        })
    ]);

    // เปลี่ยนการ Return เป็นรูปแบบใหม่
    return {
        data: items,
        pagination: {
            currentPage: page,
            pageSize: limit,
            totalItems: totalCount,
            totalPages: Math.ceil(totalCount / limit)
        }
    };
};

// ฟังก์ชันลบห้องแชท (Soft Delete) 
export const deleteConversation = async (userId: string, conversationId: string) => {
  // 1. Soft Delete ใน PostgreSQL
  await prisma.conversations.updateMany({
    where: { id: conversationId, user_id: userId },
    data: { is_deleted: true }
  });

  // 2. ลบ Cache ใน Redis )
  await redis.del(`chat:history:${conversationId}`);
  
  return { message: "delete conversation" };
};

// ฟังก์ชันแก้ไขข้อความ
export const editMessage = async (userId: string, messageId: string, newContent: string) => {

      const oldMessage = await prisma.messages.findFirst({
    where: { id: messageId, conversations: { user_id: userId } }
  });

  if (!oldMessage) throw new Error("Message not found");
  if (oldMessage.role !== 'user') throw new Error("Message not Edits");
  //ต้องเช็คเฉพาะข้อความที่ "ยังไม่ถูกลบ"
  const laterUserMessage = await prisma.messages.findFirst({
    where:{
        conversation_id: oldMessage.conversation_id,
        role: 'user',
        deleted_at: null // 
    },
    orderBy:{created_at:'desc'}
  });

  if (laterUserMessage?.id !== messageId){
    throw new Error("Edit laterMessage");
  }
  await prisma.messages.updateMany({
    where: {
        conversation_id: oldMessage.conversation_id,
        created_at: {gt: oldMessage.created_at! },
        deleted_at: null
    },
    data:{
        deleted_at: new Date()
    }
  });
  // 2. อัปเดตใน PostgreSQL
  const updatedMessage = await prisma.messages.update({
    where: { id: messageId },
    data: {
      content: newContent,
      metadata: {
        ...(typeof oldMessage.metadata === 'object' && oldMessage.metadata !== null ? oldMessage.metadata : {}),
        is_edited: true,
        last_edited_at: new Date(),
        original_content: oldMessage.content // เก็บประวัติเก่าไว้
      }
    }
  });

  // 3. ลบ Cache ใน Redis ของห้องนั้นทิ้ง 
  await redis.del(`chat:history:${oldMessage.conversation_id}`);

  return updatedMessage;
      

};
