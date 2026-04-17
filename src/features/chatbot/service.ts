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

export const processChatMessageStream = (userId: string, body: ChatRequestBody) => {
    if (!body || !body.message?.trim()) {
        throw Errors.badRequest('ไม่พบข้อมูลข้อความ');
    }

    const message = body.message.trim();
    const isNewConv = !body.conversationId;
    const convId = body.conversationId || ulid();
    const redisKey = `chat:${convId}`;
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

            const run = async () => {
                try {
                    // แจ้ง metadata กลับทันที เพื่อให้ frontend เข้าสู่โหมด stream เร็วที่สุด
                    writeSse(controller, 'meta', { conversationId: convId });

                    // ส่ง heartbeat กัน proxy/ngrok ตัด connection ตอนโมเดลยังไม่ตอบ token แรก
                    heartbeat = setInterval(() => {
                        if (!isClosed) {
                            writeSse(controller, 'ping', { ts: Date.now() });
                        }
                    }, 10000);

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
                        });

                        await redis.rpush(redisKey, JSON.stringify({ role: 'assistant', content: assistantReply }));
                        await redis.ltrim(redisKey, -MAX_HISTORY, -1);
                        await redis.expire(redisKey, REDIS_TTL);
                    }

                    // แจ้งจบ stream ให้ frontend ปิด loading/state
                    writeSse(controller, 'done', { conversationId: convId, tokenUsage });
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

export const getChatHistory = async (userId: string, conversationId: string, page: number = 1, limit: number = 20) => {
    // 1. Check ห้องแชทมีจริงไหม
    const conversation = await prisma.conversations.findFirst({
        where: { id: conversationId, is_deleted: false, user_id: userId }
    });

    if (!conversation) {
        throw Errors.badRequest('ไม่พบห้องแชท หรือคุณไม่มีสิทธิ์เข้าถึงข้อมูลนี้');
    }

    const skip = (page - 1) * limit;
    const redisKey = `chat:${conversationId}`;

    // 2.   อ่านแคชจาก Redis เฉพาะตอนที่ขอ "หน้าแรก" 
    if (page === 1) {
        const cached = await redis.lrange(redisKey, 0, -1);
        if (cached && cached.length > 0) {
            const totalCount = await prisma.messages.count({ where: { conversation_id: conversationId } });
            return {
                data: cached.map(msg => JSON.parse(msg)),
                pagination: {
                    currentPage: 1,
                    pageSize: limit,
                    totalItems: totalCount,
                    totalPages: Math.ceil(totalCount / limit)
                }
            };
        }
    }

    // 3. ถ้าไม่มีใน Redis หรือเป็นการขอหน้า 2, 3, 4... ให้ดึงจาก DB
    const [dbMessages, totalCount] = await Promise.all([
        prisma.messages.findMany({
            where: { conversation_id: conversationId },
            orderBy: { created_at: 'desc' }, // ดึงอันใหม่ล่าสุดมาก่อน
            skip: skip,
            take: limit,
            select: { role: true, content: true } // แนะนำให้ select id และ created_at เผื่อ Frontend ด้วยนะครับ
        }),
        prisma.messages.count({
            where: { conversation_id: conversationId }
        })
    ]);

    // กลับด้าน Array เพื่อให้ข้อความเรียงจาก บนลงล่าง (เก่าไปใหม่) เหมือนแชทปกติ
    const messages = dbMessages.reverse();

    // 4. เอาลง Redis "เฉพาะตอนที่ดึงหน้าแรกเท่านั้น" 
    if (page === 1 && messages.length > 0) {
        const pipeline = redis.pipeline();
        messages.forEach(msg => pipeline.rpush(redisKey, JSON.stringify(msg)));
        // สมมติว่าดึง REDIS_TTL มาจาก env นะครับ
        pipeline.expire(redisKey, Number(REDIS_TTL) || 3600); 
        await pipeline.exec();
    }

    //  5. ส่ง Response กลับไปให้ตรงสเปค
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
// src/features/chat/service.ts

export const getUserConversations = async (userId: string, page: number = 1, limit: number = 10) => {
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
  // 1. ดึงข้อความเดิมมาดูเพื่อเอาข้อมูลเข้า metadata
  const oldMessage = await prisma.messages.findFirst({
    where: { id: messageId, conversations: { user_id: userId } }
  });

  if (!oldMessage) throw new Error("Message not found");

  // 2. อัปเดตใน PostgreSQL
  const updatedMessage = await prisma.messages.update({
    where: { id: messageId },
    data: {
      content: newContent,
      metadata: {
        ...(oldMessage.metadata as object || {}),
        is_edited: true,
        last_edited_at: new Date(),
        // เก็บประวัติข้อความเก่าไว้เผื่อแอดมินตรวจสอบ
        original_content: oldMessage.content 
      }
    }
  });

  // 3. ลบ Cache ใน Redis ของห้องนั้นทิ้ง 
  await redis.del(`chat:history:${oldMessage.conversation_id}`);

  return updatedMessage;
};