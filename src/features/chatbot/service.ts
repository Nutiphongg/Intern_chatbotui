// src/features/chat/service.ts
import { prisma } from '../setup/prisma';
import { redis } from '../setup/redis';
import { Errors } from '../../lib/errors';
import { ChatRequestBody } from './types';

const OLLAMA_URL = process.env.OLLAMA_URL || 'http://localhost:11434/api/chat';
const MAX_HISTORY = 10; // จำแค่ 10 ประโยคล่าสุด
const REDIS_TTL = 3600; // ให้ Redis จำไว้ 1 ชั่วโมง

export const processChatMessage = async (
    userIdOrBody: string | ChatRequestBody,
    maybeBody?: ChatRequestBody
) => {
    // รองรับทั้ง signature ใหม่ (body) และเก่า (userId, body)
    const body = typeof userIdOrBody === 'string' ? maybeBody : userIdOrBody;
    if (!body) {
        throw Errors.badRequest('ไม่พบข้อมูลข้อความ');
    }

    //  ปรับให้สร้าง Mock ID สำหรับทดสอบ Redis ถ้ายังไม่มี convId
    let convId = body.conversationId || `temp-session-${Date.now()}`;
    let isNewConv = !body.conversationId;
    const { message } = body;

   
    //  1. จัดการห้องแชท (Conversations) - ปิด DB ไว้ชั่วคราว
  
    /*
    if (!convId) {
        // แชทใหม่ 
        const newConv = await prisma.conversations.create({
            // temporary: อนุญาต guest conversation โดยยังไม่ผูก users relation
            data: { 
                title: message.substring(0, 30), // เอา 30 ตัวอักษรแรกมาทำชื่อห้อง
                last_message_at: new Date()
            } as any
        });
        convId = newConv.id;
        isNewConv = true;
    } else {
        // แชทเก่า อัปเดตเวลาที่มีการคุยล่าสุด
        const updated = await prisma.conversations.updateMany({
            where: { id: convId },
            data: { last_message_at: new Date() }
        });
        if (updated.count === 0) {
            throw Errors.badRequest('ไม่พบ conversation ที่ระบุ');
        }
    }

    // เซฟข้อความ User ลง DB
    await prisma.messages.create({
        data: { conversation_id: convId, role: 'user', content: message }
    });
    */

    // 2. ดึงประวัติจาก Redis (ทำงานปกติ)
  
    const redisKey = `chat:${convId}`;
    let messagesForLLM: Array<{ role: string, content: string }> = [];

    if (isNewConv) {
        messagesForLLM = [{ role: 'user', content: message }];
        await redis.rpush(redisKey, JSON.stringify(messagesForLLM[0]));
    } else {
        const cached = await redis.lrange(redisKey, 0, -1);
        if (cached.length > 0) {
            // เจอใน Redis เอามาใช้เลย!
            messagesForLLM = cached.map(msg => JSON.parse(msg));
            const newMessage = { role: 'user', content: message };
            messagesForLLM.push(newMessage);
            await redis.rpush(redisKey, JSON.stringify(newMessage));
        } else {
            // หมดอายุใน Redis ไปแล้ว ปิดการหาจาก DB ไว้ชั่วคราว
            /*
            const dbHistory = await prisma.messages.findMany({
                where: { conversation_id: convId },
                orderBy: { created_at: 'desc' },
                take: MAX_HISTORY,
                select: { role: true, content: true }
            });
            messagesForLLM = dbHistory.reverse(); 

            // ลงใน Redis
            const pipeline = redis.pipeline();
            messagesForLLM.forEach(msg => pipeline.rpush(redisKey, JSON.stringify(msg)));
            await pipeline.exec();
            */

            // 💡 สร้างเป็นข้อความใหม่ไปเลยสำหรับการทดสอบ กรณี Redis หาย
            messagesForLLM = [{ role: 'user', content: message }];
            await redis.rpush(redisKey, JSON.stringify(messagesForLLM[0]));
        }
    }

    //  3. ยิง API หา Ollama (ทำงานปกติ)

    let assistantReply = "";
    let ollamaData: any = {};
    const startTime = Date.now();

    try {
        const response = await fetch(OLLAMA_URL, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                model: "llama3", // เช็คด้วยนะครับว่าใน Ollama โหลด llama3 ไว้แล้ว
                messages: messagesForLLM, 
                stream: false 
            })
        });

        if (!response.ok) throw new Error("Ollama API Error");
        ollamaData = await response.json();
        assistantReply = ollamaData?.message?.content || ''; 
        if (!assistantReply) throw new Error('Ollama returned empty response');

    } catch (error) {
        console.error("LLM Error:", error);
        throw Errors.internalServerError();
    }

    const responseTimeMs = Date.now() - startTime;

    
    //  4. บันทึกคำตอบกลับลงระบบ - ปิด DB ไว้ชั่วคราว
   
    /*
    await prisma.messages.create({
        data: { 
            conversation_id: convId, 
            role: 'assistant', 
            content: assistantReply,
            model: "llama3", 
            response_time: responseTimeMs, 
            token_usage: ollamaData.eval_count || 0 
        }
    });
    */

    //  แต่ยังคงเซฟประวัติลง Redis อยู่
    await redis.rpush(redisKey, JSON.stringify({ role: 'assistant', content: assistantReply }));
    await redis.ltrim(redisKey, -MAX_HISTORY, -1);
    await redis.expire(redisKey, REDIS_TTL);

    // 5. ส่งคืน Frontend
 
    return {
        conversationId: convId,
        reply: {
            role: "assistant",
            content: assistantReply
        }
    };
};

// ปล่อยฟังก์ชัน getChatHistory และ getUserConversations ไว้เหมือนเดิม
// เพราะไม่ได้ถูกเรียกตอนส่งข้อความ (แต่อาจจะพังถ้ากดดูประวัติจาก Frontend ตอนนี้ เพราะเรายังไม่ได้เซฟลง DB)
export const getChatHistory = async (
    userIdOrConversationId: string,
    conversationIdOrLimit?: string | number,
    maybeLimit = 50
) => {
    // 1. จัดการเรื่องการรับค่า Parameter (โค้ดเดิมของคุณ)
    const conversationId =
        typeof conversationIdOrLimit === 'string'
            ? conversationIdOrLimit
            : userIdOrConversationId;

    // 2. สร้าง Key ให้ตรงกับที่เราเซฟไว้ใน Redis
    const redisKey = `chat:${conversationId}`;

    // 3. ดึงข้อมูลประวัติทั้งหมดจาก Redis (จาก Index 0 ถึงตัวสุดท้าย -1)
    const cached = await redis.lrange(redisKey, 0, -1);

    // 4. ถ้าไม่มีข้อมูลใน Redis เลย ให้ส่ง Array ว่างกลับไป
    if (!cached || cached.length === 0) {
        return [];
    }

    // 5. แปลงข้อมูลที่ได้จาก Redis (ซึ่งเป็น String) กลับมาเป็น JSON Object
    const messages = cached.map(msg => JSON.parse(msg));

    return messages;
};

export const getUserConversations = async (userId: string) => {
    return prisma.conversations.findMany({
        where: { user_id: userId },
        orderBy: { last_message_at: 'desc' },
        select: {
            id: true,
            title: true,
            created_at: true,
            updated_at: true,
            last_message_at: true
        }
    });
};