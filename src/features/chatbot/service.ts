// src/features/chat/service.ts
import { prisma } from '../setup/prisma';
import { redis } from '../setup/redis';
import { Errors } from '../../lib/errors';
import { ChatRequestBody } from './types';
import { ulid } from 'ulid';


const OLLAMA_URL = process.env.OLLAMA_URL || 'http://localhost:11434/api/chat';
const MAX_HISTORY = 10; // จำแค่ 10 ประโยคล่าสุด
const REDIS_TTL = 3600; // ให้ Redis จำไว้ 1 ชั่วโมง

export const processChatMessage = async (userId: string , body : ChatRequestBody) => {
    if (!body) {
        throw Errors.badRequest('ไม่พบข้อมูลข้อความ');
    }

    let convId = body.conversationId ;
    let isNewConv = !body.conversationId;
    const { message } = body;

   
    //  1. จัดการห้องแชท (Conversations)   
    if (!convId) {
        // แชทใหม่ 
        const newConvId = ulid();
        const newConv = await prisma.conversations.create({
            data: { 
                id: newConvId,
                user_id: userId,
                title: message.substring(0, 30), // เอา 30 ตัวอักษรแรกมาทำชื่อห้อง
                last_message_at: new Date()
            } as any
        });
        convId = newConv.id;
        isNewConv = true;
    } else {
        // แชทเก่า อัปเดตเวลาที่มีการคุยล่าสุด
        const updated = await prisma.conversations.updateMany({
            where: { id: convId,user_id: userId },
            data: { last_message_at: new Date() }
        });
        if (updated.count === 0) {
            throw Errors.badRequest('ไม่พบ conversation ที่ระบุ');
        }
    }

    // เซฟข้อความ User ลง DB
    await prisma.messages.create({
        data: { 
            id: ulid(), 
            conversation_id: convId, 
            role: 'user', 
            content: message }
    });
    

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
            
            const dbHistory = await prisma.messages.findMany({
                where: { conversation_id: convId },
                orderBy: { created_at: 'desc' },
                take: MAX_HISTORY,
                select: { role: true, content: true }
            });
            messagesForLLM = dbHistory.reverse(); 

            const newMessage = {role: 'user' , content: message };
            messagesForLLM.push(newMessage);

            // ลงใน Redis
            const pipeline = redis.pipeline();
            messagesForLLM.forEach(msg => pipeline.rpush(redisKey, JSON.stringify(msg)));
            await pipeline.exec();
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
   
    
    await prisma.messages.create({
        data: { 
            id:ulid(),
            conversation_id: convId, 
            role: 'assistant', 
            content: assistantReply,
            model: "llama3", 
            response_time: responseTimeMs, 
            token_usage: ollamaData.eval_count || 0 
        }
    });
    

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

export const getChatHistory = async (userId: string, conversationId: string, limit: number = 50) => {
    // check ห้องแชทมีจริง
    const conversation = await prisma.conversations.findFirst({
        where: {id:conversationId,is_deleted: false,user_id: userId}
    });

    if(!conversation) {
        throw Errors.badRequest('ไม่พบห้องแชท หรือคุณไม่มีสิทธิ์เข้าถึงข้อมูลนี้');
    }

    const redisKey = `chat:${conversationId}`;
   // 2. ลองหาประวัติใน Redis ดูก่อน
    const cached = await redis.lrange(redisKey,0,-1);

    if(cached && cached.length > 0) {
        // ถ้าเจอใน Redis ก็ส่งกลับไปเลย
        return cached.map(msg => JSON.parse(msg));
    }
    
    const dbMessages = await prisma.messages.findMany({
        where: { conversation_id: conversationId },
        orderBy: {created_at: 'desc'},
        take: limit,
        select: {role:true, content: true }
    });

    if (dbMessages.length === 0) return [];

    const messages = dbMessages.reverse();
    const pipeline = redis.pipeline();
    messages.forEach(msg => pipeline.rpush(redisKey, JSON.stringify(msg)));
    pipeline.expire(redisKey, REDIS_TTL); // ตั้งเวลาหมดอายุใหม่
    await pipeline.exec();

    return messages;
};

export const getUserConversations = async (userId: string) => {
    return prisma.conversations.findMany({
        where: { user_id: userId,is_deleted: false, }, // ดึงมาเฉพาะห้องของ User 
        orderBy: { last_message_at: 'desc' }, // เรียงจากแชทที่เพิ่งคุยล่าสุด
        
        select: {
            id: true,
            title: true,
            created_at: true,
            updated_at: true,
            last_message_at: true
        }
    });
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
  
  return { message: "ลบข้อมูลสำเร็จ" };
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