// src/features/chat/route.ts
import { Elysia, t } from 'elysia';
import { chatHistoryQuerySchema, chatRequestSchema, conversationParamsSchema } from './types';
import { processChatMessageStream, getChatHistory, getUserConversations,deleteConversation,editMessage } from './service';
import { authPlugin } from '../../plugins/plugin';
import { redis } from '../setup/redis';

export const chatRoutes = new Elysia({ prefix: '/chat' })
    .use(authPlugin)
    // POST /chatbot
    .post('/', async ({ body, user })  => {
        const result = processChatMessageStream(user.id, body);
        return new Response(result.stream, {
            headers: {
                'Content-Type': 'text/event-stream; charset=utf-8',
                'Cache-Control': 'no-cache',
                'X-Conversation-Id': result.conversationId
            }
        });
    }, {
        body: chatRequestSchema
    })
   // GET /chat/histories
    .get('/histories', async ({ query, user }) => {
        const rawPage = query.page || 1;
        const page = Math.max(1, rawPage); // บังคับว่าหน้าต้อง >= 1 เสมอ
        
        const rawLimit = query.limit || 10;
        const limit = Math.max(1, rawLimit); // บังคับว่าต้องดึงอย่างน้อย 1 รายการ
        
        // เรียกใช้งาน service
        return await getUserConversations(user.id, page, limit);
    }, {
        query: t.Object({
            page: t.Optional(t.Numeric()), 
            limit: t.Optional(t.Numeric())
        })
    })
    // GET /chat/history/:conversationId
    .get('/history/:conversationId', async ({ params, query, user }) => {
        
        const rawPage = query.page || 1;
        const page = Math.max(1, rawPage); // บังคับว่าหน้าต้อง >= 1 เสมอ
        const rawLimit = query.limit || 1;
        const limit = Math.max(1, rawLimit); // บังคับว่าต้องดึงอย่างน้อย 1 รายการ
        
        return await getChatHistory(user.id, params.conversationId, page, limit);
        
    }, {
        params: conversationParamsSchema,
        query: chatHistoryQuerySchema 
    })
    // DELETE /chat/:conversationId (ลบห้องแชท)
    .delete('/delete/:conversationId', async ({ params, user }) => {
        return await deleteConversation(user.id, params.conversationId);
    }, {
        params: conversationParamsSchema
    })
    
   /*
   // ไว้ทดสอบ redis
    .get('/debug/redis/:conversationId', async ({ params }) => {
        // ดึงข้อมูลทั้งหมดจาก Redis 
        const history = await redis.lrange(`chat:history:${params.conversationId}`, 0, -1);
        // แปลง Text กลับเป็น JSON 
        return history.map((item: string) => JSON.parse(item));
    }, {
        params: t.Object({
            conversationId: t.String()
        })
    });*/