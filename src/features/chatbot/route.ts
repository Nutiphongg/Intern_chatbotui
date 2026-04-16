// src/features/chat/route.ts
import { Elysia, t } from 'elysia';
import { chatRequestSchema } from './types';
import { processChatMessage, getChatHistory, getUserConversations,deleteConversation,editMessage } from './service';
import { authPlugin } from '../../plugins/plugin';
import { redis } from '../setup/redis';

export const chatRoutes = new Elysia({ prefix: '/chat' })
    .use(authPlugin)
    // POST /chatbot
    .post('/', async ({ body, user })  => {
        const result = await processChatMessage(user.id,body);
        return { data: result };
    }, {
        body: chatRequestSchema
    })
    // GET /chat/histories
    .get('/histories', async ({user}) => {
        const data = await getUserConversations(user.id);
        return {data};
    })

    // GET /chat/history/:conversationId
    .get('/history/:conversationId', async ({ params, query, user }) => {
        const limit = query.limit || 50; 
        const data = await getChatHistory(user.id ,params.conversationId, limit);
        return  { data };
    }, {
        params: t.Object({
            conversationId: t.String()
        }),
        query: t.Object({
            limit: t.Optional(t.Numeric())
        })
    })
    // DELETE /chat/:conversationId (ลบห้องแชท)
    .delete('/delete/:conversationId', async ({ params, user }) => {
        return await deleteConversation(user.id, params.conversationId);
    }, {
        params: t.Object({ conversationId: t.String() })
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