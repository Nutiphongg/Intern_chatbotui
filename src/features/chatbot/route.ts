// src/features/chat/route.ts
import { Elysia, t } from 'elysia';
import { chatHistoryQuerySchema, chatRequestSchema, conversationParamsSchema ,editMessageParamsSchema,editMessageBodySchema} from './types';
import { processChatMessageStream, getChatHistory, getUserConversations,deleteConversation,editMessage, } from './service';
import { authPlugin } from '../../plugins/plugin';
import { redis } from '../setup/redis';

export const chatRoutes = new Elysia({ prefix: '/chat' })
    .use(authPlugin)
    // POST /chatbot
    .post('/', async ({ body, user })  => {
        //  1. ด่านตรวจ Auto-Routing แยกสาย Guest vs User
        if (user.role === 'guest') {
            // ถ้าเป็น Guest: บังคับให้ ID ห้องแชท เป็น ID ของ Guest 
            body.conversationId = user.id; 
        } 
        // 2. ส่งข้อมูลที่ปรับแต่ง เข้าฟังก์ชันสตรีม
        const result = processChatMessageStream(user.id, user.role, body);
    
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
        return await getUserConversations(user.id, user.role, page, limit);
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
        const rawLimit = query.limit || 5;
        const limit = Math.max(1, rawLimit); // บังคับว่าต้องดึงอย่างน้อย 1 รายการ
        
        return await getChatHistory(user.id,user.role, params.conversationId, page, limit);
        
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
    // PUT /chat/message/:messageId (แก้ไขข้อความ)
 
    .put('/editmessage/:messageId', async ({ params, body, user }) => {
        const { messageId } = params;
        const { newContent } = body;

        const updatedMessage = await editMessage(user.id, messageId, newContent);
        return {
            data: updatedMessage
        };
    }, {
        
        params: editMessageParamsSchema,
        body: editMessageBodySchema,
    })