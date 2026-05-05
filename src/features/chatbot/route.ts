// src/features/chat/route.ts
import { Elysia, t } from 'elysia';
import { chatHistoryQuerySchema, chatRequestSchema, conversationParamsSchema ,editMessageParamsSchema,editMessageBodySchema,updateConvTitleParamsSchema,updateConveTitleBodySchema} from './types';
import { processChatMessageStream, getChatHistory, getUserConversations,deleteConversation,editMessage, editConvTitle,getAvailableModels} from './service';
import { authPlugin } from '../../plugins/plugin';
import { redis } from '../setup/redis';
import { env} from '../../lib/env'

export const chatRoutes = new Elysia({ prefix: '/chat' })
 //GET /models
    .get('/models', async () => {
     return await getAvailableModels();
    })
    .use(authPlugin)
    // POST /chatbot
    .post('', async ({ body, user, request })  => {
        //  1. ด่านตรวจ Auto-Routing แยกสาย Guest vs User
        if (user.role === 'guest') {
            // ถ้าเป็น Guest: บังคับให้ ID ห้องแชท เป็น ID ของ Guest 
            body.conversationId = user.id; 
        } 
        // 2. ส่งข้อมูลที่ปรับแต่ง เข้าฟังก์ชันสตรีม
        const apiKey = request.headers.get('x-api-key') || undefined;
        const vectorApiKey = request.headers.get('x-vector-api-key')
            || request.headers.get('x-vallaris-api-key')
            || undefined;
        const result = processChatMessageStream(user.id, user.role, body, apiKey, vectorApiKey);
    
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
   
   // GET /conversations
    .get('/conversations', async ({ query, user }) => {
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
    // GET /chat/conversations/:conversationId
    .get('/conversations/:conversationId/messages', async ({ params, query, user }) => {
        
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
    .delete('/conversations/:conversationId', async ({ params, user }) => {
        return await deleteConversation(user.id, params.conversationId);
    }, {
        params: conversationParamsSchema
    })
    
 
    .put('/messages/:messageId', async ({ params, body, user }) => {
        if (!user) throw new Error("Unauthorized");

        const { messageId } = params;
        const { newContent, is_generate } = body;

        

        const updatedMessage = await editMessage(
            user.id,
            messageId,
            newContent,
            is_generate
        );

        return {
            message: "Edit message success",
            data: updatedMessage
        };
        }, {
        params: editMessageParamsSchema,
        body: editMessageBodySchema,
    })

    .put('/conversations/:conversationId', async ({ params, body ,user}) => {
        if (!user) throw new Error("Unauthorized");

        const { conversationId } = params;
        const { title } = body;

        const updatedConv = await editConvTitle(
            user.id,
            conversationId,
            title
        );

        return {
            data:updatedConv
        };

    }, {
        params: updateConvTitleParamsSchema,
        body:updateConveTitleBodySchema
        
    })

