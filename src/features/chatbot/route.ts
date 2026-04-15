// src/features/chat/route.ts
import { Elysia, t } from 'elysia';
import { chatRequestSchema } from './types';
import { processChatMessage, getChatHistory } from './service';

export const chatRoutes = new Elysia({ prefix: '/chat' })

    // POST /chatbot
    .post('/', async ({ body })  => {
        const result = await processChatMessage(body);
        return { success: true, data: result };
    }, {
        body: chatRequestSchema
    })

    // GET /chat/history/:conversationId
    .get('/history/:conversationId', async ({ params, query }) => {
        const limit = query.limit || 50; 
        const data = await getChatHistory(params.conversationId, limit);
        return { success: true, data };
    }, {
        params: t.Object({
            conversationId: t.String()
        }),
        query: t.Object({
            limit: t.Optional(t.Numeric())
        })
    });