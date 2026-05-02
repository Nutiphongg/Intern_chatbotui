// src/features/chat/service.ts
import { prisma } from '../setup/prisma';
import { redis } from '../setup/redis';
import { Errors } from '../../lib/errors';
import { ChatRequestBody,EditMessageBody } from './types';
import { ulid } from 'ulid';
import { env } from '../../lib/env';
import { get_map_layer_catalog } from '../mapv2/tools';
import type { Prisma } from '@prisma/client';


const OLLAMA_URL = env.OLLAMA_URL;
const DEFAULT_CHAT_MODEL = 'qwen2.5';
const MAX_HISTORY = 10; // จำแค่ 10 ประโยคล่าสุด
const REDIS_TTL = 3600; // ให้ Redis จำไว้ 1 ชั่วโมง

const shouldUseMapV2Tool = (message: string): boolean => {
    const normalized = message.toLowerCase();
    const mapWords = ['map', 'maps', 'แมพ', 'แมป', 'แผนที่', 'layer', 'url', 'wms', 'wmts', 'tms', 'tile', 'tiles', 'ไทล์', 'vector', 'vector tile', 'vector tiles', 'mvt', 'pbf', 'เวกเตอร์'];
    const hazardWords = ['viirs', 'hotspot', 'hotspots', 'ไฟป่า', 'ไฟไหม้', 'จุดความร้อน', 'น้ำท่วม', 'flood', 'ภัยแล้ง', 'drought', 'dri'];

    return mapWords.some((word) => normalized.includes(word))
        || hazardWords.some((word) => normalized.includes(word));
};

const toPrismaJsonObject = (value: unknown): Prisma.InputJsonObject => {
    return JSON.parse(JSON.stringify(value)) as Prisma.InputJsonObject;
};

export const processChatMessageStream = (userId: string,role:string, body: ChatRequestBody, apiKey?: string) => {
    if (!body || !body.message?.trim()) {
        throw Errors.badRequest('no message data found');
    }

    const isGuest = role === 'guest';
    const message = body.message.trim();
    const selectedModel = body.model?.trim() || DEFAULT_CHAT_MODEL;
    const isSilentRetry = body.is_silent_retry === true;
    const isMapRequest = shouldUseMapV2Tool(message);
    const hasMapApiKey = Boolean(apiKey?.trim());
    const isNewConv = !body.conversationId;
    const convId = body.conversationId || ulid();
    const userMessageId = ulid();
    const userMessageCreatedAt = new Date();
    const redisKey = isGuest? `guest_chat:${convId}`:`chat:${convId}`;


    const encoder = new TextEncoder();
    
    const writeSse = (controller: ReadableStreamDefaultController<Uint8Array>, event: string, data: unknown) => {
        try {
        controller.enqueue(encoder.encode(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`));
        } catch (err) {
            console.log(`[SSE] skipped sending event '${event}' because the client disconnected`);
        }
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
            writeSse(controller, 'meta', {
                conversationId: convId,
                usermessage_id: userMessageId,
                model: selectedModel
            });
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
                                    last_message_at: userMessageCreatedAt
                                } as any
                            });
                        } else {
                            const updated = await prisma.conversations.updateMany({
                                where: { id: convId, user_id: userId },
                                data: { last_message_at: userMessageCreatedAt }
                            });
                        
                            if (updated.count === 0) {
                                writeSse(controller, 'error', { message: 'conversation_not_found' });
                                closeSafely();
                                return;
                            }
                        }
                        if (isSilentRetry && !isNewConv){
                            const latestVisibleUserMessage = await prisma.messages.findFirst({
                                where: {
                                    conversation_id: convId,
                                    role: 'user',
                                    deleted_at:null,
                                    is_generate:false
                                },
                                orderBy:[{created_at:'desc'},{id:'desc'}],
                                select: {id: true}

                            });
                            if (latestVisibleUserMessage) {
                                await prisma.messages.update({
                                    where: { id: latestVisibleUserMessage.id },
                                    data: {
                                        is_generate: true,
                                        is_silent_retry: true
                                    }
                                });
                            }

                            await redis.del(redisKey);
                        }
                        
                        
                        // บันทึกข้อความ user ลงฐานข้อมูล
                        await prisma.messages.create({
                            data: {
                                id: userMessageId,
                                conversation_id: convId,
                                role: 'user',
                                content: message,
                                created_at: userMessageCreatedAt,
                                is_silent_retry: false
                            }
                        });
                    }
                    const personas: Record<string, string> = {
                        normal: `
                        You are a helpful, neutral, and efficient AI assistant.

                        Your goal is to answer clearly, accurately, and concisely.
                        Use direct language without unnecessary emotional tone, exaggeration, or filler.
                        Prioritize practical, useful information.
                        Format answers so they are easy to scan and understand.
                        If the user asks in Thai, respond in Thai.
                        If the user asks in English, respond in English.
                        `,
                        polite: `
                        You are a highly polite, empathetic, and professional assistant.
                        Communicate with warmth, patience, and respect.
                        Use formal, diplomatic language.
                        Acknowledge the user's request before answering.
                        Be helpful without being overly verbose.
                        When appropriate, apologize for inconvenience and offer clear next steps.
                        If responding in Thai:
                        - Use polite vocabulary.
                        - Use appropriate ending particles such as "ครับ" or "ค่ะ".
                        - Avoid casual, harsh, or confrontational wording.
                        `,
                        aggressive: `
                        Style mode: AGGRESSIVE.
                        You are a blunt, playful, rude-but-helpful close friend.

                        Core behavior:
                        - Always answer in the same language as the user.
                        - Be useful first, with a casual, teasing, slightly annoyed tone.
                        - Sound like a close friend, not a customer-service agent.
                        - Keep the rudeness light and playful, not scripted.
                        - Avoid formal Thai particles such as "ครับ", "ค่ะ", "นะครับ", or "นะคะ" unless the user clearly wants politeness.
                        - Avoid stock polite openers such as "ได้เลย", "ยินดี", "พร้อมช่วย", "แน่นอน", "Certainly", or "Of course".
                        - Do not force a fixed opening sentence.
                        - Match the user's energy and the seriousness of the topic.

                        Thai style:
                        - Use natural casual Thai.
                        - Use mild sarcasm only when it fits.
                        - Keep explanations clear and helpful.

                        Safety boundaries:
                        - Do not use hate speech, slurs, threats, or targeted harassment.
                        - Do not encourage violence, self-harm, illegal activity, or abuse.
                        - The rudeness should feel playful and stylistic, not genuinely abusive.
                        `
                        };
                    
                    //ดึงอารมณ์ใช้ที่ User เลือก default 
                    const feelingAliases: Record<string, string> = {
                        normal: 'normal',
                        default: 'normal',
                        polite: 'polite',
                        humble: 'polite',
                        gentle: 'polite',
                        สุภาพ: 'polite',
                        อ่อนน้อม: 'polite',
                        aggressive: 'aggressive',
                        angry: 'aggressive',
                        rude: 'aggressive',
                        sarcastic: 'aggressive',
                        savage: 'aggressive',
                        เกรี้ยวกราด: 'aggressive',
                        เกี้ยวกาจ: 'aggressive',
                        กวน: 'aggressive',
                        กวนตีน: 'aggressive'
                    };
                    const rawFeelingKey = body.feeling?.trim().toLowerCase() || 'normal';
                    const selectedFeelingKey = feelingAliases[rawFeelingKey] || 'normal';
                    if (!feelingAliases[rawFeelingKey]) {
                        console.warn(`[chat] unknown feeling '${body.feeling}', fallback to normal`);
                    }
                    
                    const selectedFeeling = personas[selectedFeelingKey] || personas['normal'];
                    const systemMessage = { role: 'system', content: selectedFeeling };
                    const styleReminder = {
                        role: 'system',
                        content: `Use the "${selectedFeelingKey}" persona naturally for the next reply. Treat it as tone guidance, not a fixed script.`
                    };
                    
                    // เตรียม history ให้ LLM: ใช้ Redis ก่อน ถ้าไม่มีค่อย fallback ไป DB
                    let messagesForLLM: Array<{ role: string, content: string }> = [];
                    if (isNewConv) {
                        const newUserMessage = { id:userMessageId ,role: 'user', content: message, created_at: userMessageCreatedAt.toISOString(), is_silent_retry: false };
                        messagesForLLM = [newUserMessage];
                        await redis.rpush(redisKey, JSON.stringify(newUserMessage));
                    } else {
                        if (isGuest && isSilentRetry) {
                            const cachedForRetry = await redis.lrange(redisKey, 0, -1);
                            for (let index = cachedForRetry.length - 1; index >= 0; index--) {
                                const cachedMessage = JSON.parse(cachedForRetry[index]);
                                if (cachedMessage.role === 'user') {
                                    cachedForRetry.splice(index, 1);
                                    break;
                                }
                            }
                            await redis.del(redisKey);
                            if (cachedForRetry.length > 0) {
                                await redis.rpush(redisKey, ...cachedForRetry);
                                await redis.expire(redisKey, REDIS_TTL);
                            }
                        }
                        const cached = await redis.lrange(redisKey, 0, -1);
                        if (cached.length > 0) {
                            messagesForLLM = cached.map(msg => JSON.parse(msg));
                            const newMessage = { id:userMessageId ,role: 'user', content: message, created_at: userMessageCreatedAt.toISOString(), is_silent_retry: false };
                            messagesForLLM.push(newMessage);
                            await redis.rpush(redisKey, JSON.stringify(newMessage));
                        } else {
                            if (!isGuest) {
                                const dbHistory = await prisma.messages.findMany({
                                    where: {
                                        conversation_id: convId,
                                        deleted_at: null,
                                        is_generate: false
                                    },
                                    orderBy: [{ created_at: 'desc' }, { id: 'desc' }],
                                    take: MAX_HISTORY,
                                    select: { id: true, role: true, content: true, metadata: true, created_at: true, is_silent_retry: true }
                                });

                                messagesForLLM = dbHistory.reverse();
                                const newMessage = { id: userMessageId, role: 'user', content: message, created_at: userMessageCreatedAt.toISOString(), is_silent_retry: false };
                                messagesForLLM.push(newMessage);

                                const pipeline = redis.pipeline();
                                messagesForLLM.forEach(msg => pipeline.rpush(redisKey, JSON.stringify(msg)));
                                await pipeline.exec();
                            
                            } else {
                                const newMessage = { id: userMessageId,role: 'user', content: message, created_at: userMessageCreatedAt.toISOString(), is_silent_retry: false };
                            messagesForLLM.push(newMessage);
                            await redis.rpush(redisKey, JSON.stringify(newMessage));
                            }
                        }
                    }
                    const sanitizedMessagesForLLM = messagesForLLM
                        .filter((msg) => msg.role !== 'system')
                        .map((msg) => ({ role: msg.role, content: msg.content }));
                    const lastMessageForLLM = sanitizedMessagesForLLM.pop();
                    let mapToolContext: { role: string, content: string } | undefined;
                    let mapMetadata: Prisma.InputJsonObject | undefined;
                    if (isMapRequest && !hasMapApiKey) {
                        writeSse(controller, 'map_error', {
                            message: 'missing_x_api_key',
                            needsApiKey: true,
                            silentRetrySupported: true
                        });
                        writeSse(controller, 'done', {
                            done: true,
                            tokenUsage: 0,
                            assistantmessage_Id: null,
                            skippedAssistantReply: true,
                            reason: 'missing_x_api_key'
                        });
                        closeSafely();
                        return;
                    }
                    // Temporarily disabled old map stream text while testing mapv2 with streamsse.
                    // try {
                    //     const mapToolResult = await runHotspotsToolFromMessage(message);
                    //     if (mapToolResult) {
                    //         const hotspots = mapToolResult.result.data.features.length;
                    //         writeSse(controller, 'map', {
                    //             event: mapToolResult.event,
                    //             query: mapToolResult.query,
                    //             geojsonUrl: mapToolResult.geojsonUrl,
                    //             ...mapToolResult.result
                    //         });
                    //         mapMetadata = toPrismaJsonObject({
                    //             type: 'map',
                    //             event: mapToolResult.event,
                    //             layerId: mapToolResult.result.layerId,
                    //             source: mapToolResult.result.source,
                    //             query: mapToolResult.query,
                    //             geojsonUrl: mapToolResult.geojsonUrl,
                    //             featureCount: hotspots,
                    //             mapAction: mapToolResult.result.mapAction,
                    //         });
                    //         const layerDescription = mapToolResult.event === 'hotspots'
                    //             ? `VIIRS hotspot (${mapToolResult.query.days} day)`
                    //             : `${mapToolResult.query.kind} at lat ${mapToolResult.query.lat}, lon ${mapToolResult.query.lon}`;
                    //         mapToolContext = {
                    //             role: 'system',
                    //             content: `Map tool already returned ${hotspots} ${layerDescription} feature(s) to the frontend as GeoJSON. Briefly tell the user that the map layer is ready, and do not paste raw GeoJSON.`
                    //         };
                    //     }
                    // } catch (error) {
                    //     console.error('Map Tool Error:', error);
                    //     writeSse(controller, 'map_error', {
                    //         message: error instanceof Error ? error.message : 'map_tool_failed'
                    //     });
                    // }
                    if (isMapRequest) {
                        try {
                            const toolResult = await get_map_layer_catalog.execute({ message, apiKey });

                            writeSse(controller, 'map', toolResult);
                            mapMetadata = toPrismaJsonObject(toolResult);
                            mapToolContext = {
                                role: 'system',
                                content: `MapV2 generated a ${toolResult.layer.type.toUpperCase()} URL for days: ${toolResult.layer.url}. Tell the user the map URL is ready and keep the response brief.`
                            };
                        } catch (error) {
                            console.error('MapV2 Tool Error:', error);
                            writeSse(controller, 'map_error', {
                                message: error instanceof Error ? error.message : 'mapv2_tool_failed'
                            });
                        }
                    }

                    const messagesForOllama = [
                        systemMessage,
                        ...sanitizedMessagesForLLM,
                        styleReminder,
                        ...(mapToolContext ? [mapToolContext] : []),
                        ...(lastMessageForLLM ? [lastMessageForLLM] : [])
                    ];
                    // เรียก Ollama แบบ stream เพื่อรับ token ทีละส่วน
                    const startTime = Date.now();
                    const ollamaPayload: Record<string, unknown> = {
                        model: selectedModel,
                        messages: messagesForOllama,
                        stream: true
                    };
                    if (selectedFeelingKey === 'aggressive') {
                        ollamaPayload.options = { temperature: 0.7, top_p: 0.9 };
                    } else if (selectedFeelingKey === 'polite') {
                        ollamaPayload.options = { temperature: 0.45, top_p: 0.85 };
                    }
                    
                    const ollamaResponse = await fetch(`${env.OLLAMA_URL}/api/chat`, {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify(ollamaPayload)
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
                    const assistantMessageId = ulid();
                    const assistantMessageCreatedAt = new Date(Math.max(Date.now(), userMessageCreatedAt.getTime() + 1));
                    if (assistantReply) {
                        const botMessage = {
                            id: assistantMessageId,
                            role: 'assistant',
                            content: assistantReply,
                            model: selectedModel,
                            metadata: mapMetadata,
                            created_at: assistantMessageCreatedAt.toISOString()
                        };
                        if(! isGuest) {
                            await prisma.messages.create({
                                data: {
                                    id: assistantMessageId,
                                    conversation_id: convId,
                                    role: 'assistant',
                                    content: assistantReply,
                                    model: selectedModel,
                                    response_time: responseTimeMs,
                                    token_usage: tokenUsage,
                                    metadata: mapMetadata,
                                    created_at: assistantMessageCreatedAt
                                }
                           });
                            await prisma.conversations.updateMany({
                                where: { id: convId, user_id: userId },
                                data: { last_message_at: assistantMessageCreatedAt }
                            });
                        };

                        await redis.rpush(redisKey, JSON.stringify(botMessage));
                        await redis.ltrim(redisKey, -MAX_HISTORY, -1);
                        await redis.expire(redisKey, REDIS_TTL);
                    }

                    // แจ้งจบ stream ให้ frontend ปิด loading/state
                    writeSse(controller, 'done', {
                        done :
                        tokenUsage,
                        assistantmessage_Id: assistantMessageId,
                    });
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

    const allMessages = cached
   
    const totalCount = cached.length;
    const start = totalCount - (page * limit);
    const end = totalCount -((page - 1) * limit);

    const safeStart = Math.max(0,start);
    const safeEnd = Math.max(0, end);
    
    const pageItems = cached.slice(safeStart, safeEnd);
    
    
    const messages = pageItems.map((msg) => {
        const parsed = JSON.parse(msg);
        return {
            id:parsed.id,
            role: parsed.role,
            content: parsed.content,
            metadata: parsed.metadata,
            is_silent_retry: parsed.is_silent_retry ?? false,
            created_at: parsed.created_at ?? new Date().toISOString(),
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
            where: { conversation_id: conversationId ,is_generate:false},
            orderBy: [{ created_at: 'desc' },{id:'desc'}], // 
            skip: skip,
            take: limit,
            select: { id: true, role: true, content: true, metadata: true, created_at: true, is_silent_retry: true }
        }),
        prisma.messages.count({
            where: { conversation_id: conversationId,is_generate: false }
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
        // ฝั่ง Guest: 
        const redisKey = `guest_chat:${userId}`; // userId ของฝั่งนี้ก็คือ guestId 
        const messageCount = await redis.llen(redisKey); // นับจำนวนข้อความ
        if (messageCount === 0) {
            return {
                data: [],
                pagination: { currentPage: 1, pageSize: limit, totalItems: 0, totalPages: 0 }
            };
        }
        const GuestConversation = {
            id: userId, // หรือจะรับค่ามาก็ได้
            title: 'Guest Session',
            created_at: new Date(),
            updated_at: new Date(),
            last_message_at: new Date()
        };

        return {
            data: [GuestConversation],
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
// ฟังก์ชันโอนย้ายประวัติแชทจาก Guest ไปหา User
export const migrateGuestChatToUser = async (userId: string, guestId: string) => {
    const redisKey = `guest_chat:${guestId}`;
    const quotaKey = `guest_quota:${guestId}`;

    // 1. ดึงประวัติทั้งหมดจาก Redis
    const cachedMessages = await redis.lrange(redisKey, 0, -1);

    // ถ้าไม่มีประวัติแชทเลย ให้จบการทำงาน
    if (!cachedMessages || cachedMessages.length === 0) {
        return null; 
    }
    const parsedMessages = cachedMessages
        .map((msgStr) => JSON.parse(msgStr))

      
    const firstMessageData = JSON.parse(cachedMessages[0]);
    // ตัดเอาแค่ 50 ตัวอักษรแรก ถ้าสั้นกว่านั้นก็เอาทั้งหมด
    const autoTitle = firstMessageData.content.length > 50 
        ? firstMessageData.content.substring(0, 50) + "..." 
        : firstMessageData.content;
   
    // 2. สร้างห้องแชทใหม่ให้ User ใน PostgreSQL
    const newConversation = await prisma.conversations.create({
        data: {
            id: ulid(),
            user_id: userId,
            is_from_guest: true,
            // อาจจะตั้งชื่อห้องเริ่มต้นไว้ก่อน
            title: autoTitle ,
            last_message_at: new Date()
        }
    });

    const baseTime = Date.now();
    
    // 3. แปลงข้อมูลจาก Redis ให้ตรงกับ Database Schema
    const messagesToInsert = parsedMessages.map((msg, index) => {
        // ใช้ created_at ที่เก็บใน Redis ถ้ามี, fallback เป็น baseTime + index
        const createdAt = msg.created_at && !isNaN(new Date(msg.created_at).getTime())
            ? new Date(msg.created_at)
            : new Date(baseTime + index);

        return {
            id: msg.id ,
            conversation_id: newConversation.id,
            role: msg.role,
            content: msg.content,
            model: msg.model ?? null,
            response_time: msg.response_time ?? null,
            token_usage: msg.token_usage ?? null,
            created_at: createdAt,
        };
    });

    // 4. บันทึกข้อความทั้งหมดลง PostgreSQL ในรวดเดียว (Bulk Insert)
    await prisma.messages.createMany({
        data: messagesToInsert
    });

    // 5. คลีนอัป: ลบข้อมูล Guest ใน Redis ทิ้งทั้งหมด
    await redis.del(redisKey);
    await redis.del(quotaKey);

    // คืนค่า ID ห้องแชทใหม่ เผื่อ Frontend อยากเอาไป Redirect
    return newConversation.id; 
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

export const editMessage = async (
  userId: string,
  messageId: string,
  newContent: string,
  is_generate: boolean // 
) => {

  // 1. หา message เดิม
  const oldMessage = await prisma.messages.findFirst({
    where: {
      id: messageId,
      conversations: { user_id: userId },
      deleted_at: null
    }
  });

  if (!oldMessage) throw new Error("Message not found");
  if (oldMessage.role !== "user") throw new Error("Edit only user message");

  // 2. หา latest ด้วย ULID (สำคัญ)
  const latestUserMessage = await prisma.messages.findFirst({
    where: {
      conversation_id: oldMessage.conversation_id,
      role: "user",
      deleted_at: null,
      is_generate: false
    },
    orderBy: { id: "desc" } // 
  });

  if (latestUserMessage?.id !== messageId) {
    throw new Error("Can edit only latest message");
  }

  // 3. mark ตั้งแต่ message นี้ไปทั้งหมดเป็น obsolete
  await prisma.messages.updateMany({
    where: {
      conversation_id: oldMessage.conversation_id,
      id: { gte: messageId }, // 
      deleted_at: null,
      is_generate: false
    },
    data: {
      is_generate: true
    }
  });

  // 4. สร้าง message ใหม่
  const newMessage = await prisma.messages.create({
    data: {
      id: ulid(),
      conversation_id: oldMessage.conversation_id,
      role: "user",
      content: newContent,
      //  ใช้ค่าจาก frontend
      is_generate: is_generate ?? false
    },
    select: {
      id: true,
      content: true,
      role: true,
      created_at: true
    }
  });

  // 5. clear cache
  await redis.del(`chat:${oldMessage.conversation_id}`);
  await redis.del(`guest_chat:${oldMessage.conversation_id}`);

  return newMessage;
};

export const editConvTitle = async (
    userId: string,
    conversationId: string,
    newTitle: string
) => {
    // check ว่ามีห้อง chat 
    const existingConv = await prisma.conversations.findFirst({
        where: {
            id: conversationId,
            user_id: userId,
            is_deleted : false
        }
    });

    if (!existingConv) {
        throw new Error("not found chat")
    }

    const updatedConv = await prisma.conversations.update({
        where: {
            id: conversationId
        },
        data: {
            title: newTitle,
            updated_at: new Date()
        },
        select: {
            id: true,
            title: true,
            updated_at: true,
        }
    });
    
    await redis.del(`chat:${conversationId}`);
    await redis.del(`guest_chat:${conversationId}`);

    return updatedConv;

}

export const getAvailableModels = async () => {
    const url = `${env.OLLAMA_URL}/api/tags`; 
    
    try {
        const response = await fetch(url);

        if (!response.ok) {
            // ถ้าเชื่อมต่อได้ แต่ Ollama ฟ้อง Error กลับมา (เช่น 404, 400)
            const errorText = await response.text();
            console.error(` Ollama Response Error [Status: ${response.status}]:`, errorText);
            throw new Error(`Ollama responded with status ${response.status}`);
        }

        const data = await response.json();
        const availableModels = data.models.map((model: any) => {
            const paramSize = model.details?.parameter_size;
          return {
              id: model.name, 
            name: model.name.split(':')[0].toUpperCase(), 
            size: paramSize 
          };
        });

        return availableModels; 
        
    } catch (error) {
       
       
        console.error('รายละเอียด Error:', error);
        
        // โยน Error 500 กลับไปให้ Route 
        throw Errors.internalServerError(); 
    }
};
