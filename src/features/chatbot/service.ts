// src/features/chat/service.ts
import { prisma } from '../setup/prisma';
import { redis } from '../setup/redis';
import { Errors } from '../../lib/errors';
import { ChatRequestBody,EditMessageBody } from './types';
import { ulid } from 'ulid';
import { env } from '../../lib/env';
import { decrypt, hashApiKey } from '../setup/encryption';
import {
    mapToolSchema,
    handleMapTool,
    buildMapStylePayload,
    editMapStyleToolSchema,
    handleEditMapStyleTool,
    clearMapLayersToolSchema,
    handleClearMapLayersTool,
    handleStyleCatalogTool,
    checkMapAccessSchema,
    handleCheckMapAccess,
    mapOptionToolSchema,
    handleMapOptionsTool,
    buildDynamicMapOptionToolSchema,
    resolveUserMapToolConfigs,
    buildMapOptionChoiceContext,
    handleMapAttributeValuesTool,
    handleRenderPmtilesLayerTool
} from '../map/tools';
import { Prisma } from '@prisma/client';
import {
    retrieveConversationMemoryChunks,
    saveConversationMemoryChunks
} from './memoryChunks';



const OLLAMA_URL = env.OLLAMA_URL;
const DEFAULT_CHAT_MODEL = 'qwen2.5';
const VISION_MODEL = env.VISION_MODEL;
const MEMORY_SUMMARY_MODEL = env.MEMORY_SUMMARY_MODEL.trim();
const SUPABASE_URL = env.SUPABASE_URL.trim().replace(/\/+$/, '').replace(/\/rest\/v1$/, '');
const SUPABASE_SERVICE_ROLE_KEY = env.SUPABASE_SERVICE_ROLE_KEY.trim();
const CHAT_ATTACHMENTS_BUCKET = env.SUPABASE_CHAT_ATTACHMENTS_BUCKET;
const MAX_HISTORY = 10; // จำแค่ 10 ประโยคล่าสุด
const REDIS_TTL = 3600; // ให้ Redis จำไว้ 1 ชั่วโมง
const DEFAULT_OUTPUT_TOKENS = 512;
const MAP_INTENT_ROUTER_TOKENS = 16;
const MAP_INTENT_ROUTER_TIMEOUT_MS = 12000;
const VISION_OUTPUT_TOKENS = 3072;
const VISION_REQUEST_TIMEOUT_MS = 180000;
const CHAT_ATTACHMENT_SIGNED_URL_TTL_SECONDS = 60 * 60 * 24;
const MAX_CHAT_IMAGES = 3;
const MAX_CHAT_IMAGE_BYTES = 5 * 1024 * 1024;
const ALLOWED_CHAT_IMAGE_MIME_TYPES = new Set(['image/jpeg', 'image/png', 'image/webp']);

type ChatImageAttachment = {
    type: 'image';
    mimeType: string;
    size: number;
    base64: string;
};

type StoredChatImageAttachment = {
    type: 'image';
    mimeType: string;
    size: number;
    bucket: string;
    path: string;
};

type VisionDominantColor = {
    name: string;
    hex?: string;
};

type VisionAnalysis = {
    summary?: string;
    dominantColors?: VisionDominantColor[];
    dominantColorsStatus?: 'pending' | 'done' | 'error';
};

type ChatHistoryMessage = {
    id: string;
    role: string;
    content: string;
    created_at: string;
    is_silent_retry: boolean;
    metadata?: Prisma.InputJsonObject;
};

type OllamaModelInfo = {
    name: string;
    model?: string;
    details?: {
        parameter_size?: string;
    };
};

type ConversationApiKeyRecord = {
    id: string;
    encryptedKey: string;
    iv: string;
};

let resolvedVisionModelPromise: Promise<string> | undefined;

const findConversationApiKey = async (
    userId: string,
    conversationId: string
): Promise<ConversationApiKeyRecord | undefined> => {
    const rows = await prisma.$queryRaw<ConversationApiKeyRecord[]>`
        SELECT api_key."id", api_key."encryptedKey", api_key."iv"
        FROM "conversation_api_keys" conversation_key
        INNER JOIN "conversations" conversation
            ON conversation."id" = conversation_key."conversationId"
        INNER JOIN "user_apikey" api_key
            ON api_key."id" = conversation_key."userApiKeyId"
        WHERE conversation_key."conversationId" = ${conversationId}
            AND conversation."user_id" = ${userId}
            AND conversation."is_deleted" = false
            AND api_key."user_id" = ${userId}
            AND api_key."isActive" = true
            AND api_key."deletedAt" IS NULL
        LIMIT 1
    `;

    return rows[0];
};

const getConversationApiKey = async (
    userId: string,
    conversationId: string
): Promise<string | undefined> => {
    const apiKey = await findConversationApiKey(userId, conversationId);
    if (!apiKey) return undefined;

    return decrypt(apiKey.encryptedKey, apiKey.iv);
};

const findUserApiKeyFromHeader = async (
    userId: string,
    headerApiKey: string
): Promise<string | undefined> => {
    const cleanApiKey = headerApiKey.trim();
    const keyHash = hashApiKey(cleanApiKey);
    const existingKey = await prisma.user_apikey.findFirst({
        where: {
            userId,
            keyHash,
            isActive: true,
            deletedAt: null
        },
        select: { id: true },
        orderBy: { createdAt: 'desc' }
    });

    return existingKey?.id;
};

const linkConversationApiKey = async (
    conversationId: string,
    userApiKeyId: string
): Promise<void> => {
    await prisma.$executeRaw`
        INSERT INTO "conversation_api_keys" ("id", "conversationId", "userApiKeyId")
        VALUES (${ulid()}, ${conversationId}, ${userApiKeyId})
        ON CONFLICT ("conversationId")
        DO UPDATE SET "userApiKeyId" = EXCLUDED."userApiKeyId"
    `;
};

const fetchOllamaModels = async (): Promise<OllamaModelInfo[]> => {
    const response = await fetch(`${OLLAMA_URL}/api/tags`);
    if (!response.ok) {
        return [];
    }

    const data = await response.json() as { models?: OllamaModelInfo[] };
    return Array.isArray(data.models) ? data.models : [];
};

const resolveVisionModelName = async (): Promise<string> => {
    const configuredModel = VISION_MODEL.trim();
    if (configuredModel.includes(':')) return configuredModel;

    const models = await fetchOllamaModels();
    const exactMatch = models.find((model) => model.name === configuredModel);
    if (exactMatch) return exactMatch.name;

    if (!configuredModel.includes(':')) {
        const taggedMatch = models.find((model) => model.name.split(':')[0] === configuredModel);
        if (taggedMatch) return taggedMatch.name;
    }

    return configuredModel;
};

const getResolvedVisionModelName = async (): Promise<string> => {
    resolvedVisionModelPromise ??= resolveVisionModelName().catch((error) => {
        resolvedVisionModelPromise = undefined;
        console.error('[vision] resolve model failed:', error);
        return VISION_MODEL;
    });

    return resolvedVisionModelPromise;
};

type MapRequestIntent = 'map_access' | 'map_control' | 'chat';

const classifyMapRequestIntent = async (
    message: string,
    hasImages: boolean,
    model: string,
    currentMapStyle?: unknown
): Promise<MapRequestIntent> => {
    const trimmedMessage = message.trim();
    if (!trimmedMessage) return 'chat';

    const abortController = new AbortController();
    const timeout = setTimeout(() => abortController.abort(), MAP_INTENT_ROUTER_TIMEOUT_MS);

    try {
        const response = await fetch(`${OLLAMA_URL}/api/chat`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                model,
                stream: false,
                options: {
                    temperature: 0,
                    num_predict: MAP_INTENT_ROUTER_TOKENS
                },
                messages: [
                    {
                        role: 'system',
                        content: [
                            'Classify the user request for the map/chat pipeline.',
                            'Return only JSON with one of these intents: {"intent":"map_access"}, {"intent":"map_control"}, or {"intent":"chat"}.',
                            'Use "map_access" only when the user wants to list, fetch, open, get a URL, get WMS/WMTS/TMS/vector tile, or retrieve map/layer data.',
                            'Use "map_access" when the user asks to list, search, fetch, or choose existing provider map styles/style records/style catalog from the map API.',
                            'An existing active map or currentStyleProperties must not change a list/search/fetch/open provider-data request into map_control.',
                            'Use "map_control" when the user wants to manage already displayed map state without fetching provider data, such as clearing, hiding, or editing visual style/paint/layout/colors of existing displayed layers.',
                            'Use "map_control" when the user wants to limit the currently displayed map features by attribute/value conditions, modify those conditions, or clear them.',
                            'Use "map_control" when the user asks to style the current map by an attribute or field, including wording shaped like "style the map attribute FIELD". This is an executable map edit, not a request for instructions.',
                            'When currentStyleProperties is not empty, use "map_control" for imperative add/change/set/remove/delete requests that semantically refer to one of those active style properties, even when the user describes the property using a synonym instead of its exact MapLibre key.',
                            'When hasImages is true, words like image/photo/picture refer to the attached image.',
                            'Use "map_control" when the user asks to use, add, apply, or change map/layer/style/paint colors from the attached image/photo/picture, including wording like "like image", "same as image", or "based on image".',
                            'Use "chat" when the user only asks what colors are in the image or asks to describe/analyze the image, even if a map was discussed earlier.',
                            'Use "chat" for general discussion, explanations, image analysis, or visual/map style advice that does not require fetching existing provider data.'
                        ].join('\n')
                    },
                    {
                        role: 'user',
                        content: JSON.stringify({
                            message: trimmedMessage,
                            hasImages,
                            currentStyleProperties: Array.from(collectMapStylePropertyKeys(currentMapStyle))
                        })
                    }
                ]
            }),
            signal: abortController.signal
        }).finally(() => clearTimeout(timeout));

        if (!response.ok) return 'chat';

        const payload = await response.json() as { message?: { content?: string } };
        const content = payload.message?.content?.trim() || '';
        const jsonMatch = content.match(/\{[\s\S]*\}/);
        const parsed = JSON.parse(jsonMatch?.[0] || content) as { intent?: string };
        return parsed.intent === 'map_access' || parsed.intent === 'map_control'
            ? parsed.intent
            : 'chat';
    } catch (error) {
        if (!(error instanceof DOMException && error.name === 'AbortError')) {
            console.error('[map-intent] classify failed:', error);
        }
        return 'chat';
    }
};

const collectChatImages = (body: ChatRequestBody): string[] => {
    return Array.isArray(body.images)
        ? body.images.filter((image): image is string => typeof image === 'string' && Boolean(image.trim()))
        : [];
};

const parseChatImage = (value: string): ChatImageAttachment => {
    const cleanValue = value.trim();
    const match = cleanValue.match(/^data:(image\/(?:jpeg|png|webp));base64,([A-Za-z0-9+/=\s]+)$/i);
    const mimeType = match?.[1]?.toLowerCase();
    const base64 = (match?.[2] || cleanValue).replace(/\s/g, '');

    if (!mimeType || !ALLOWED_CHAT_IMAGE_MIME_TYPES.has(mimeType)) {
        throw Errors.badRequest('unsupported image type');
    }

    const size = Buffer.byteLength(base64, 'base64');
    if (size <= 0 || size > MAX_CHAT_IMAGE_BYTES) {
        throw Errors.badRequest('image size is too large');
    }

    return {
        type: 'image',
        mimeType,
        size,
        base64
    };
};

const getImageExtension = (mimeType: string): string => {
    if (mimeType === 'image/jpeg') return 'jpg';
    if (mimeType === 'image/webp') return 'webp';
    return 'png';
};

const encodeStoragePath = (path: string): string => {
    return path.split('/').map(encodeURIComponent).join('/');
};

const assertSupabaseStorageConfigured = () => {
    if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY || !CHAT_ATTACHMENTS_BUCKET) {
        console.error('[chat-attachments] Supabase storage env is not configured');
        throw Errors.internalServerError();
    }
};

const uploadChatImageAttachments = async (
    images: ChatImageAttachment[],
    userId: string,
    conversationId: string,
    messageId: string
): Promise<StoredChatImageAttachment[]> => {
    if (images.length === 0) return [];
    assertSupabaseStorageConfigured();

    return Promise.all(images.map(async (image, index) => {
        const extension = getImageExtension(image.mimeType);
        const fileName = `${String(index + 1).padStart(2, '0')}.${extension}`;
        const path = `messages/${messageId}/${fileName}`;
        const response = await fetch(`${SUPABASE_URL}/storage/v1/object/${CHAT_ATTACHMENTS_BUCKET}/${encodeStoragePath(path)}`, {
            method: 'POST',
            headers: {
                apikey: SUPABASE_SERVICE_ROLE_KEY,
                Authorization: `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
                'Content-Type': image.mimeType,
                'Cache-Control': '3600',
                'x-upsert': 'false'
            },
            body: Buffer.from(image.base64, 'base64')
        });

        if (!response.ok) {
            const errorText = await response.text().catch(() => '');
            throw new Error(`attachment_upload_failed: ${response.status} ${response.statusText}${errorText ? ` - ${errorText}` : ''}`);
        }

        return {
            type: image.type,
            mimeType: image.mimeType,
            size: image.size,
            bucket: CHAT_ATTACHMENTS_BUCKET,
            path
        };
    }));
};

const createAttachmentSignedUrl = async (bucket: string, path: string): Promise<string | undefined> => {
    if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY || !bucket || !path) return undefined;

    const response = await fetch(`${SUPABASE_URL}/storage/v1/object/sign/${bucket}/${encodeStoragePath(path)}`, {
        method: 'POST',
        headers: {
            apikey: SUPABASE_SERVICE_ROLE_KEY,
            Authorization: `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
            'Content-Type': 'application/json'
        },
        body: JSON.stringify({ expiresIn: CHAT_ATTACHMENT_SIGNED_URL_TTL_SECONDS })
    });

    if (!response.ok) {
        const errorText = await response.text().catch(() => '');
        console.error(`[chat-attachments] signed url failed: ${response.status} ${response.statusText}${errorText ? ` - ${errorText}` : ''}`);
        return undefined;
    }

    const payload = await response.json() as { signedURL?: string; signedUrl?: string };
    const signedPath = payload.signedURL || payload.signedUrl;
    if (!signedPath) return undefined;

    if (signedPath.startsWith('http')) return signedPath;

    const normalizedSignedPath = signedPath.startsWith('/storage/v1')
        ? signedPath
        : `/storage/v1${signedPath.startsWith('/') ? signedPath : `/${signedPath}`}`;
    return `${SUPABASE_URL}${normalizedSignedPath}`;
};

const hydrateChatAttachmentUrls = async (metadata: unknown): Promise<unknown> => {
    if (!metadata || typeof metadata !== 'object' || Array.isArray(metadata)) {
        return metadata;
    }

    const metadataRecord = metadata as Record<string, unknown>;
    const attachments = Array.isArray(metadataRecord.attachments) ? metadataRecord.attachments : [];
    if (attachments.length === 0) return metadata;

    const hydratedAttachments = await Promise.all(attachments.map(async (attachment) => {
        if (!attachment || typeof attachment !== 'object' || Array.isArray(attachment)) {
            return attachment;
        }

        const attachmentRecord = attachment as Record<string, unknown>;
        const bucket = typeof attachmentRecord.bucket === 'string' ? attachmentRecord.bucket : CHAT_ATTACHMENTS_BUCKET;
        const path = typeof attachmentRecord.path === 'string' ? attachmentRecord.path : undefined;
        const url = path ? await createAttachmentSignedUrl(bucket, path) : undefined;

        const {
            dataUrl,
            base64,
            url: existingUrl,
            ...safeAttachment
        } = attachmentRecord;
        return url ? { ...safeAttachment, url } : safeAttachment;
    }));

    return {
        ...metadataRecord,
        attachments: hydratedAttachments
    };
};

const getStreamErrorMessage = (error: unknown): string => {
    const message = error instanceof Error ? error.message : '';
    if (message.startsWith('attachment_upload_failed')) return 'attachment_upload_failed';
    if (message.toLowerCase().includes('map')) return 'map_stream_failed';
    if (message.toLowerCase().includes('vallaris')) return 'map_stream_failed';
    return 'stream_failed';
};

const extractOllamaTextContent = (payload: unknown): string | undefined => {
    const record = asRecord(payload);
    const message = asRecord(record.message);
    const content = message.content
        ?? record.response
        ?? record.content
        ?? message.text
        ?? record.text
        ?? asRecord(record.output).text;

    if (typeof content === 'string' && content.trim()) {
        return content.trim();
    }

    if (Array.isArray(content)) {
        const text = content
            .map((item) => {
                if (typeof item === 'string') return item;
                const itemRecord = asRecord(item);
                return typeof itemRecord.text === 'string' ? itemRecord.text : '';
            })
            .join('')
            .trim();
        return text || undefined;
    }

    const choices = Array.isArray(record.choices) ? record.choices : [];
    const choiceText = choices
        .map((choice) => {
            const choiceRecord = asRecord(choice);
            const choiceMessage = asRecord(choiceRecord.message);
            return choiceMessage.content || choiceRecord.text || choiceRecord.content || '';
        })
        .filter((value): value is string => typeof value === 'string')
        .join('')
        .trim();

    return choiceText || undefined;
};

const extractJsonObjectText = (value: string): string | undefined => {
    const trimmed = value.trim();
    if (!trimmed) return undefined;
    if (trimmed.startsWith('{') && trimmed.endsWith('}')) return trimmed;

    const fencedJson = trimmed.match(/```(?:json)?\s*([\s\S]*?)\s*```/i);
    if (fencedJson?.[1]) return fencedJson[1].trim();

    const firstBrace = trimmed.indexOf('{');
    const lastBrace = trimmed.lastIndexOf('}');
    if (firstBrace >= 0 && lastBrace > firstBrace) {
        return trimmed.slice(firstBrace, lastBrace + 1);
    }

    return undefined;
};

const normalizeVisionDominantColors = (value: unknown): VisionDominantColor[] | undefined => {
    if (!Array.isArray(value)) return undefined;

    const colors = value
        .map((item) => {
            const record = asRecord(item);
            const name = typeof record.name === 'string'
                ? record.name.trim()
                : typeof record.key === 'string'
                    ? record.key.trim()
                    : '';
            const hex = typeof record.hex === 'string' && /^#[0-9a-f]{6}$/i.test(record.hex.trim())
                ? record.hex.trim().toUpperCase()
                : undefined;

            return name ? { name, ...(hex ? { hex } : {}) } : undefined;
        })
        .filter((color): color is VisionDominantColor => Boolean(color));

    return colors.length > 0 ? colors : undefined;
};

const parseVisionAnalysis = (content: string | undefined): VisionAnalysis | undefined => {
    if (!content?.trim()) return undefined;

    const jsonText = extractJsonObjectText(content);
    if (jsonText) {
        try {
            const parsed = JSON.parse(jsonText) as Record<string, unknown>;
            const summary = typeof parsed.summary === 'string' && parsed.summary.trim()
                ? parsed.summary.trim()
                : undefined;
            const dominantColors = normalizeVisionDominantColors(parsed.dominantColors);

            if (summary || dominantColors) {
                return {
                    ...(summary ? { summary } : {}),
                    ...(dominantColors ? { dominantColors } : {})
                };
            }
        } catch {
            // Truncated JSON should trigger the plain-text retry instead of being stored as summary text.
            return undefined;
        }
    }

    return { summary: content.trim() };
};

const extractVisionDominantColorsWithTextModel = async (
    visionText: string,
    userMessage: string
): Promise<Pick<VisionAnalysis, 'dominantColors'> | undefined> => {
    if (!visionText.trim()) return undefined;

    const response = await fetch(`${OLLAMA_URL}/api/chat`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
            model: DEFAULT_CHAT_MODEL,
            stream: false,
            options: {
                temperature: 0,
                num_predict: 256
            },
            messages: [
                {
                    role: 'system',
                    content: [
                        'Convert vision text into compact valid JSON only.',
                        'Schema: {"dominantColors":[{"name":"color name","hex":"#RRGGBB"}]}',
                        'dominantColors must be inferred only from the provided vision text.',
                        'If hex is not explicitly present, infer common hex values for simple color names.',
                        'Do not include markdown or extra text.'
                    ].join('\n')
                },
                {
                    role: 'user',
                    content: JSON.stringify({
                        userQuestion: userMessage || undefined,
                        visionText
                    })
                }
            ]
        })
    });

    if (!response.ok) {
        const errorText = await response.text().catch(() => '');
        throw new Error(`vision structuring request failed: ${response.status} ${response.statusText}${errorText ? ` - ${errorText}` : ''}`);
    }

    const payload = await response.json();
    const structured = parseVisionAnalysis(extractOllamaTextContent(payload));
    return structured?.dominantColors ? { dominantColors: structured.dominantColors } : undefined;
};

const summarizeVisionThinkingWithTextModel = async (
    thinkingText: string,
    userMessage: string
): Promise<string | undefined> => {
    if (!thinkingText.trim()) return undefined;

    const response = await fetch(`${OLLAMA_URL}/api/chat`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
            model: DEFAULT_CHAT_MODEL,
            stream: false,
            options: {
                temperature: 0,
                num_predict: 128
            },
            messages: [
                {
                    role: 'system',
                    content: [
                        'Convert the vision model notes into one concise visible-image summary.',
                        'Keep only final visible facts: objects, readable text, and color names.',
                        'Do not include uncertainty, reasoning steps, or markdown.'
                    ].join('\n')
                },
                {
                    role: 'user',
                    content: JSON.stringify({
                        userQuestion: userMessage || undefined,
                        visionNotes: thinkingText
                    })
                }
            ]
        })
    });

    if (!response.ok) return undefined;

    return extractOllamaTextContent(await response.json());
};

const analyzeImagesWithVisionModel = async (
    images: ChatImageAttachment[],
    userMessage: string,
    visionModel: string
): Promise<VisionAnalysis | undefined> => {
    if (images.length === 0) return undefined;

    const requestVisionText = async (prompt: string, outputTokens = VISION_OUTPUT_TOKENS): Promise<{
        text?: string;
        thinking?: string;
    }> => {
        const abortController = new AbortController();
        const timeout = setTimeout(() => abortController.abort(), VISION_REQUEST_TIMEOUT_MS);

        const response = await fetch(`${OLLAMA_URL}/api/chat`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                model: visionModel,
                stream: false,
                think: false,
                options: {
                    temperature: 0.1,
                    top_p: 0.8,
                    num_predict: outputTokens
                },
                messages: [
                    {
                        role: 'user',
                        content: prompt,
                        images: images.map((image) => image.base64)
                    }
                ]
            }),
            signal: abortController.signal
        }).finally(() => clearTimeout(timeout));

        if (!response.ok) {
            const errorText = await response.text().catch(() => '');
            throw new Error(`vision model "${visionModel}" request failed: ${response.status} ${response.statusText}${errorText ? ` - ${errorText}` : ''}`);
        }

        const payload = await response.json();
        const text = extractOllamaTextContent(payload);
        const messageThinking = asRecord(asRecord(payload).message).thinking;
        const rootThinking = asRecord(payload).thinking;
        const thinking = typeof messageThinking === 'string' && messageThinking.trim()
            ? messageThinking.trim()
            : typeof rootThinking === 'string' && rootThinking.trim()
                ? rootThinking.trim()
                : undefined;
        return { text, thinking };
    };

    const textPrompt = [
        '/no_think',
        'Describe the attached image in one short paragraph.',
        'Mention the most visible colors by name.',
        'Mention any readable text if visible.',
        'Do not return JSON. Do not return an empty response.',
        `Question: ${userMessage || '(image only)'}`
    ].join('\n');

    const visionResult = await requestVisionText(textPrompt);
    const fallbackVisionResult = visionResult.text?.trim()
        ? visionResult
        : await requestVisionText('/no_think\nLook at the image and write one plain English sentence describing what is visible, including the main colors. Do not output JSON.', 512);
    const summary = fallbackVisionResult.text?.trim()
        || await summarizeVisionThinkingWithTextModel(
            fallbackVisionResult.thinking || visionResult.thinking || '',
            userMessage
        );
    return summary
        ? {
            summary,
            dominantColorsStatus: 'pending'
        }
        : undefined;
};

// const toPrismaJsonObject = (value: unknown): Prisma.InputJsonObject => {
//     return JSON.parse(JSON.stringify(value)) as Prisma.InputJsonObject;
// };

const toPrismaJsonObject = (value: unknown): Prisma.InputJsonObject => {
    return JSON.parse(JSON.stringify(value)) as Prisma.InputJsonObject;
};

const parseToolArguments = (rawArguments: unknown): Record<string, unknown> => {
    if (!rawArguments) return {};
    if (typeof rawArguments === 'object') {
        return rawArguments as Record<string, unknown>;
    }
    if (typeof rawArguments !== 'string') {
        return {};
    }

    try {
        return JSON.parse(rawArguments) as Record<string, unknown>;
    } catch {
        return {};
    }
};

const asRecord = (value: unknown): Record<string, unknown> => {
    return value && typeof value === 'object' && !Array.isArray(value)
        ? value as Record<string, unknown>
        : {};
};

type ConversationRollingSummary = Record<string, unknown>;

let ensureConversationMemorySummaryColumnsPromise: Promise<void> | undefined;

const ensureConversationMemorySummaryColumns = async () => {
    ensureConversationMemorySummaryColumnsPromise ??= prisma.$executeRaw`
        ALTER TABLE "conversations"
        ADD COLUMN IF NOT EXISTS "memory_summary" JSONB,
        ADD COLUMN IF NOT EXISTS "memory_summary_updated_at" TIMESTAMP(6)
    `.then(() => undefined);

    return ensureConversationMemorySummaryColumnsPromise;
};

const truncateSummaryText = (value: unknown, limit = 4000): string | undefined => {
    if (typeof value !== 'string') return undefined;
    const trimmed = value.trim();
    if (!trimmed) return undefined;
    return trimmed.length > limit ? `${trimmed.slice(0, limit)}...` : trimmed;
};

const compactSummaryMetadata = (metadata: unknown): Record<string, unknown> | undefined => {
    const record = asRecord(metadata);
    if (Object.keys(record).length === 0) return undefined;

    const layerRecord = asRecord(record.layer);
    const mapStyleRecord = asRecord(record.mapStyle);
    const payloadRecord = asRecord(record.payload);
    const compact = {
        event: record.event,
        title: record.title || layerRecord.title || payloadRecord.title,
        layerId: record.layerId || layerRecord.layerId || mapStyleRecord.layerId || payloadRecord.layerId,
        type: record.type || layerRecord.type || payloadRecord.type,
        styleKey: record.styleKey || mapStyleRecord.styleKey,
        activeStyle: record.activeStyle || mapStyleRecord.activeStyle,
        geometryType: record.geometryType || layerRecord.geometryType || mapStyleRecord.geometryType
    };

    return Object.fromEntries(
        Object.entries(compact).filter(([, value]) => value !== undefined && value !== null && value !== '')
    );
};

const parseJsonObjectFromText = (value: string): Record<string, unknown> | undefined => {
    const trimmed = value.trim()
        .replace(/^```(?:json)?/i, '')
        .replace(/```$/i, '')
        .trim();

    try {
        const parsed = JSON.parse(trimmed);
        return asRecord(parsed);
    } catch {
        const start = trimmed.indexOf('{');
        const end = trimmed.lastIndexOf('}');
        if (start < 0 || end <= start) return undefined;

        try {
            return asRecord(JSON.parse(trimmed.slice(start, end + 1)));
        } catch {
            return undefined;
        }
    }
};

const getConversationRollingSummary = async (
    conversationId: string
): Promise<ConversationRollingSummary | undefined> => {
    await ensureConversationMemorySummaryColumns();

    const rows = await prisma.$queryRaw<Array<{ memorySummary: unknown }>>`
        SELECT "memory_summary" AS "memorySummary"
        FROM "conversations"
        WHERE "id" = ${conversationId}
            AND "is_deleted" = false
        LIMIT 1
    `;
    const summary = asRecord(rows[0]?.memorySummary);

    return Object.keys(summary).length > 0 ? summary : undefined;
};

const buildConversationSummaryContext = (
    summary?: ConversationRollingSummary
): { role: 'system'; content: string } | undefined => {
    if (!summary || Object.keys(summary).length === 0) return undefined;

    // Memory helps the model keep conversation continuity, but map tools must still
    // trust structured DB state when they need the current layer/style payload.
    return {
        role: 'system',
        content: [
            'Rolling summary for this conversation is available below.',
            'Use it as durable context for goals, decisions, preferences, and open tasks from earlier turns.',
            'Do not mention this summary unless the user asks how memory works.',
            `Rolling summary JSON:\n${JSON.stringify(summary)}`
        ].join('\n')
    };
};

const updateConversationRollingSummary = async ({
    conversationId,
    previousSummary,
    latestUserMessage,
    latestAssistantMessage,
    latestUserMetadata,
    latestAssistantMetadata
}: {
    conversationId: string;
    previousSummary?: ConversationRollingSummary;
    latestUserMessage?: string;
    latestAssistantMessage?: string;
    latestUserMetadata?: unknown;
    latestAssistantMetadata?: unknown;
}) => {
    if (!MEMORY_SUMMARY_MODEL) return;

    const response = await fetch(`${OLLAMA_URL}/api/chat`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
            model: MEMORY_SUMMARY_MODEL,
            stream: false,
            messages: [
                {
                    role: 'system',
                    content: [
                        'You are a rolling memory summarizer for one chat conversation.',
                        'Update the previous summary using only durable information from the latest turn.',
                        'Keep technical decisions, user goals, preferences, open tasks, and important map/API context.',
                        'Do not copy large payloads, code dumps, raw layer/style JSON, or small talk.',
                        'Return valid compact JSON only with keys: overview, decisions, openTasks, preferences, notes.',
                        'Keep arrays short and remove stale/duplicated items.'
                    ].join('\n')
                },
                {
                    role: 'user',
                    content: JSON.stringify({
                        previousSummary: previousSummary || {},
                        latestTurn: {
                            user: truncateSummaryText(latestUserMessage),
                            assistant: truncateSummaryText(latestAssistantMessage),
                            userMetadata: compactSummaryMetadata(latestUserMetadata),
                            assistantMetadata: compactSummaryMetadata(latestAssistantMetadata)
                        }
                    })
                }
            ],
            options: {
                temperature: 0.1,
                top_p: 0.8,
                num_predict: 700
            }
        })
    });

    if (!response.ok) {
        throw new Error(`memory_summary_model_failed:${response.status}`);
    }

    const payload = await response.json();
    const content = typeof payload?.message?.content === 'string'
        ? payload.message.content
        : '';
    const nextSummary = parseJsonObjectFromText(content);
    if (!nextSummary || Object.keys(nextSummary).length === 0) return;

    await ensureConversationMemorySummaryColumns();
    await prisma.$executeRaw`
        UPDATE "conversations"
        SET
            "memory_summary" = CAST(${JSON.stringify(nextSummary)} AS jsonb),
            "memory_summary_updated_at" = CURRENT_TIMESTAMP,
            "updated_at" = CURRENT_TIMESTAMP
        WHERE "id" = ${conversationId}
            AND "is_deleted" = false
    `;
};

const scheduleConversationSummaryUpdate = ({
    conversationId,
    latestUserMessage,
    latestAssistantMessage,
    latestUserMetadata,
    latestAssistantMetadata
}: {
    conversationId: string;
    latestUserMessage?: string;
    latestAssistantMessage?: string;
    latestUserMetadata?: unknown;
    latestAssistantMetadata?: unknown;
}) => {
    if (!MEMORY_SUMMARY_MODEL) return;

    void (async () => {
        try {
            const previousSummary = await getConversationRollingSummary(conversationId);
            await updateConversationRollingSummary({
                conversationId,
                previousSummary,
                latestUserMessage,
                latestAssistantMessage,
                latestUserMetadata,
                latestAssistantMetadata
            });
        } catch (error) {
            console.error('[memory-summary] failed to update conversation summary:', error);
        }
    })();
};

const mergeVisionAnalysisIntoMetadata = (
    metadata: unknown,
    analysis: VisionAnalysis
): Prisma.InputJsonObject => {
    const metadataRecord = asRecord(metadata);
    const vision = asRecord(metadataRecord.vision);

    return toPrismaJsonObject({
        ...metadataRecord,
        vision: {
            ...vision,
            ...(analysis.summary ? { summary: analysis.summary } : {}),
            ...(analysis.dominantColors ? { dominantColors: analysis.dominantColors } : {}),
            ...(analysis.dominantColorsStatus ? { dominantColorsStatus: analysis.dominantColorsStatus } : {})
        }
    });
};

const updateUserMessageVisionAnalysis = async ({
    userId,
    conversationId,
    messageId,
    content,
    analysis
}: {
    userId: string;
    conversationId: string;
    messageId: string;
    content: string;
    analysis: VisionAnalysis;
}): Promise<Prisma.InputJsonObject | undefined> => {
    const message = await prisma.messages.findFirst({
        where: {
            id: messageId,
            conversation_id: conversationId,
            role: 'user',
            deleted_at: null,
            conversations: {
                user_id: userId,
                is_deleted: false
            }
        },
        select: {
            metadata: true,
            created_at: true
        }
    });

    if (!message) return undefined;

    const metadata = mergeVisionAnalysisIntoMetadata(message.metadata, analysis);
    await prisma.messages.update({
        where: { id: messageId },
        data: { metadata }
    });
    await saveConversationMemoryChunks({
        userId,
        message: {
            id: messageId,
            conversation_id: conversationId,
            role: 'user',
            content,
            metadata,
            created_at: message.created_at
        }
    });

    return metadata;
};

const getToolErrorMessage = (value: unknown, fallback: string): string => {
    const record = asRecord(value);
    const error = record.error;
    if (typeof error === 'string' && error.trim()) return error;

    const message = record.message;
    return typeof message === 'string' && message.trim()
        ? message
        : fallback;
};

const getClearLayerIds = (metadata: Record<string, unknown>): string[] => {
    const layerIds = Array.isArray(metadata.layerIds)
        ? metadata.layerIds.filter((item): item is string => typeof item === 'string' && Boolean(item.trim()))
        : [];

    return Array.from(new Set([
        ...layerIds,
        ...(typeof metadata.layerId === 'string' && metadata.layerId.trim() ? [metadata.layerId] : [])
    ]));
};

type ConversationMapLayerState = {
    layer?: unknown;
    mapPayload?: unknown;
    styles: Record<string, unknown>;
    activeStyle?: string;
    latestMapStyle?: unknown;
};

type ConversationMapState = {
    layers: Record<string, ConversationMapLayerState>;
    activeLayerId?: string;
};

const toClearLayerIdList = (...values: unknown[]): string[] => {
    const layerIds = values.flatMap((value) => {
        if (Array.isArray(value)) {
            return value.filter((item): item is string => typeof item === 'string' && Boolean(item.trim()));
        }

        if (typeof value !== 'string' || !value.trim()) return [];
        return value
            .split(/[\s,]+/g)
            .map((item) => item.trim())
            .filter((item) => item.length >= 3);
    });

    return Array.from(new Set(layerIds));
};

const getMapStyleKey = (mapStyle: unknown): string | undefined => {
    const record = asRecord(mapStyle);
    const directStyleKey = typeof record.styleKey === 'string'
        ? record.styleKey
        : typeof record.activeStyle === 'string'
            ? record.activeStyle
            : typeof record.preset === 'string'
                ? record.preset
                : undefined;
    if (directStyleKey?.trim()) return directStyleKey.trim();

    const layers = Array.isArray(record.layers) ? record.layers : [];
    const firstLayerType = asRecord(layers[0]).type;
    return typeof firstLayerType === 'string' && firstLayerType.trim()
        ? firstLayerType.trim()
        : undefined;
};

const getMapStyleLayerId = (mapStyle: unknown, fallbackLayerId?: string): string | undefined => {
    const layerId = asRecord(mapStyle).layerId;
    if (typeof layerId === 'string' && layerId.trim()) return layerId.trim();

    const styleId = asRecord(mapStyle).styleId;
    if (typeof styleId === 'string' && styleId.trim()) return styleId.trim();

    return fallbackLayerId;
};

const getLayerIdFromMapPayload = (mapPayload: unknown): string | undefined => {
    const payloadRecord = asRecord(mapPayload);
    const layerRecord = asRecord(payloadRecord.layer);
    const layerId = layerRecord.layerId
        || layerRecord.styleId
        || layerRecord.id
        || payloadRecord.layerId
        || payloadRecord.styleId
        || payloadRecord.id;
    return typeof layerId === 'string' && layerId.trim()
        ? layerId.trim()
        : undefined;
};

const getLayerRecordFromMapPayload = (mapPayload: unknown): unknown | undefined => {
    const payloadRecord = asRecord(mapPayload);
    return Object.keys(asRecord(payloadRecord.layer)).length > 0
        ? payloadRecord.layer
        : undefined;
};

type ConversationMapLayerRow = {
    id: string;
    layerKey: string;
    title: string | null;
    type: string | null;
    order: number;
    visible: boolean;
    layerPayload: unknown;
    mapStyle: unknown | null;
    activeStyle: string | null;
    createdAt: Date;
    updatedAt: Date;
};

type MapLayerOrderPayload = {
    layerIds: string[];
};

let ensureConversationMapLayersTablePromise: Promise<void> | undefined;

const ensureConversationMapLayersTable = async () => {
    ensureConversationMapLayersTablePromise ??= (async () => {
        await prisma.$executeRaw`
            CREATE TABLE IF NOT EXISTS "conversation_map_layers" (
                "id" TEXT NOT NULL,
                "conversation_id" TEXT NOT NULL,
                "layer_key" TEXT NOT NULL,
                "title" TEXT,
                "type" TEXT,
                "order" INTEGER NOT NULL DEFAULT 0,
                "visible" BOOLEAN NOT NULL DEFAULT true,
                "layer_payload" JSONB NOT NULL,
                "map_style" JSONB,
                "active_style" TEXT,
                "deleted_at" TIMESTAMP(6),
                "created_at" TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
                "updated_at" TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
                CONSTRAINT "conversation_map_layers_pkey" PRIMARY KEY ("id")
            )
        `;
        await prisma.$executeRaw`
            CREATE UNIQUE INDEX IF NOT EXISTS "conversation_map_layers_conversation_layer_key"
            ON "conversation_map_layers"("conversation_id", "layer_key")
        `;
        await prisma.$executeRaw`
            CREATE INDEX IF NOT EXISTS "idx_conversation_map_layers_conversation_order"
            ON "conversation_map_layers"("conversation_id", "order")
        `;
        await prisma.$executeRaw`
            CREATE INDEX IF NOT EXISTS "idx_conversation_map_layers_conversation_deleted"
            ON "conversation_map_layers"("conversation_id", "deleted_at")
        `;
        await prisma.$executeRawUnsafe(`
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1
                    FROM pg_constraint
                    WHERE conname = 'fk_conversation_map_layers_conversation'
                ) THEN
                    ALTER TABLE "conversation_map_layers"
                    ADD CONSTRAINT "fk_conversation_map_layers_conversation"
                    FOREIGN KEY ("conversation_id") REFERENCES "conversations"("id")
                    ON DELETE CASCADE ON UPDATE NO ACTION;
                END IF;
            END $$;
        `);
    })();

    return ensureConversationMapLayersTablePromise;
};

const getMapLayerTitle = (mapPayload: unknown): string | undefined => {
    const payloadRecord = asRecord(mapPayload);
    const layerRecord = asRecord(payloadRecord.layer);
    const title = layerRecord.title
        || layerRecord.styleTitle
        || layerRecord.name
        || payloadRecord.title
        || payloadRecord.styleTitle
        || payloadRecord.name;

    return typeof title === 'string' && title.trim() ? title.trim() : undefined;
};

const getMapLayerType = (mapPayload: unknown): string | undefined => {
    const payloadRecord = asRecord(mapPayload);
    const layerRecord = asRecord(payloadRecord.layer);
    const type = layerRecord.type || payloadRecord.type;

    return typeof type === 'string' && type.trim() ? type.trim() : undefined;
};

type ConversationMapAttributeField = {
    type: string;
};

const normalizeMapAttributeFields = (fields: unknown): Record<string, ConversationMapAttributeField> | undefined => {
    if (!fields) return undefined;

    const entries = new Map<string, ConversationMapAttributeField>();
    const addField = (name: unknown, type: unknown) => {
        const fieldName = typeof name === 'string' && name.trim() ? name.trim() : undefined;
        const fieldType = typeof type === 'string' && type.trim() ? type.trim() : undefined;
        if (!fieldName || !fieldType) return;
        entries.set(fieldName, { type: fieldType });
    };

    if (Array.isArray(fields)) {
        for (const field of fields) {
            const record = asRecord(field);
            addField(
                record.name || record.key || record.id || record.field,
                record.type || record.dataType || record.fieldType
            );
        }
    } else {
        const record = asRecord(fields);
        for (const [fieldName, fieldDefinition] of Object.entries(record)) {
            const definition = asRecord(fieldDefinition);
            addField(fieldName, typeof fieldDefinition === 'string' ? fieldDefinition : definition.type || definition.dataType || definition.fieldType);
        }
    }

    return entries.size > 0 ? Object.fromEntries(entries) : undefined;
};

const findMapAttributeFields = (payload: unknown, depth = 0): Record<string, ConversationMapAttributeField> | undefined => {
    if (!payload || depth > 5) return undefined;

    if (Array.isArray(payload)) {
        for (const item of payload) {
            const fields = findMapAttributeFields(item, depth + 1);
            if (fields) return fields;
        }
        return undefined;
    }

    const record = asRecord(payload);
    if (Object.keys(record).length === 0) return undefined;

    const attributes = asRecord(record.attributes);
    const directFields = normalizeMapAttributeFields(attributes.fields)
        || normalizeMapAttributeFields(record.fields)
        || normalizeMapAttributeFields(asRecord(record.schema).fields)
        || normalizeMapAttributeFields(asRecord(record.metadata).fields);
    if (directFields) return directFields;

    for (const value of Object.values(record)) {
        const fields = findMapAttributeFields(value, depth + 1);
        if (fields) return fields;
    }

    return undefined;
};

const withMapLayerAttributeFields = (mapPayload: unknown): unknown => {
    const payloadRecord = asRecord(mapPayload);
    const layerRecord = asRecord(payloadRecord.layer);
    if (Object.keys(layerRecord).length === 0) return mapPayload;

    const existingFields = normalizeMapAttributeFields(asRecord(layerRecord.attributes).fields);
    const fields = existingFields || findMapAttributeFields(mapPayload);
    if (!fields) return mapPayload;

    return {
        ...payloadRecord,
        layer: {
            ...layerRecord,
            attributes: {
                ...asRecord(layerRecord.attributes),
                fields
            }
        }
    };
};

const normalizeLayerTitle = (value: string): string => {
    return value.trim().replace(/\s+/g, ' ').toLowerCase();
};

const toTrimmedString = (value: unknown): string | undefined => {
    return typeof value === 'string' && value.trim() ? value.trim() : undefined;
};

const getRowTitleCandidates = (row: ConversationMapLayerRow): string[] => {
    return [
        row.title,
        getMapLayerTitle(row.layerPayload)
    ].filter((value): value is string => typeof value === 'string' && Boolean(value.trim()));
};

const resolveMapLayerOrderIds = (
    rows: ConversationMapLayerRow[],
    payload: MapLayerOrderPayload
): string[] => {
    const activeLayerKeys = new Set(rows.map((row) => row.layerKey));
    const titleToLayerKey = new Map<string, string>();

    for (const row of rows) {
        for (const title of getRowTitleCandidates(row)) {
            const normalizedTitle = normalizeLayerTitle(title);
            if (!titleToLayerKey.has(normalizedTitle)) {
                titleToLayerKey.set(normalizedTitle, row.layerKey);
            }
        }
    }

    const orderedLayerIds: string[] = [];
    const seenLayerIds = new Set<string>();

    for (const item of payload.layerIds) {
        const candidate = toTrimmedString(item);
        if (!candidate) continue;

        const layerId = activeLayerKeys.has(candidate)
            ? candidate
            : titleToLayerKey.get(normalizeLayerTitle(candidate));

        if (layerId && !seenLayerIds.has(layerId)) {
            orderedLayerIds.push(layerId);
            seenLayerIds.add(layerId);
        }
    }

    return orderedLayerIds;
};

const toCompactConversationMapLayer = (row: ConversationMapLayerRow) => ({
    id: row.layerKey,
    layerKey: row.layerKey,
    title: row.title,
    type: row.type,
    order: row.order,
    visible: row.visible,
    activeStyle: row.activeStyle,
    createdAt: row.createdAt,
    updatedAt: row.updatedAt
});

const fetchConversationMapLayerRows = async (
    conversationId: string
): Promise<ConversationMapLayerRow[]> => {
    await ensureConversationMapLayersTable();

    return prisma.$queryRaw<ConversationMapLayerRow[]>`
        SELECT
            "id",
            "layer_key" AS "layerKey",
            "title",
            "type",
            "order",
            "visible",
            "layer_payload" AS "layerPayload",
            "map_style" AS "mapStyle",
            "active_style" AS "activeStyle",
            "created_at" AS "createdAt",
            "updated_at" AS "updatedAt"
        FROM "conversation_map_layers"
        WHERE "conversation_id" = ${conversationId}
            AND "deleted_at" IS NULL
        ORDER BY "order" ASC, "created_at" ASC, "id" ASC
    `;
};

const rebuildConversationMapLayersFromMessages = async (
    conversationId: string
) => {
    await ensureConversationMapLayersTable();

    await prisma.$executeRaw`
        DELETE FROM "conversation_map_layers"
        WHERE "conversation_id" = ${conversationId}
    `;

    const messages = await prisma.messages.findMany({
        where: {
            conversation_id: conversationId,
            deleted_at: null,
            metadata: { not: Prisma.JsonNull }
        },
        orderBy: [{ created_at: 'asc' }, { id: 'asc' }],
        select: { metadata: true }
    });

    for (const message of messages) {
        const metadata = asRecord(message.metadata);
        if (metadata.event === 'map_clear') {
            await syncConversationMapClear(conversationId, metadata);
            continue;
        }

        if (metadata.event === 'layer_catalog' && metadata.layer) {
            await syncConversationMapLayerCatalog(conversationId, metadata, metadata.mapStyle);
            continue;
        }

        if (metadata.event === 'map_style') {
            await syncConversationMapStyle(conversationId, metadata);
            continue;
        }

        if (metadata.mapStyle) {
            await syncConversationMapLayerCatalog(conversationId, metadata, metadata.mapStyle);
        }
    }
};

const getConversationMapLayerRows = async (
    conversationId: string,
    rebuildIfEmpty = true
): Promise<ConversationMapLayerRow[]> => {
    const rows = await fetchConversationMapLayerRows(conversationId);
    if (rows.length > 0 || !rebuildIfEmpty) return rows;

    await rebuildConversationMapLayersFromMessages(conversationId);
    return fetchConversationMapLayerRows(conversationId);
};

const toPublicConversationMapLayer = (row: ConversationMapLayerRow) => ({
    ...toCompactConversationMapLayer(row),
    id: row.layerKey,
    layerKey: row.layerKey,
    layer: row.layerPayload,
    mapStyle: row.mapStyle
});



const verifyConversationAccess = async (
    userId: string,
    conversationId: string
) => {
    const conversation = await prisma.conversations.findFirst({
        where: {
            id: conversationId,
            user_id: userId,
            is_deleted: false
        },
        select: { id: true }
    });

    if (!conversation) {
        throw Errors.badRequest('Conversation not found or you do not have permission to access it.');
    }
};

const syncConversationMapLayerCatalog = async (
    conversationId: string,
    mapPayload: unknown,
    mapStyle?: unknown
) => {
    await ensureConversationMapLayersTable();

    const layerKey = getLayerIdFromMapPayload(mapPayload);
    if (!layerKey) return;

    const normalizedMapPayload = withMapLayerAttributeFields(mapPayload);
    const title = getMapLayerTitle(normalizedMapPayload);
    const type = getMapLayerType(normalizedMapPayload);
    const activeStyle = mapStyle ? getMapStyleKey(mapStyle) : undefined;
    const layerPayloadJson = JSON.stringify(normalizedMapPayload);
    const mapStyleJson = mapStyle ? JSON.stringify(mapStyle) : null;

    await prisma.$executeRaw`
        INSERT INTO "conversation_map_layers" (
            "id",
            "conversation_id",
            "layer_key",
            "title",
            "type",
            "order",
            "visible",
            "layer_payload",
            "map_style",
            "active_style",
            "deleted_at",
            "created_at",
            "updated_at"
        )
        VALUES (
            ${ulid()},
            ${conversationId},
            ${layerKey},
            ${title ?? null},
            ${type ?? null},
            COALESCE((
                SELECT MAX("order") + 1
                FROM "conversation_map_layers"
                WHERE "conversation_id" = ${conversationId}
                    AND "deleted_at" IS NULL
            ), 0),
            true,
            CAST(${layerPayloadJson} AS jsonb),
            ${mapStyleJson ? Prisma.sql`CAST(${mapStyleJson} AS jsonb)` : Prisma.sql`NULL`},
            ${activeStyle ?? null},
            NULL,
            CURRENT_TIMESTAMP,
            CURRENT_TIMESTAMP
        )
        ON CONFLICT ("conversation_id", "layer_key")
        DO UPDATE SET
            "title" = COALESCE(EXCLUDED."title", "conversation_map_layers"."title"),
            "type" = COALESCE(EXCLUDED."type", "conversation_map_layers"."type"),
            "visible" = true,
            "layer_payload" = EXCLUDED."layer_payload",
            "map_style" = COALESCE(EXCLUDED."map_style", "conversation_map_layers"."map_style"),
            "active_style" = COALESCE(EXCLUDED."active_style", "conversation_map_layers"."active_style"),
            "deleted_at" = NULL,
            "updated_at" = CURRENT_TIMESTAMP
    `;
};

const syncConversationMapStyle = async (
    conversationId: string,
    mapStyle: unknown
) => {
    await ensureConversationMapLayersTable();

    const layerKey = getMapStyleLayerId(mapStyle);
    if (!layerKey) return;

    const activeStyle = getMapStyleKey(mapStyle);
    const mapStyleJson = JSON.stringify(mapStyle);

    await prisma.$executeRaw`
        UPDATE "conversation_map_layers"
        SET
            "map_style" = CAST(${mapStyleJson} AS jsonb),
            "active_style" = ${activeStyle ?? null},
            "visible" = true,
            "deleted_at" = NULL,
            "updated_at" = CURRENT_TIMESTAMP
        WHERE "conversation_id" = ${conversationId}
            AND "layer_key" = ${layerKey}
    `;
};

const syncConversationMapClear = async (
    conversationId: string,
    payload: unknown
) => {
    await ensureConversationMapLayersTable();

    const record = asRecord(payload);
    const mode = record.mode;

    if (mode === 'all') {
        await prisma.$executeRaw`
            UPDATE "conversation_map_layers"
            SET
                "visible" = false,
                "deleted_at" = CURRENT_TIMESTAMP,
                "updated_at" = CURRENT_TIMESTAMP
            WHERE "conversation_id" = ${conversationId}
                AND "deleted_at" IS NULL
        `;
        return;
    }

    if (mode !== 'selected') return;

    const layerIds = getClearLayerIds(record);
    if (layerIds.length === 0) return;

    await prisma.$executeRaw`
        UPDATE "conversation_map_layers"
        SET
            "visible" = false,
            "deleted_at" = CURRENT_TIMESTAMP,
            "updated_at" = CURRENT_TIMESTAMP
        WHERE "conversation_id" = ${conversationId}
            AND "layer_key" IN (${Prisma.join(layerIds)})
            AND "deleted_at" IS NULL
    `;
};

const safeSyncConversationMapLayerCatalog = async (
    conversationId: string,
    mapPayload: unknown,
    mapStyle?: unknown
) => {
    try {
        await syncConversationMapLayerCatalog(conversationId, mapPayload, mapStyle);
    } catch (error) {
        console.error('[map-state] failed to sync layer catalog:', error);
    }
};

const safeSyncConversationMapStyle = async (
    conversationId: string,
    mapStyle: unknown
) => {
    try {
        await syncConversationMapStyle(conversationId, mapStyle);
    } catch (error) {
        console.error('[map-state] failed to sync map style:', error);
    }
};

const safeSyncConversationMapClear = async (
    conversationId: string,
    payload: unknown
) => {
    try {
        await syncConversationMapClear(conversationId, payload);
    } catch (error) {
        console.error('[map-state] failed to sync map clear:', error);
    }
};

const ensureMapLayerState = (
    state: ConversationMapState,
    layerId: string
): ConversationMapLayerState => {
    state.layers[layerId] ??= { styles: {} };
    return state.layers[layerId];
};

const applyMapPayloadToState = (
    state: ConversationMapState,
    mapPayload: unknown
) => {
    const layerId = getLayerIdFromMapPayload(mapPayload);
    if (!layerId) return;

    const layerState = ensureMapLayerState(state, layerId);
    const layer = getLayerRecordFromMapPayload(mapPayload);
    layerState.mapPayload = mapPayload;
    if (layer) {
        layerState.layer = layer;
    }
    state.activeLayerId = layerId;

    const mapStyle = asRecord(mapPayload).mapStyle;
    if (mapStyle) {
        applyMapStyleToState(state, mapStyle, layerId);
    }
};

const applyMapStyleToState = (
    state: ConversationMapState,
    mapStyle: unknown,
    fallbackLayerId?: string
) => {
    const layerId = getMapStyleLayerId(mapStyle, fallbackLayerId);
    const styleKey = getMapStyleKey(mapStyle);
    if (!layerId || !styleKey) return;

    const layerState = ensureMapLayerState(state, layerId);
    layerState.styles[styleKey] = mapStyle;
    layerState.activeStyle = styleKey;
    layerState.latestMapStyle = mapStyle;
    state.activeLayerId = layerId;
};

const getLatestLayerState = (state?: unknown): ConversationMapLayerState | undefined => {
    const mapState = state as ConversationMapState | undefined;
    if (!mapState?.layers) return undefined;

    if (mapState.activeLayerId && mapState.layers[mapState.activeLayerId]) {
        return mapState.layers[mapState.activeLayerId];
    }

    const layerIds = Object.keys(mapState.layers);
    return layerIds.length > 0 ? mapState.layers[layerIds[layerIds.length - 1]] : undefined;
};

const layerStateHasStyle = (layerState?: ConversationMapLayerState): boolean => {
    if (!layerState) return false;
    if (layerState.latestMapStyle) return true;
    return Boolean(layerState.activeStyle && layerState.styles[layerState.activeStyle]);
};

const getLatestStyledLayerState = (state?: unknown): ConversationMapLayerState | undefined => {
    const mapState = state as ConversationMapState | undefined;
    if (!mapState?.layers) return undefined;

    if (mapState.activeLayerId) {
        const activeLayerState = mapState.layers[mapState.activeLayerId];
        if (layerStateHasStyle(activeLayerState)) return activeLayerState;
    }

    const layerIds = Object.keys(mapState.layers);
    for (let index = layerIds.length - 1; index >= 0; index -= 1) {
        const layerState = mapState.layers[layerIds[index]];
        if (layerStateHasStyle(layerState)) return layerState;
    }

    return getLatestLayerState(mapState);
};

const getLatestMapPayloadFromState = (state?: unknown): unknown | undefined => {
    return getLatestLayerState(state)?.mapPayload;
};

const getLatestMapStyleFromState = (state?: unknown): unknown | undefined => {
    const layerState = getLatestLayerState(state);
    if (!layerState) return undefined;
    if (layerState.activeStyle && layerState.styles[layerState.activeStyle]) {
        return layerState.styles[layerState.activeStyle];
    }
    return layerState.latestMapStyle;
};

const buildConversationMapStateFromMessages = (
    messages: Array<{ role: string; content: string }>
): ConversationMapState => {
    const state: ConversationMapState = { layers: {} };

    for (const message of messages) {
        const metadata = asRecord((message as Record<string, unknown>).metadata);
        if (Object.keys(metadata).length === 0) continue;

        if (metadata.event === 'map_clear') {
            if (metadata.mode === 'all') {
                state.layers = {};
                state.activeLayerId = undefined;
                continue;
            }

            if (metadata.mode === 'selected') {
                for (const layerId of getClearLayerIds(metadata)) {
                    delete state.layers[layerId];
                    if (state.activeLayerId === layerId) {
                        const activeLayerIds = Object.keys(state.layers);
                        state.activeLayerId = activeLayerIds[activeLayerIds.length - 1];
                    }
                }
                continue;
            }
        }

        if (metadata.event === 'layer_catalog' && metadata.layer) {
            applyMapPayloadToState(state, metadata);
            continue;
        }

        if (metadata.event === 'map_style') {
            applyMapStyleToState(state, metadata);
            continue;
        }

        if (metadata.mapStyle) {
            applyMapPayloadToState(state, metadata);
        }
    }

    return state;
};

const buildConversationMapStateFromRows = (
    rows: ConversationMapLayerRow[]
): ConversationMapState => {
    const state: ConversationMapState = { layers: {} };

    for (const row of rows) {
        if (!row.visible) continue;

        if (row.layerPayload) {
            applyMapPayloadToState(state, row.layerPayload);
        } else {
            ensureMapLayerState(state, row.layerKey);
        }

        if (row.mapStyle) {
            applyMapStyleToState(state, row.mapStyle, row.layerKey);
        }
    }

    const latestUpdatedRow = rows
        .filter((row) => row.visible && state.layers[row.layerKey])
        .sort((left, right) => right.updatedAt.getTime() - left.updatedAt.getTime())[0];
    if (latestUpdatedRow) {
        state.activeLayerId = latestUpdatedRow.layerKey;
    }

    return state;
};

const getLatestMapStyleFromMessages = (messages: Array<{ role: string; content: string }>): unknown | undefined => {
    const clearedLayerIds = new Set<string>();

    for (const message of [...messages].reverse()) {
        const metadata = asRecord((message as Record<string, unknown>).metadata);
        if (metadata.event === 'map_clear') {
            const mode = metadata.mode;
            if (mode === 'all') return undefined;
            if (mode === 'selected') {
                for (const layerId of getClearLayerIds(metadata)) {
                    clearedLayerIds.add(layerId);
                }
            }
            continue;
        }

        const mapStyle = metadata.mapStyle || (metadata.event === 'map_style' ? metadata : undefined);
        const mapStyleLayerId = asRecord(mapStyle).layerId;
        if (mapStyle && (typeof mapStyleLayerId !== 'string' || !clearedLayerIds.has(mapStyleLayerId))) {
            return mapStyle;
        }
    }

    return undefined;
};

const getMapStyleFromMessageMetadata = (metadata: unknown): Record<string, unknown> | undefined => {
    const record = asRecord(metadata);
    const mapStyle = record.mapStyle || (record.event === 'map_style' ? record : undefined);
    const mapStyleRecord = asRecord(mapStyle);
    const layers = Array.isArray(mapStyleRecord.layers) ? mapStyleRecord.layers : [];
    return Object.keys(mapStyleRecord).length > 0 && layers.length > 0
        ? mapStyleRecord
        : undefined;
};

const toPublicMapStylePayload = (payload: unknown): unknown => {
    const payloadRecord = asRecord(payload);
    const { attributeStyleVariants, ...publicPayload } = payloadRecord;
    return publicPayload;
};

const getLatestMapPayloadFromMessages = (messages: Array<{ role: string; content: string }>): unknown | undefined => {
    const removedLayerIds = new Set<string>();

    for (const message of [...messages].reverse()) {
        const metadata = asRecord((message as Record<string, unknown>).metadata);
        if (metadata.event === 'map_clear') {
            const mode = metadata.mode;
            if (mode === 'all') return undefined;
            if (mode === 'selected') {
                for (const layerId of getClearLayerIds(metadata)) {
                    removedLayerIds.add(layerId);
                }
            }
            continue;
        }

        if (metadata.event === 'layer_catalog' && metadata.layer) {
            const layerRecord = asRecord(metadata.layer);
            const layerId = layerRecord.layerId || layerRecord.styleId || layerRecord.id;
            if (typeof layerId !== 'string' || !removedLayerIds.has(layerId)) {
                return metadata;
            }
        }
    }

    return undefined;
};

const getLatestVisionFromMessages = (messages: Array<{ role: string; content: string }>): unknown | undefined => {
    for (const message of [...messages].reverse()) {
        const vision = asRecord(asRecord((message as Record<string, unknown>).metadata).vision);
        if (Object.keys(vision).length > 0) {
            return vision;
        }
    }

    return undefined;
};

const getLatestMapClearFromMessages = (messages: Array<{ role: string; content: string }>): unknown | undefined => {
    for (const message of [...messages].reverse()) {
        const metadata = asRecord((message as Record<string, unknown>).metadata);
        if (metadata.event === 'map_clear') {
            return metadata;
        }
    }

    return undefined;
};

const getLatestMapOptionsFromMessages = (messages: Array<{ role: string; content: string }>): unknown | undefined => {
    for (const message of [...messages].reverse()) {
        const metadata = asRecord((message as Record<string, unknown>).metadata);
        if (metadata.event === 'map_options') {
            return metadata.payload || metadata;
        }
    }

    return undefined;
};

const buildConversationMemoryFromDb = async (
    conversationId: string
): Promise<Record<string, unknown>> => {
    const dbMessages = await prisma.messages.findMany({
        where: {
            conversation_id: conversationId,
            deleted_at: null
        },
        orderBy: [{ created_at: 'desc' }, { id: 'desc' }],
        take: 80,
        select: {
            role: true,
            content: true,
            metadata: true
        }
    });

    const messages = dbMessages.reverse();
    const latestVision = getLatestVisionFromMessages(messages);
    const messageMapState = buildConversationMapStateFromMessages(messages);
    let mapLayerRows: ConversationMapLayerRow[] = [];
    try {
        mapLayerRows = await getConversationMapLayerRows(conversationId);
    } catch (error) {
        console.error('[map-state] failed to load conversation map layers:', error);
    }
    const dbMapState = buildConversationMapStateFromRows(mapLayerRows);
    const conversationMapState = Object.keys(dbMapState.layers).length > 0
        ? dbMapState
        : messageMapState;
    const latestMap = getLatestMapPayloadFromState(conversationMapState)
        || getLatestMapPayloadFromMessages(messages);
    const latestMapStyle = getLatestMapStyleFromState(conversationMapState)
        || getLatestMapStyleFromMessages(messages);
    const latestMapClear = getLatestMapClearFromMessages(messages);
    const latestMapOptions = getLatestMapOptionsFromMessages(messages);

    return {
        ...(latestVision ? { latestVision } : {}),
        ...(latestMap ? { latestMap } : {}),
        ...(latestMapStyle ? { latestMapStyle } : {}),
        ...(Object.keys(conversationMapState.layers).length > 0 ? { conversationMapState } : {}),
        ...(latestMapClear ? { latestMapClear } : {}),
        ...(latestMapOptions ? { latestMapOptions } : {})
    };
};

const normalizeStyleSwitchText = (value: unknown): string => {
    return typeof value === 'string'
        ? value.toLowerCase().replace(/[\s()[\]{}"'`.,:;|/_-]+/g, '')
        : '';
};

const getLayerTextMatchTerms = (value: unknown): string[] => {
    if (typeof value !== 'string' || !value.trim()) return [];

    const spaced = value
        .replace(/([a-z0-9])([A-Z])/g, '$1 $2')
        .replace(/([A-Za-z])(\d)/g, '$1 $2')
        .replace(/(\d)([A-Za-z])/g, '$1 $2');
    const wholeTerm = normalizeStyleSwitchText(value);
    const tokenTerms = spaced
        .split(/[\s()[\]{}"'`.,:;|/_-]+/g)
        .map(normalizeStyleSwitchText)
        .filter((term) => term.length >= 3);

    return [
        ...(wholeTerm.length >= 3 ? [wholeTerm] : []),
        ...tokenTerms
    ];
};

const resolveRequestedMapStyleKey = (
    message: string,
    styles: unknown[],
    currentMapStyle?: unknown
): string | undefined => {
    const normalizedMessage = normalizeStyleSwitchText(message);
    if (!normalizedMessage) return undefined;

    const currentStyleKey = normalizeStyleSwitchText(asRecord(currentMapStyle).styleKey || asRecord(currentMapStyle).activeStyle);
    const matches = styles
        .map((style) => {
            const record = asRecord(style);
            const styleKey = typeof record.styleKey === 'string'
                ? record.styleKey
                : typeof record.key === 'string'
                    ? record.key
                    : undefined;
            if (!styleKey) return undefined;

            const terms = [
                record.styleKey,
                record.key,
                record.styleName,
                record.description,
                record.layerType
            ]
                .map(normalizeStyleSwitchText)
                .filter((term, index, allTerms) => term.length >= 3 && allTerms.indexOf(term) === index);
            const matchedTerm = terms.find((term) => normalizedMessage.includes(term));
            if (!matchedTerm) return undefined;

            return {
                styleKey,
                score: matchedTerm.length,
                isCurrent: normalizeStyleSwitchText(styleKey) === currentStyleKey
            };
        })
        .filter((item): item is { styleKey: string; score: number; isCurrent: boolean } => Boolean(item))
        .sort((left, right) => Number(left.isCurrent) - Number(right.isCurrent) || right.score - left.score);

    return matches[0]?.styleKey;
};

const mapStyleMatchesRequest = (
    styleKey: string,
    mapStyle: unknown,
    requestText: string,
    target?: string
): boolean => {
    const normalizedRequest = normalizeStyleSwitchText(requestText);
    const normalizedTarget = normalizeStyleSwitchText(target);
    const styleRecord = asRecord(mapStyle);
    const layers = Array.isArray(styleRecord.layers) ? styleRecord.layers : [];
    const layerTypes = layers
        .map((layer) => asRecord(layer).type)
        .filter((value): value is string => typeof value === 'string' && Boolean(value.trim()));
    const terms = [
        styleKey,
        styleRecord.styleKey,
        styleRecord.activeStyle,
        styleRecord.styleName,
        styleRecord.preset,
        ...layerTypes
    ]
        .map(normalizeStyleSwitchText)
        .filter((term, index, allTerms) => term.length >= 3 && allTerms.indexOf(term) === index);

    if (normalizedTarget && terms.some((term) => term === normalizedTarget || term.includes(normalizedTarget) || normalizedTarget.includes(term))) {
        return true;
    }

    return Boolean(normalizedRequest) && terms.some((term) => normalizedRequest.includes(term));
};

const getRequestedLayerIdFromToolArgs = (aiArgs: Record<string, unknown>): string | undefined => {
    const layerId = aiArgs.layerId
        || aiArgs.styleId
        || asRecord(aiArgs.params).layerId
        || asRecord(aiArgs.params).styleId
        || asRecord(aiArgs.options).layerId
        || asRecord(aiArgs.options).styleId;
    return typeof layerId === 'string' && layerId.trim() ? layerId.trim() : undefined;
};

const getRequestedLayerTextFromToolArgs = (aiArgs: Record<string, unknown>): string => {
    return [
        aiArgs.layerName,
        aiArgs.layerTitle,
        aiArgs.sourceLayer,
        asRecord(aiArgs.params).layerName,
        asRecord(aiArgs.params).layerTitle,
        asRecord(aiArgs.params).sourceLayer,
        asRecord(aiArgs.options).layerName,
        asRecord(aiArgs.options).layerTitle,
        asRecord(aiArgs.options).sourceLayer
    ]
        .filter((value): value is string => typeof value === 'string' && Boolean(value.trim()))
        .join(' ');
};

const getLayerMatchTerms = (
    layerId: string,
    layerState: ConversationMapLayerState
): string[] => {
    const layerRecord = asRecord(layerState.layer);
    const mapPayloadLayer = asRecord(asRecord(layerState.mapPayload).layer);
    const mapPayloadRecord = asRecord(layerState.mapPayload);
    const styleValues = Object.values(layerState.styles).flatMap((style) => {
        const styleRecord = asRecord(style);
        return [
            styleRecord.layerId,
            styleRecord.styleId,
            styleRecord.sourceLayer,
            styleRecord.source_layer,
            styleRecord.title,
            styleRecord.name
        ];
    });

    const values = [
        layerId,
        layerRecord.layerId,
        layerRecord.styleId,
        layerRecord.id,
        layerRecord.title,
        layerRecord.styleTitle,
        layerRecord.name,
        layerRecord.sourceLayer,
        layerRecord.source_layer,
        layerRecord.geometryType,
        mapPayloadLayer.layerId,
        mapPayloadLayer.styleId,
        mapPayloadLayer.id,
        mapPayloadLayer.title,
        mapPayloadLayer.styleTitle,
        mapPayloadLayer.name,
        mapPayloadLayer.sourceLayer,
        mapPayloadLayer.source_layer,
        mapPayloadRecord.title,
        mapPayloadRecord.name,
        ...styleValues
    ];

    return values
        .flatMap(getLayerTextMatchTerms)
        .filter((term, index, allTerms) => term.length >= 3 && allTerms.indexOf(term) === index);
};

const scoreLayerTextMatch = (
    terms: string[],
    normalizedRequest: string
): { score: number; longestMatch: number; matchCount: number } | undefined => {
    let score = 0;
    let longestMatch = 0;
    let matchCount = 0;

    for (const term of terms) {
        if (!term) continue;
        if (normalizedRequest === term) {
            score += term.length * 20;
            longestMatch = Math.max(longestMatch, term.length);
            matchCount += 1;
            continue;
        }
        if (normalizedRequest.includes(term)) {
            score += term.length * (term.length >= 8 ? 4 : 1);
            longestMatch = Math.max(longestMatch, term.length);
            matchCount += 1;
            continue;
        }
        if (term.includes(normalizedRequest)) {
            score += normalizedRequest.length * 2;
            longestMatch = Math.max(longestMatch, normalizedRequest.length);
            matchCount += 1;
        }
    }

    return score > 0
        ? { score, longestMatch, matchCount }
        : undefined;
};

const selectMapLayerStateByText = (
    mapState: ConversationMapState,
    requestText: string
): ConversationMapLayerState | undefined => {
    const normalizedRequest = normalizeStyleSwitchText(requestText);
    if (!normalizedRequest) return undefined;

    const matches = Object.entries(mapState.layers)
        .map(([layerId, layerState]) => {
            const terms = getLayerMatchTerms(layerId, layerState);
            const match = scoreLayerTextMatch(terms, normalizedRequest);
            if (!match) return undefined;

            return {
                layerState,
                ...match,
                isActive: mapState.activeLayerId === layerId
            };
        })
        .filter((item): item is { layerState: ConversationMapLayerState; score: number; longestMatch: number; matchCount: number; isActive: boolean } => Boolean(item))
        .sort((left, right) => (
            right.score - left.score
            || right.longestMatch - left.longestMatch
            || right.matchCount - left.matchCount
            || Number(right.isActive) - Number(left.isActive)
        ));

    return matches[0]?.layerState;
};

const getMapLayerStateAttributeNames = (layerState: ConversationMapLayerState): string[] => {
    const names = new Set<string>();
    const addName = (value: unknown) => {
        const name = toSuggestionString(value);
        if (name) names.add(name);
    };
    const collectFromPayload = (payload: unknown) => {
        const layer = getSuggestionLayerRecord(payload);
        const fields = asRecord(asRecord(layer.attributes).fields);
        for (const key of Object.keys(fields)) addName(key);
    };
    const collectFromStyle = (style: unknown) => {
        const styleRecord = asRecord(style);
        addName(styleRecord.attributeStyleKey || styleRecord.attributeKey);
        for (const variant of getMapStyleAttributeVariants(style).values()) {
            addName(variant.attributeKey || variant.name || variant.value);
        }
        for (const key of collectMapStyleAttributeKeys(style)) addName(key);
    };

    collectFromPayload(layerState.mapPayload || layerState.layer);
    collectFromStyle(layerState.latestMapStyle);
    Object.values(layerState.styles).forEach(collectFromStyle);

    return Array.from(names);
};

const getRequestedAttributeNamesForLayerSelection = (
    mapState: ConversationMapState,
    aiArgs: Record<string, unknown>,
    message?: string
): string[] => {
    const requested = new Set<string>();
    const addRequested = (value: unknown) => {
        const name = toSuggestionString(value);
        if (name) requested.add(name);
    };

    addRequested(aiArgs.attributeKey);
    addRequested(aiArgs.field);
    addRequested(aiArgs.key);
    addRequested(asRecord(aiArgs.params).attributeKey);
    addRequested(asRecord(aiArgs.params).field);
    addRequested(asRecord(aiArgs.options).attributeKey);
    addRequested(asRecord(aiArgs.options).field);

    const normalizedMessage = normalizeStyleSwitchText(message);
    if (normalizedMessage) {
        Object.values(mapState.layers)
            .flatMap(getMapLayerStateAttributeNames)
            .sort((left, right) => right.length - left.length)
            .forEach((name) => {
                const normalizedName = normalizeStyleSwitchText(name);
                if (normalizedName && normalizedMessage.includes(normalizedName)) {
                    requested.add(name);
                }
            });
    }

    return Array.from(requested);
};

const selectMapLayerStateByAttribute = (
    mapState: ConversationMapState,
    aiArgs: Record<string, unknown>,
    message?: string
): ConversationMapLayerState | undefined => {
    const requestedAttributes = getRequestedAttributeNamesForLayerSelection(mapState, aiArgs, message)
        .map((name) => normalizeStyleSwitchText(name))
        .filter(Boolean);
    if (requestedAttributes.length === 0) return undefined;

    const matches = Object.entries(mapState.layers)
        .map(([layerId, layerState]) => {
            const attributeNames = getMapLayerStateAttributeNames(layerState);
            const matchedAttribute = attributeNames
                .map((name) => normalizeStyleSwitchText(name))
                .filter(Boolean)
                .find((name) => requestedAttributes.includes(name));
            if (!matchedAttribute) return undefined;

            return {
                layerState,
                score: matchedAttribute.length,
                isActive: mapState.activeLayerId === layerId
            };
        })
        .filter((item): item is { layerState: ConversationMapLayerState; score: number; isActive: boolean } => Boolean(item))
        .sort((left, right) => (
            Number(right.isActive) - Number(left.isActive)
            || right.score - left.score
        ));

    return matches[0]?.layerState;
};

const selectMapLayerIdsByText = (
    mapState: ConversationMapState | undefined,
    requestText: string
): string[] => {
    if (!mapState?.layers) return [];

    const normalizedRequest = normalizeStyleSwitchText(requestText);
    if (!normalizedRequest) return [];

    return Object.entries(mapState.layers)
        .map(([layerId, layerState]) => {
            const terms = getLayerMatchTerms(layerId, layerState);
            const match = scoreLayerTextMatch(terms, normalizedRequest);
            if (!match) return undefined;

            return {
                layerId,
                ...match,
                isActive: mapState.activeLayerId === layerId
            };
        })
        .filter((item): item is { layerId: string; score: number; longestMatch: number; matchCount: number; isActive: boolean } => Boolean(item))
        .sort((left, right) => (
            right.score - left.score
            || right.longestMatch - left.longestMatch
            || right.matchCount - left.matchCount
            || Number(right.isActive) - Number(left.isActive)
        ))
        .map((item) => item.layerId);
};

const selectMapLayerStateForEdit = (
    mapState: ConversationMapState | undefined,
    aiArgs: Record<string, unknown>,
    message?: string
): ConversationMapLayerState | undefined => {
    if (!mapState?.layers) return undefined;

    // Explicit layerId wins over text matching because conversations can contain
    // several layers with similar titles or translated labels.
    const requestedLayerId = getRequestedLayerIdFromToolArgs(aiArgs);
    if (requestedLayerId && mapState.layers[requestedLayerId]) {
        return mapState.layers[requestedLayerId];
    }

    const explicitLayerText = getRequestedLayerTextFromToolArgs(aiArgs);
    const explicitLayerState = selectMapLayerStateByText(mapState, explicitLayerText);
    if (explicitLayerState) {
        return explicitLayerState;
    }

    const requestedLayerState = selectMapLayerStateByText(mapState, message || '');
    if (requestedLayerState) {
        return requestedLayerState;
    }

    const latestLayerState = getLatestLayerState(mapState);
    if (latestLayerState) {
        return latestLayerState;
    }

    const requestedAttributeLayerState = selectMapLayerStateByAttribute(mapState, aiArgs, message);
    if (requestedAttributeLayerState) {
        return requestedAttributeLayerState;
    }

    return getLatestStyledLayerState(mapState);
};

const selectMapStyleForEdit = (
    mapState: ConversationMapState | undefined,
    aiArgs: Record<string, unknown>,
    message: string,
    fallbackMapStyle?: unknown
): unknown | undefined => {
    const layerState = selectMapLayerStateForEdit(mapState, aiArgs, message);
    if (!layerState) return fallbackMapStyle;

    const selectAttributeVariant = (mapStyle: unknown): unknown => {
        const styleRecord = asRecord(mapStyle);
        const variants = getMapStyleAttributeVariants(styleRecord);
        const explicitAttribute = toSuggestionString(aiArgs.attributeKey);
        const normalizedMessage = normalizeStyleSwitchText(message);
        const attributeKey = explicitAttribute || Array.from(variants.keys())
            .sort((left, right) => right.length - left.length)
            .find((name) => normalizedMessage.includes(normalizeStyleSwitchText(name)));
        const variant = attributeKey ? variants.get(attributeKey) : undefined;
        if (!variant || !Array.isArray(variant.layers)) return mapStyle;

        // Attribute variants are stored as edit history for a style. Rehydrate the
        // requested variant here so later patches target the user's active attribute.
        return {
            ...styleRecord,
            ...variant,
            layers: variant.layers,
            attributeStyleKey: attributeKey,
            attributeStyleVariants: styleRecord.attributeStyleVariants
        };
    };

    const target = typeof aiArgs.target === 'string' ? aiArgs.target : undefined;
    const styleEntries = Object.entries(layerState.styles);
    const requestedStyle = styleEntries.find(([styleKey, mapStyle]) => {
        return mapStyleMatchesRequest(styleKey, mapStyle, message, target);
    });
    if (requestedStyle) return selectAttributeVariant(requestedStyle[1]);

    if (layerState.activeStyle && layerState.styles[layerState.activeStyle]) {
        return selectAttributeVariant(layerState.styles[layerState.activeStyle]);
    }

    if (layerState.latestMapStyle) {
        return selectAttributeVariant(layerState.latestMapStyle);
    }

    const fallbackStyleLayerId = getMapStyleLayerId(fallbackMapStyle);
    const selectedLayerId = getLayerIdFromMapPayload(layerState.mapPayload);
    if (fallbackMapStyle && fallbackStyleLayerId && selectedLayerId && fallbackStyleLayerId === selectedLayerId) {
        return selectAttributeVariant(fallbackMapStyle);
    }

    return undefined;
};

const selectMapPayloadForEdit = (
    mapState: ConversationMapState | undefined,
    aiArgs: Record<string, unknown>,
    fallbackMapPayload?: unknown,
    message?: string
): unknown | undefined => {
    return selectMapLayerStateForEdit(mapState, aiArgs, message)?.mapPayload || fallbackMapPayload;
};

const buildClearMapLayerArgs = (
    aiArgs: Record<string, unknown>,
    message: string,
    mapState?: ConversationMapState
) => {
    const params = asRecord(aiArgs.params);
    const options = asRecord(aiArgs.options);
    const explicitLayerIds = toClearLayerIdList(
        aiArgs.layerIds,
        aiArgs.layerId,
        aiArgs.styleId,
        aiArgs.layerTitle,
        aiArgs.styleTitle,
        params.layerIds,
        params.layerId,
        params.styleId,
        params.layerTitle,
        params.styleTitle,
        options.layerIds,
        options.layerId,
        options.styleId,
        options.layerTitle,
        options.styleTitle
    );
    const inferredLayerIds = selectMapLayerIdsByText(mapState, message);
    const layerIds = Array.from(new Set([
        ...explicitLayerIds,
        ...inferredLayerIds
    ]));
    const mode = typeof aiArgs.mode === 'string' && aiArgs.mode.trim()
        ? aiArgs.mode
        : layerIds.length > 0
            ? 'selected'
            : undefined;

    return {
        mode,
        layerId: layerIds.length === 1 ? layerIds[0] : undefined,
        layerIds: layerIds.length > 0 ? layerIds : undefined
    };
};

const toSuggestionString = (value: unknown): string | undefined => {
    return typeof value === 'string' && value.trim() ? value.trim() : undefined;
};

const normalizeSuggestionGeometryType = (value: unknown): string | undefined => {
    const normalized = toSuggestionString(value)?.toLowerCase();
    if (!normalized) return undefined;
    if (['point', 'multipoint'].includes(normalized)) return 'point';
    if (['line', 'linestring', 'multilinestring'].includes(normalized)) return 'line';
    if (['polygon', 'multipolygon'].includes(normalized)) return 'polygon';
    if (['raster', 'image'].includes(normalized)) return 'raster';
    return normalized;
};

const getSuggestionLayerRecord = (mapPayload: unknown): Record<string, unknown> => {
    const payloadRecord = asRecord(mapPayload);
    return asRecord(payloadRecord.layer) || payloadRecord;
};

const getSuggestionStyleGeometryTypes = (style: unknown): string[] => {
    const record = asRecord(style);
    if (!Array.isArray(record.geometryTypes)) return [];

    return Array.from(new Set(
        record.geometryTypes
            .map(normalizeSuggestionGeometryType)
            .filter((value): value is string => Boolean(value))
    ));
};

const hasEditableColorPaint = (mapStyle: unknown): boolean => {
    const layers = asRecord(mapStyle).layers;
    if (!Array.isArray(layers)) return false;

    return layers.some((layer) => {
        const paint = asRecord(asRecord(layer).paint);
        return Object.keys(paint).some((key) => {
            return key.endsWith('-color') && key !== 'heatmap-color';
        });
    });
};

const normalizeSuggestionColorValue = (value: unknown): string | undefined => {
    if (typeof value !== 'string') return undefined;
    const color = value.trim();
    if (/^#[0-9a-f]{3}$/i.test(color)) {
        const [, r = '', g = '', b = ''] = color;
        return `#${r}${r}${g}${g}${b}${b}`.toUpperCase();
    }
    if (/^#[0-9a-f]{6}$/i.test(color)) return color.toUpperCase();
    return undefined;
};

const normalizeStyleOutputColorValue = (value: unknown): string | undefined => {
    const directColor = normalizeSuggestionColorValue(value);
    if (directColor) return directColor;

    const record = asRecord(value);
    for (const key of ['colorValue', 'color', 'value', 'output']) {
        const color = normalizeSuggestionColorValue(record[key]);
        if (color) return color;
    }

    return undefined;
};

const getPrimaryColorFromPaintValue = (value: unknown): string | undefined => {
    const directColor = normalizeStyleOutputColorValue(value);
    if (directColor) return directColor;

    if (!Array.isArray(value)) return undefined;

    const colors = value
        .map((item) => isTransparentColorValue(item) ? undefined : normalizeStyleOutputColorValue(item))
        .filter((item): item is string => Boolean(item));
    if (colors.length === 0) return undefined;
    const colorCounts = colors.map((color) => ({
        color,
        count: colors.filter((item) => item === color).length
    }));
    const repeatedColor = colorCounts.find((item) => item.count > 1);
    if (repeatedColor) return repeatedColor.color;

    for (let index = value.length - 1; index >= 0; index -= 1) {
        const color = isTransparentColorValue(value[index]) ? undefined : normalizeStyleOutputColorValue(value[index]);
        if (color) return color;
    }

    return undefined;
};

const isTransparentColorValue = (value: unknown): boolean => {
    if (typeof value !== 'string') return false;
    const normalized = value.trim().toLowerCase().replace(/\s+/g, '');
    if (normalized === 'transparent') return true;
    const rgbaMatch = normalized.match(/^rgba\([^,]+,[^,]+,[^,]+,([^)]+)\)$/);
    return rgbaMatch ? Number(rgbaMatch[1]) === 0 : false;
};

const replacePaintColors = (value: unknown, color: string): unknown => {
    if (typeof value === 'string') {
        if (isTransparentColorValue(value)) return value;
        return normalizeSuggestionColorValue(value) || /^rgba?\(/i.test(value.trim())
            ? color
            : value;
    }

    if (Array.isArray(value)) {
        return value.map((item) => replacePaintColors(item, color));
    }

    const record = asRecord(value);
    if (Object.keys(record).length > 0) {
        let changed = false;
        const next = Object.fromEntries(
            Object.entries(record).map(([key, nestedValue]) => {
                const nestedColor = normalizeStyleOutputColorValue(nestedValue);
                if (!nestedColor) return [key, nestedValue];
                changed = true;
                return [key, color];
            })
        );
        return changed ? next : value;
    }

    return value;
};

const getCurrentMapStyleColor = (mapStyle: unknown): string | undefined => {
    const styleRecord = asRecord(mapStyle);
    const appliedColor = normalizeSuggestionColorValue(styleRecord.appliedColor);
    if (appliedColor) return appliedColor;

    const layers = styleRecord.layers;
    if (!Array.isArray(layers)) return undefined;

    for (const layer of layers) {
        const paint = asRecord(asRecord(layer).paint);
        for (const [key, value] of Object.entries(paint)) {
            if (!key.endsWith('-color') || key === 'heatmap-color') continue;

            const color = getPrimaryColorFromPaintValue(value);
            if (color) return color;
        }
    }

    return undefined;
};

const buildClearMapSuggestionItems = () => [
    {
        key: 'clear_layer',
        label: 'Clear layer',
        promptTemplate: 'Clear map layer '
    },
    {
        key: 'clear_all_layers',
        label: 'Clear all layers',
        promptTemplate: 'Clear all map layers'
    }
];

const collectMapStyleAttributeKeys = (value: unknown, keys = new Set<string>()): Set<string> => {
    if (!Array.isArray(value)) {
        if (value && typeof value === 'object') {
            for (const nestedValue of Object.values(value as Record<string, unknown>)) {
                collectMapStyleAttributeKeys(nestedValue, keys);
            }
        }
        return keys;
    }

    const [operator, attributeKey] = value;
    if (operator === 'get') {
        const key = toSuggestionString(attributeKey);
        if (key) keys.add(key);
    }

    for (const nestedValue of value) {
        collectMapStyleAttributeKeys(nestedValue, keys);
    }

    return keys;
};

const getMapStyleAttributeVariants = (mapStyle: unknown): Map<string, Record<string, unknown>> => {
    const variants = asRecord(mapStyle).attributeStyleVariants;
    const items = new Map<string, Record<string, unknown>>();

    if (Array.isArray(variants)) {
        for (const item of variants) {
            const record = asRecord(item);
            const name = toSuggestionString(record.attributeKey || record.name || record.value);
            if (name) items.set(name, { ...record, attributeKey: name });
        }
        return items;
    }

    for (const [key, value] of Object.entries(asRecord(variants))) {
        const record = asRecord(value);
        const name = toSuggestionString(record.attributeKey || key);
        if (name) items.set(name, { ...record, attributeKey: name });
    }

    return items;
};

const getMapStyleAttributeVariantFields = (mapStyle: unknown): Array<{ name: string; type?: string }> => {
    return Array.from(getMapStyleAttributeVariants(mapStyle).values())
        .map((record) => {
            const name = toSuggestionString(record.attributeKey || record.name || record.value);
            const type = toSuggestionString(record.attributeType || record.type);
            return name ? { name, ...(type ? { type } : {}) } : undefined;
        })
        .filter((item): item is { name: string; type?: string } => Boolean(item));
};

const areStyleLayersEqual = (left: unknown, right: unknown): boolean => {
    if (!Array.isArray(left) || !Array.isArray(right)) return false;
    try {
        return JSON.stringify(left) === JSON.stringify(right);
    } catch {
        return false;
    }
};

const mergeMapStyleAttributeVariants = (
    mapStyle: unknown,
    previousMapStyle?: unknown,
    mapPayload?: unknown
): Record<string, unknown> => {
    const styleRecord = asRecord(mapStyle);
    const variantsByName = getMapStyleAttributeVariants(previousMapStyle);
    for (const [name, variant] of getMapStyleAttributeVariants(styleRecord)) {
        variantsByName.set(name, variant);
    }

    const layer = getSuggestionLayerRecord(mapPayload || styleRecord);
    const fields = asRecord(asRecord(layer.attributes).fields);
    const explicitAttribute = toSuggestionString(styleRecord.attributeStyleKey || styleRecord.attributeKey);
    const usedAttributes = explicitAttribute
        ? new Set([explicitAttribute])
        : new Set<string>();
    const layers = Array.isArray(styleRecord.layers) ? styleRecord.layers : undefined;
    if (explicitAttribute && layers) {
        for (const [name, variant] of variantsByName) {
            if (name !== explicitAttribute && areStyleLayersEqual(asRecord(variant).layers, layers)) {
                variantsByName.delete(name);
            }
        }
    }
    for (const name of usedAttributes) {
        const fieldType = name === explicitAttribute
            ? toSuggestionString(styleRecord.attributeStyleType || asRecord(fields[name]).type)
            : toSuggestionString(asRecord(fields[name]).type);
        const previousVariant = asRecord(variantsByName.get(name));
        const previousType = toSuggestionString(previousVariant.attributeType || previousVariant.type);
        variantsByName.set(name, {
            ...previousVariant,
            attributeKey: name,
            ...(fieldType ? { attributeType: fieldType } : previousType ? { attributeType: previousType } : {}),
            ...(layers ? { layers } : {}),
            ...(styleRecord.styleKey ? { styleKey: styleRecord.styleKey } : {}),
            ...(styleRecord.styleName ? { styleName: styleRecord.styleName } : {}),
            ...(styleRecord.activeStyle ? { activeStyle: styleRecord.activeStyle } : {}),
            ...(styleRecord.defaultStyle ? { defaultStyle: styleRecord.defaultStyle } : {}),
            ...(styleRecord.geometryType ? { geometryType: styleRecord.geometryType } : {})
        });
    }
    const firstAttribute = usedAttributes.values().next().value;

    return {
        ...styleRecord,
        ...(firstAttribute ? { attributeStyleKey: firstAttribute } : {}),
        ...(variantsByName.size > 0
            ? {
                attributeStyleVariants: Object.fromEntries(variantsByName)
            }
            : {})
    };
};

const syncMapStyleFiltersToAttributeVariants = (mapStyle: unknown): Record<string, unknown> => {
    const styleRecord = asRecord(mapStyle);
    const activeLayers = Array.isArray(styleRecord.layers) ? styleRecord.layers.map(asRecord) : [];
    const variants = getMapStyleAttributeVariants(styleRecord);
    if (activeLayers.length === 0 || variants.size === 0) return styleRecord;

    const updatedVariants = Object.fromEntries(Array.from(variants.entries()).map(([name, variant]) => {
        const variantLayers = Array.isArray(variant.layers) ? variant.layers.map(asRecord) : [];
        const layers = variantLayers.map((layer, index) => {
            const layerId = toSuggestionString(layer.id);
            const activeLayer = activeLayers.find((candidate) => (
                layerId && toSuggestionString(candidate.id) === layerId
            )) || activeLayers[index];
            if (!activeLayer) return layer;

            if (activeLayer.filter === undefined) {
                const { filter: _removedFilter, ...rest } = layer;
                return rest;
            }
            return {
                ...layer,
                filter: activeLayer.filter
            };
        });
        return [name, {
            ...variant,
            ...(variantLayers.length > 0 ? { layers } : {})
        }];
    }));

    return {
        ...styleRecord,
        attributeStyleVariants: updatedVariants
    };
};

const getMapStyleAttributeKey = (value: unknown): string | undefined => {
    if (!Array.isArray(value)) return undefined;
    const [operator, attributeKey] = value;
    if (operator === 'get') return toSuggestionString(attributeKey);

    for (const item of value) {
        const key = getMapStyleAttributeKey(item);
        if (key) return key;
    }

    return undefined;
};

const getMapStyleExpressionOutputKind = (value: unknown): 'color' | 'number' | 'boolean' | 'string' | undefined => {
    if (typeof value === 'number') return 'number';
    if (typeof value === 'boolean') return 'boolean';
    if (typeof value === 'string') return normalizeSuggestionColorValue(value) ? 'color' : 'string';
    if (!Array.isArray(value)) return undefined;

    const outputs: Array<'color' | 'number' | 'boolean' | 'string'> = [];
    const [operator] = value;
    const startIndex = operator === 'interpolate' ? 3 : operator === 'match' ? 2 : -1;
    if (startIndex >= 0) {
        for (let index = startIndex + 1; index < value.length; index += 2) {
            const outputKind = getMapStyleExpressionOutputKind(value[index]);
            if (outputKind) outputs.push(outputKind);
        }
    }

    if (outputs.length === 0) return undefined;
    return outputs.every((item) => item === outputs[0]) ? outputs[0] : undefined;
};

const isMapStyleAttributeExpression = (value: unknown): boolean => {
    return Array.isArray(value) && Boolean(getMapStyleAttributeKey(value));
};

const getPaintKeyRole = (paintKey: string, value: unknown): string | undefined => {
    const outputKind = getMapStyleExpressionOutputKind(value);
    const parts = paintKey.split('-').filter(Boolean);
    const keyRole = parts.length > 1 ? parts[parts.length - 1] : undefined;
    if (outputKind === 'color') return 'color';
    return keyRole || outputKind;
};

const collectAttributePaintExpressions = (
    paint: Record<string, unknown>
): Array<{ paintKey: string; value: unknown; role?: string }> => {
    return Object.entries(paint)
        .filter(([, value]) => isMapStyleAttributeExpression(value))
        .map(([paintKey, value]) => ({
            paintKey,
            value,
            role: getPaintKeyRole(paintKey, value)
        }));
};

const findPresetPaintTargetKey = (
    source: { paintKey: string; value: unknown; role?: string },
    presetPaint: Record<string, unknown>,
    usedTargets: Set<string>
): string | undefined => {
    if (presetPaint[source.paintKey] !== undefined && !usedTargets.has(source.paintKey)) {
        return source.paintKey;
    }

    if (!source.role) return undefined;

    return Object.entries(presetPaint)
        .find(([candidateKey, candidateValue]) => (
            !usedTargets.has(candidateKey)
            && (!Array.isArray(candidateValue) || isMapStyleAttributeExpression(candidateValue))
            && getPaintKeyRole(candidateKey, candidateValue) === source.role
        ))?.[0];
};

const getFilterEqualityAttributeValue = (
    filter: unknown
): { attributeKey: string; value: unknown } | undefined => {
    if (!Array.isArray(filter)) return undefined;
    const [operator, left, right] = filter;
    if (operator === '==' && getMapStyleAttributeKey(left) && right !== undefined) {
        return {
            attributeKey: getMapStyleAttributeKey(left)!,
            value: right
        };
    }

    for (const item of filter.slice(1)) {
        const found = getFilterEqualityAttributeValue(item);
        if (found) return found;
    }

    return undefined;
};

const getLayerPrimaryColor = (layer: Record<string, unknown>): string | undefined => {
    const paint = asRecord(layer.paint);
    for (const [key, value] of Object.entries(paint)) {
        if (!key.endsWith('-color')) continue;
        const color = getPrimaryColorFromPaintValue(value);
        if (color) return color;
    }
    return undefined;
};

const collectFilteredAttributeColorStops = (
    layers: Array<Record<string, unknown>>
): Map<string, Array<{ value: unknown; color: string }>> => {
    const stopsByAttribute = new Map<string, Array<{ value: unknown; color: string }>>();
    for (const layer of layers) {
        const filterMatch = getFilterEqualityAttributeValue(layer.filter);
        const color = getLayerPrimaryColor(layer);
        if (!filterMatch || !color) continue;

        const stops = stopsByAttribute.get(filterMatch.attributeKey) || [];
        const serializedValue = JSON.stringify(filterMatch.value);
        if (!stops.some((item) => JSON.stringify(item.value) === serializedValue)) {
            stops.push({ value: filterMatch.value, color });
        }
        stopsByAttribute.set(filterMatch.attributeKey, stops);
    }
    return stopsByAttribute;
};

const collectExpressionAttributeColorStops = (
    mapStyle: unknown,
    preferredAttribute?: string
): { attributeKey: string; stops: Array<{ value: unknown; color: string }>; fallbackColor?: string } | undefined => {
    const styleRecord = asRecord(mapStyle);
    const activeLayers = Array.isArray(styleRecord.layers) ? styleRecord.layers : [];
    const valuesByAttribute = collectMapStyleAttributeValues({ layers: activeLayers });
    const attributeKeys = [
        ...(preferredAttribute ? [preferredAttribute] : []),
        ...Array.from(valuesByAttribute.keys())
    ].filter((key, index, keys) => keys.indexOf(key) === index);

    for (const attributeKey of attributeKeys) {
        const values = valuesByAttribute.get(attributeKey) || [];
        const stops = values
            .map((item) => {
                const color = normalizeStyleOutputColorValue(item.output);
                return color ? { value: item.value, color } : undefined;
            })
            .filter((item): item is { value: unknown; color: string } => Boolean(item))
            .filter((item, index, items) => {
                const serializedValue = JSON.stringify(item.value);
                return items.findIndex((candidate) => JSON.stringify(candidate.value) === serializedValue) === index;
            });
        if (stops.length > 0) {
            const fallbackColor = activeLayers
                .map((layer) => getAttributeColorExpressionFallback(asRecord(layer).paint, attributeKey))
                .find(Boolean);
            return {
                attributeKey,
                stops,
                ...(fallbackColor ? { fallbackColor } : {})
            };
        }
    }

    return undefined;
};

const getAttributeColorExpressionFallback = (
    value: unknown,
    attributeKey: string
): string | undefined => {
    if (Array.isArray(value)) {
        const [operator] = value;
        const expressionAttribute = operator === 'match'
            ? getMapStyleAttributeKey(value[1])
            : operator === 'interpolate'
                ? getMapStyleAttributeKey(value[2])
                : undefined;
        if (expressionAttribute === attributeKey) {
            const fallbackColor = operator === 'match'
                ? normalizeStyleOutputColorValue(value[value.length - 1])
                : undefined;
            if (fallbackColor) return fallbackColor;
        }

        for (const item of value) {
            const fallbackColor = getAttributeColorExpressionFallback(item, attributeKey);
            if (fallbackColor) return fallbackColor;
        }
        return undefined;
    }

    const record = asRecord(value);
    for (const nestedValue of Object.values(record)) {
        const fallbackColor = getAttributeColorExpressionFallback(nestedValue, attributeKey);
        if (fallbackColor) return fallbackColor;
    }

    return undefined;
};

const combineMapStyleFilters = (existingFilter: unknown, nextFilter: unknown): unknown => {
    if (existingFilter === undefined || existingFilter === null) return nextFilter;
    return ['all', existingFilter, nextFilter];
};

const buildExcludeAttributeValuesFilter = (
    attributeKey: string,
    values: unknown[]
): unknown | undefined => {
    const conditions = values.map((value) => ['!=', ['get', attributeKey], value]);
    if (conditions.length === 0) return undefined;
    return conditions.length === 1 ? conditions[0] : ['all', ...conditions];
};

const buildStyleLayerIdSuffix = (value: unknown, index: number): string => {
    const text = toSuggestionString(value) || String(index);
    return text.toLowerCase().replace(/[^a-z0-9]+/gi, '-').replace(/^-+|-+$/g, '') || String(index);
};

const areMapStyleValuesEqual = (left: unknown, right: unknown): boolean => {
    return JSON.stringify(left) === JSON.stringify(right);
};

const buildFallbackLayerPaint = (
    paint: Record<string, unknown>,
    colorPaintKey: string,
    colorPaintValue: unknown,
    color: string
): Record<string, unknown> => {
    return Object.fromEntries(
        Object.entries({
            ...paint,
            [colorPaintKey]: replacePaintColors(colorPaintValue, color)
        }).map(([key, value]) => {
            if (!key.endsWith('-opacity') || typeof value !== 'number') return [key, value];
            return [key, Math.max(0, Math.min(1, value / 4))];
        })
    );
};

const buildFilteredAttributeColorLayers = (
    layer: Record<string, unknown>,
    currentStyle: unknown,
    preferredAttribute?: string
): Array<Record<string, unknown>> | undefined => {
    const attributeStops = collectExpressionAttributeColorStops(currentStyle, preferredAttribute);
    if (!attributeStops) return undefined;

    const paint = asRecord(layer.paint);
    const targetEntry = Object.entries(paint).find(([paintKey, paintValue]) => (
        paintKey.endsWith('-color')
        && Array.isArray(paintValue)
        && !isMapStyleAttributeExpression(paintValue)
    ));
    if (!targetEntry) return undefined;

    const [paintKey, paintValue] = targetEntry;
    const existingFilterMatch = getFilterEqualityAttributeValue(layer.filter);
    const constrainedValue = existingFilterMatch?.attributeKey === attributeStops.attributeKey
        ? existingFilterMatch.value
        : undefined;
    const activeStops = constrainedValue !== undefined
        ? attributeStops.stops.filter((stop) => areMapStyleValuesEqual(stop.value, constrainedValue))
        : attributeStops.stops;
    if (activeStops.length === 0) return undefined;

    const baseId = toSuggestionString(layer.id)
        || [
            toSuggestionString(asRecord(currentStyle).layerId),
            toSuggestionString(asRecord(currentStyle).styleKey || asRecord(currentStyle).activeStyle),
            toSuggestionString(layer.type)
        ].filter(Boolean).join('-')
        || 'map-style-layer';
    const filteredLayers = activeStops.map((stop, index) => ({
        ...layer,
        id: `${baseId}-${attributeStops.attributeKey}-${buildStyleLayerIdSuffix(stop.value, index)}`,
        filter: constrainedValue !== undefined
            ? layer.filter
            : combineMapStyleFilters(layer.filter, ['==', ['get', attributeStops.attributeKey], stop.value]),
        paint: {
            ...paint,
            [paintKey]: replacePaintColors(paintValue, stop.color)
        }
    }));
    if (constrainedValue !== undefined) return filteredLayers;

    const fallbackFilter = buildExcludeAttributeValuesFilter(
        attributeStops.attributeKey,
        attributeStops.stops.map((stop) => stop.value)
    );
    const fallbackColor = attributeStops.fallbackColor || getPrimaryColorFromPaintValue(paintValue);
    if (!fallbackFilter || !fallbackColor) return filteredLayers;

    return [
        {
            ...layer,
            id: `${baseId}-${attributeStops.attributeKey}-fallback`,
            filter: combineMapStyleFilters(layer.filter, fallbackFilter),
            paint: buildFallbackLayerPaint(paint, paintKey, paintValue, fallbackColor)
        },
        ...filteredLayers
    ];
};

const mergeFilteredAttributeColorsIntoLayer = (
    layer: Record<string, unknown>,
    currentLayers: Array<Record<string, unknown>>,
    preferredAttribute?: string
): Record<string, unknown> => {
    const stopsByAttribute = collectFilteredAttributeColorStops(currentLayers);
    if (stopsByAttribute.size === 0) return layer;

    const attributeKey = preferredAttribute && stopsByAttribute.has(preferredAttribute)
        ? preferredAttribute
        : Array.from(stopsByAttribute.keys())[0];
    const stops = stopsByAttribute.get(attributeKey) || [];
    if (!attributeKey || stops.length === 0) return layer;

    const paint = asRecord(layer.paint);
    const targetKey = findPresetPaintTargetKey(
        { paintKey: '', value: stops[0]?.color, role: 'color' },
        paint,
        new Set()
    );
    if (!targetKey) return layer;

    const fallbackColor = getPrimaryColorFromPaintValue(paint[targetKey]) || stops[stops.length - 1]?.color;
    const expression: unknown[] = ['match', ['get', attributeKey]];
    for (const stop of stops) {
        expression.push(stop.value, stop.color);
    }
    expression.push(fallbackColor);

    return {
        ...layer,
        paint: {
            ...paint,
            [targetKey]: expression
        }
    };
};

const getLayerAttributeFieldNames = (mapPayload?: unknown): Set<string> => {
    if (!mapPayload) return new Set();

    const layer = getSuggestionLayerRecord(mapPayload);
    const fields = asRecord(asRecord(layer.attributes).fields);
    return new Set(Object.keys(fields));
};

const collectExpressionOutputsForFallback = (value: unknown, outputs: unknown[] = []): unknown[] => {
    if (!Array.isArray(value)) return outputs;

    const [operator] = value;
    if (operator === 'interpolate') {
        for (let index = 4; index < value.length; index += 2) {
            outputs.push(value[index]);
        }
    }
    if (operator === 'match') {
        for (let index = 3; index < value.length; index += 2) {
            outputs.push(value[index]);
        }
        if (value.length > 2) outputs.push(value[value.length - 1]);
    }

    for (const item of value) {
        collectExpressionOutputsForFallback(item, outputs);
    }

    return outputs;
};

const getExpressionFallbackOutput = (value: unknown): unknown => {
    const outputs = collectExpressionOutputsForFallback(value);
    const numbers = outputs
        .map((item) => typeof item === 'number' ? item : undefined)
        .filter((item): item is number => item !== undefined);
    if (numbers.length > 0) return Math.max(...numbers);

    return outputs.find((item) => item !== undefined && item !== null);
};

const replaceUnavailableAttributeExpressions = (
    value: unknown,
    fieldNames: Set<string>
): unknown => {
    if (fieldNames.size === 0) return value;

    if (Array.isArray(value)) {
        const referencedAttributes = Array.from(collectMapStyleAttributeKeys(value));
        if (referencedAttributes.some((attribute) => !fieldNames.has(attribute))) {
            const fallbackOutput = getExpressionFallbackOutput(value);
            if (fallbackOutput !== undefined) return fallbackOutput;
        }

        return value.map((item) => replaceUnavailableAttributeExpressions(item, fieldNames));
    }

    const record = asRecord(value);
    if (Object.keys(record).length > 0) {
        return Object.fromEntries(
            Object.entries(record).map(([key, nestedValue]) => [
                key,
                replaceUnavailableAttributeExpressions(nestedValue, fieldNames)
            ])
        );
    }

    return value;
};

const filterReferencesUnavailableAttribute = (
    filter: unknown,
    fieldNames: Set<string>
): boolean => {
    if (fieldNames.size === 0 || filter === undefined || filter === null) return false;
    const referencedAttributes = Array.from(collectMapStyleAttributeKeys(filter));
    return referencedAttributes.some((attribute) => !fieldNames.has(attribute));
};

const sanitizeMapStyleLayerForPayload = (
    layer: Record<string, unknown>,
    mapPayload?: unknown
): Record<string, unknown> => {
    const fieldNames = getLayerAttributeFieldNames(mapPayload);
    if (fieldNames.size === 0) return layer;

    const paint = asRecord(layer.paint);
    const layout = asRecord(layer.layout);
    const nextLayer: Record<string, unknown> = {
        ...layer,
        ...(Object.keys(paint).length > 0
            ? {
                paint: Object.fromEntries(
                    Object.entries(paint).map(([key, value]) => [
                        key,
                        replaceUnavailableAttributeExpressions(value, fieldNames)
                    ])
                )
            }
            : {}),
        ...(Object.keys(layout).length > 0
            ? {
                layout: Object.fromEntries(
                    Object.entries(layout).map(([key, value]) => [
                        key,
                        replaceUnavailableAttributeExpressions(value, fieldNames)
                    ])
                )
            }
            : {})
    };

    if (filterReferencesUnavailableAttribute(layer.filter, fieldNames)) {
        const { filter: _removedFilter, ...withoutFilter } = nextLayer;
        return withoutFilter;
    }

    return nextLayer;
};

const mergeLayerEditsIntoPresetLayer = (
    currentLayer: Record<string, unknown>,
    presetLayer: Record<string, unknown>,
    currentColor?: string
): Record<string, unknown> => {
    const currentPaint = asRecord(currentLayer.paint);
    const presetPaint = asRecord(presetLayer.paint);
    const currentLayout = asRecord(currentLayer.layout);
    const presetLayout = asRecord(presetLayer.layout);
    const paint: Record<string, unknown> = { ...presetPaint };
    const layout: Record<string, unknown> = { ...presetLayout };

    for (const [key, value] of Object.entries(currentPaint)) {
        if (paint[key] !== undefined) paint[key] = value;
    }
    const usedPaintTargets = new Set(Object.keys(currentPaint).filter((key) => presetPaint[key] !== undefined));
    for (const source of collectAttributePaintExpressions(currentPaint)) {
        const targetKey = findPresetPaintTargetKey(source, presetPaint, usedPaintTargets);
        if (!targetKey) continue;
        paint[targetKey] = source.value;
        usedPaintTargets.add(targetKey);
    }
    if (currentColor) {
        for (const [key, value] of Object.entries(presetPaint)) {
            if (!key.endsWith('-color') || usedPaintTargets.has(key)) continue;
            paint[key] = replacePaintColors(value, currentColor);
            usedPaintTargets.add(key);
        }
    }
    for (const [key, value] of Object.entries(currentLayout)) {
        if (layout[key] !== undefined) layout[key] = value;
    }

    return {
        ...presetLayer,
        ...(currentLayer.filter !== undefined ? { filter: currentLayer.filter } : {}),
        ...(Object.keys(layout).length > 0 ? { layout } : {}),
        ...(Object.keys(paint).length > 0 ? { paint } : {})
    };
};

const getMapStyleLayerTypes = (style: unknown): Set<string> => {
    const layers = asRecord(style).layers;
    if (!Array.isArray(layers)) return new Set();

    return new Set(
        layers
            .map((layer) => toSuggestionString(asRecord(layer).type)?.toLowerCase())
            .filter((type): type is string => Boolean(type))
    );
};

const isCircleHeatmapStyleSwitch = (presetStyle: unknown, currentStyle: unknown): boolean => {
    const presetTypes = getMapStyleLayerTypes(presetStyle);
    const currentTypes = getMapStyleLayerTypes(currentStyle);

    return (
        (currentTypes.has('circle') && presetTypes.has('heatmap'))
        || (currentTypes.has('heatmap') && presetTypes.has('circle'))
    );
};

const mergeCurrentFiltersIntoPresetLayers = (
    presetLayers: Array<Record<string, unknown>>,
    currentLayers: Array<Record<string, unknown>>,
    mapPayload?: unknown
): Array<Record<string, unknown>> => {
    // Filters describe which features are visible, so they can survive a style
    // shape change such as circle <-> heatmap even when paint expressions cannot.
    const canMatchByIndex = currentLayers.length === 1 || currentLayers.length === presetLayers.length;

    return presetLayers.map((presetLayer, index) => {
        const presetId = toSuggestionString(presetLayer.id);
        const currentLayer = presetId
            ? currentLayers.find((layer) => toSuggestionString(layer.id) === presetId)
            : canMatchByIndex
                ? currentLayers[index] || currentLayers[0]
                : undefined;
        const layerWithFilter = currentLayer?.filter !== undefined
            ? { ...presetLayer, filter: currentLayer.filter }
            : presetLayer;

        return sanitizeMapStyleLayerForPayload(layerWithFilter, mapPayload);
    });
};

const mergeCurrentMapStyleIntoPreset = (
    presetStyle: unknown,
    currentStyle?: unknown,
    mapPayload?: unknown
): Record<string, unknown> => {
    const presetRecord = asRecord(presetStyle);
    const currentRecord = asRecord(currentStyle);
    const presetLayers = Array.isArray(presetRecord.layers) ? presetRecord.layers.map(asRecord) : [];
    const currentLayers = Array.isArray(currentRecord.layers) ? currentRecord.layers.map(asRecord) : [];
    if (presetLayers.length === 0 || currentLayers.length === 0) return presetRecord;
    if (isCircleHeatmapStyleSwitch(presetRecord, currentRecord)) {
        // Circle and heatmap paints have different MapLibre semantics. Keep the
        // new preset paint and only carry filters forward.
        return {
            ...presetRecord,
            layers: mergeCurrentFiltersIntoPresetLayers(presetLayers, currentLayers, mapPayload)
        };
    }
    const currentColor = getCurrentMapStyleColor(currentStyle);
    const activeAttribute = toSuggestionString(currentRecord.attributeStyleKey || currentRecord.attributeKey);

    return {
        ...presetRecord,
        layers: presetLayers.flatMap((presetLayer, index) => {
            const presetId = toSuggestionString(presetLayer.id);
            const currentLayer = presetId
                ? currentLayers.find((layer) => toSuggestionString(layer.id) === presetId)
                : currentLayers[index];
            const baseMergedLayer = currentLayer
                ? mergeLayerEditsIntoPresetLayer(currentLayer, presetLayer)
                : presetLayer;
            const filteredAttributeLayers = buildFilteredAttributeColorLayers(baseMergedLayer, currentStyle, activeAttribute);
            if (filteredAttributeLayers) {
                return filteredAttributeLayers.map((layer) => sanitizeMapStyleLayerForPayload(layer, mapPayload));
            }
            const mergedLayer = currentLayer
                ? mergeLayerEditsIntoPresetLayer(currentLayer, presetLayer, currentColor)
                : presetLayer;
            return [
                sanitizeMapStyleLayerForPayload(
                    mergeFilteredAttributeColorsIntoLayer(mergedLayer, currentLayers, activeAttribute),
                    mapPayload
                )
            ];
        }),
        ...(currentRecord.attributeStyleKey ? { attributeStyleKey: currentRecord.attributeStyleKey } : {}),
        ...(currentRecord.attributeStyleType ? { attributeStyleType: currentRecord.attributeStyleType } : {}),
        ...(currentRecord.attributeKey ? { attributeKey: currentRecord.attributeKey } : {}),
        ...(currentRecord.attributeType ? { attributeType: currentRecord.attributeType } : {}),
        ...(currentRecord.attributeStyleVariants ? { attributeStyleVariants: currentRecord.attributeStyleVariants } : {})
    };
};

const collectMapStyleAttributeValues = (
    value: unknown,
    valuesByAttribute = new Map<string, Array<Record<string, unknown>>>(),
    paintKey?: string
): Map<string, Array<Record<string, unknown>>> => {
    if (Array.isArray(value)) {
        const [operator] = value;

        if (operator === 'interpolate') {
            const attributeKey = getMapStyleAttributeKey(value[2]);
            if (attributeKey) {
                const values = valuesByAttribute.get(attributeKey) || [];
                for (let index = 3; index < value.length - 1; index += 2) {
                    values.push({
                        value: value[index],
                        output: value[index + 1],
                        ...(paintKey ? { paintKey } : {})
                    });
                }
                valuesByAttribute.set(attributeKey, values);
            }
        }

        if (operator === 'match') {
            const attributeKey = getMapStyleAttributeKey(value[1]);
            if (attributeKey) {
                const values = valuesByAttribute.get(attributeKey) || [];
                for (let index = 2; index < value.length - 1; index += 2) {
                    values.push({
                        value: value[index],
                        output: value[index + 1],
                        ...(paintKey ? { paintKey } : {})
                    });
                }
                valuesByAttribute.set(attributeKey, values);
            }
        }

        if (['==', '!='].includes(String(operator))) {
            const attributeKey = getMapStyleAttributeKey(value[1]);
            const attributeValue = value[2];
            if (attributeKey && attributeValue !== undefined && attributeValue !== null && attributeValue !== '') {
                const values = valuesByAttribute.get(attributeKey) || [];
                values.push({
                    value: attributeValue,
                    ...(paintKey ? { paintKey } : {})
                });
                valuesByAttribute.set(attributeKey, values);
            }
        }

        if (['in', '!in'].includes(String(operator))) {
            const attributeKey = getMapStyleAttributeKey(value[1]);
            const literalValues = Array.isArray(value[2]) && value[2][0] === 'literal' && Array.isArray(value[2][1])
                ? value[2][1]
                : Array.isArray(value[2])
                    ? value[2]
                    : [];
            if (attributeKey && literalValues.length > 0) {
                const values = valuesByAttribute.get(attributeKey) || [];
                for (const attributeValue of literalValues) {
                    if (attributeValue === undefined || attributeValue === null || attributeValue === '') continue;
                    values.push({
                        value: attributeValue,
                        ...(paintKey ? { paintKey } : {})
                    });
                }
                valuesByAttribute.set(attributeKey, values);
            }
        }

        for (const item of value) {
            collectMapStyleAttributeValues(item, valuesByAttribute, paintKey);
        }

        return valuesByAttribute;
    }

    if (value && typeof value === 'object') {
        for (const [key, nestedValue] of Object.entries(value as Record<string, unknown>)) {
            collectMapStyleAttributeValues(nestedValue, valuesByAttribute, key);
        }
    }

    return valuesByAttribute;
};

const collectMapStylePaintKeys = (
    value: unknown,
    keys = new Set<string>()
): Set<string> => {
    if (Array.isArray(value)) {
        for (const item of value) {
            collectMapStylePaintKeys(item, keys);
        }
        return keys;
    }

    const record = asRecord(value);
    for (const [key, nestedValue] of Object.entries(record)) {
        if (key === 'paint') {
            for (const paintKey of Object.keys(asRecord(nestedValue))) {
                keys.add(paintKey);
            }
            continue;
        }
        collectMapStylePaintKeys(nestedValue, keys);
    }

    return keys;
};

const buildCurrentMapEditContext = (
    mapPayload: unknown,
    mapStyle: unknown
) => {
    const layerId = getLayerIdFromMapPayload(mapPayload) || getMapStyleLayerId(mapStyle);
    const title = getMapLayerTitle(mapPayload);
    const type = getMapLayerType(mapPayload);
    const styleKey = getMapStyleKey(mapStyle);
    const styleRecord = asRecord(mapStyle);
    const layerTypes = Array.from(getMapStyleLayerTypes(mapStyle));
    const paintKeys = Array.from(collectMapStylePaintKeys(mapStyle));
    const attributeKeys = Array.from(collectMapStyleAttributeKeys(mapStyle));
    const attributeStyleKey = toSuggestionString(styleRecord.attributeStyleKey || styleRecord.attributeKey);

    return {
        ...(layerId ? { layerId } : {}),
        ...(title ? { title } : {}),
        ...(type ? { type } : {}),
        ...(styleKey ? { styleKey } : {}),
        ...(layerTypes.length > 0 ? { layerTypes } : {}),
        ...(paintKeys.length > 0 ? { paintKeys } : {}),
        ...(attributeStyleKey ? { attributeStyleKey } : {}),
        ...(attributeKeys.length > 0 ? { attributeKeys } : {})
    };
};

const collectMapStylePropertyKeys = (
    value: unknown,
    keys = new Set<string>()
): Set<string> => {
    if (Array.isArray(value)) {
        for (const item of value) collectMapStylePropertyKeys(item, keys);
        return keys;
    }

    const record = asRecord(value);
    for (const [key, nestedValue] of Object.entries(record)) {
        if (key === 'paint' || key === 'layout') {
            for (const propertyKey of Object.keys(asRecord(nestedValue))) {
                keys.add(propertyKey);
            }
            continue;
        }
        collectMapStylePropertyKeys(nestedValue, keys);
    }
    return keys;
};

const getStyleOutputType = (paintKey: string, value: unknown): string => {
    if (typeof value === 'number') return 'number';
    if (typeof value === 'boolean') return 'boolean';
    if (typeof value === 'string') {
        return normalizeSuggestionColorValue(value) ? 'color' : 'string';
    }
    if (paintKey.endsWith('-color')) return 'color';
    return 'unknown';
};

const getMapStylePaintPropertyOptions = (
    mapStyle: unknown,
    attributeKey?: string
): Array<Record<string, unknown>> => {
    const styleValues = attributeKey
        ? collectMapStyleAttributeValues(mapStyle).get(attributeKey) || []
        : [];
    const outputsByPaintKey = new Map<string, unknown>();
    for (const styleValue of styleValues) {
        const paintKey = toSuggestionString(styleValue.paintKey);
        if (paintKey && !outputsByPaintKey.has(paintKey)) {
            outputsByPaintKey.set(paintKey, styleValue.output);
        }
    }

    return Array.from(collectMapStylePaintKeys(mapStyle)).map((paintKey) => ({
        value: paintKey,
        outputType: getStyleOutputType(paintKey, outputsByPaintKey.get(paintKey))
    }));
};

const resolveRequestedPaintKey = (
    mapStyle: unknown,
    instruction?: string,
    attributeKey?: string
): string | undefined => {
    const normalizedInstruction = normalizeStyleSwitchText(instruction);
    const options = getMapStylePaintPropertyOptions(mapStyle, attributeKey);
    const requested = options.find((option) => {
        const paintKey = toSuggestionString(option.value);
        return Boolean(
            paintKey
            && normalizedInstruction
            && normalizedInstruction.includes(normalizeStyleSwitchText(paintKey))
        );
    });
    if (requested) return toSuggestionString(requested.value);

    const attributePaintKeys = new Set(
        (attributeKey ? collectMapStyleAttributeValues(mapStyle).get(attributeKey) || [] : [])
            .map((item) => toSuggestionString(item.paintKey))
            .filter((paintKey): paintKey is string => Boolean(paintKey))
    );
    return attributePaintKeys.size === 1 ? Array.from(attributePaintKeys)[0] : undefined;
};

const isPaintStyleEditInstruction = (
    instruction: string,
    mapStyle: unknown
): boolean => {
    const normalizedInstruction = normalizeStyleSwitchText(instruction);
    if (!normalizedInstruction) return false;

    if (/[A-Za-z][\w]*-[A-Za-z][\w-]*/.test(instruction)) {
        return true;
    }

    return Array.from(collectMapStylePaintKeys(mapStyle)).some((paintKey) => {
        const normalizedPaintKey = normalizeStyleSwitchText(paintKey);
        return Boolean(normalizedPaintKey && normalizedInstruction.includes(normalizedPaintKey));
    });
};

const getSuggestionAttributeFields = (
    mapPayload: unknown,
    mapStyle: unknown,
    instruction?: string
): Array<{ name: string; type?: string }> => {
    const layer = getSuggestionLayerRecord(mapPayload);
    const fields = asRecord(asRecord(layer.attributes).fields);
    const usedAttributes = collectMapStyleAttributeKeys(mapStyle);
    const variantAttributes = getMapStyleAttributeVariantFields(mapStyle);
    const variantAttributeNames = new Set(variantAttributes.map((item) => item.name));
    const normalizedInstruction = normalizeStyleSwitchText(instruction);

    const fieldEntries = Object.entries(fields)
        .map(([name, definition]) => {
            const normalizedName = normalizeStyleSwitchText(name);
            const isStyleAttribute = usedAttributes.has(name);
            const isVariantAttribute = variantAttributeNames.has(name);
            const isRequestedAttribute = Boolean(
                normalizedInstruction
                && normalizedName
                && normalizedInstruction.includes(normalizedName)
            );
            if (!isStyleAttribute && !isVariantAttribute && !isRequestedAttribute) return undefined;

            const fieldType = toSuggestionString(asRecord(definition).type);
            return name.trim()
                ? {
                    name,
                    ...(fieldType ? { type: fieldType } : {})
                }
                : undefined;
        })
        .filter((field): field is { name: string; type?: string } => Boolean(field));
    const fieldNames = new Set(fieldEntries.map((field) => field.name));
    const variantEntries = variantAttributes.filter((field) => !fieldNames.has(field.name));

    return [...fieldEntries, ...variantEntries].slice(0, 8);
};

const hasSuggestionAttributeFields = (
    mapPayload: unknown,
    mapStyle: unknown
): boolean => {
    const layer = getSuggestionLayerRecord(mapPayload);
    const fields = asRecord(asRecord(layer.attributes).fields);
    if (Object.keys(fields).some((name) => Boolean(name.trim()))) return true;

    if (getMapStyleAttributeVariantFields(mapStyle).length > 0) return true;
    return collectMapStyleAttributeKeys(mapStyle).size > 0;
};

const buildAttributeStyleSuggestionItems = (
    mapPayload: unknown,
    mapStyle: unknown
) => {
    if (!hasSuggestionAttributeFields(mapPayload, mapStyle)) return [];

    return [{
        key: 'style_by_attribute',
        label: 'Add attribute for map style',
        promptTemplate: 'edit style the map by attribute '
    }];
};

const resolveRequestedSuggestionAttribute = (
    fields: Array<{ name: string; type?: string }>,
    instruction?: string
): { name: string; type?: string } | undefined => {
    const normalizedInstruction = normalizeStyleSwitchText(instruction);
    if (!normalizedInstruction) return undefined;

    return fields.find((field) => {
        const normalizedField = normalizeStyleSwitchText(field.name);
        return Boolean(normalizedField && normalizedInstruction.includes(normalizedField));
    });
};

const buildAttributeValueSuggestionItems = (
    mapPayload: unknown,
    mapStyle: unknown,
    instruction?: string
) => {
    const fields = getSuggestionAttributeFields(mapPayload, mapStyle, instruction);
    if (fields.length === 0) return [];

    const selectedField = resolveRequestedSuggestionAttribute(fields, instruction);
    if (!selectedField) return [];

    const styleValues = collectMapStyleAttributeValues(mapStyle).get(selectedField.name) || [];
    if (styleValues.length === 0) return [];

    return [{
        key: 'style_attribute_value',
        label: 'Choose attribute value ',
        attributeKey: selectedField.name,
        ...(selectedField.type ? { attributeType: selectedField.type } : {}),
        values: styleValues,
        promptTemplate: `Change ${selectedField.name} value {value} style property `
    }];
};

const buildAttributeFilterSuggestionItems = (
    mapPayload: unknown,
    mapStyle: unknown,
    instruction?: string
) => {
    const fields = getSuggestionAttributeFields(mapPayload, mapStyle, instruction);
    if (fields.length === 0) return [];

    const styleRecord = asRecord(mapStyle);
    const activeAttributeKey = toSuggestionString(styleRecord.attributeStyleKey || styleRecord.attributeKey);
    const selectedField = activeAttributeKey
        ? fields.find((field) => normalizeStyleSwitchText(field.name) === normalizeStyleSwitchText(activeAttributeKey))
        : resolveRequestedSuggestionAttribute(fields, instruction);
    if (!selectedField) return [];

    const isNumberAttribute = selectedField.type?.trim().toLowerCase() === 'number';

    return [{
        key: 'filter_by_attribute',
        label: 'Filter by attribute ',
        attributeKey: selectedField.name,
        ...(selectedField.type ? { attributeType: selectedField.type } : {}),
        promptTemplate: isNumberAttribute
            ? `Filter the map ${selectedField.name} greater than `
            : `Filter  the map ${selectedField.name} is `
    }];
};

const isAttributeValueStyleEditInstruction = (instruction?: string): boolean => {
    if (typeof instruction !== 'string') return false;
    return /\bvalue\s+\S+/i.test(instruction);
};

const isAttributeValueSuggestionInstruction = (instruction?: string): boolean => {
    if (typeof instruction !== 'string') return false;
    return /\bchoose\b.*\battribute\b|\bselect\b.*\battribute\b/i.test(instruction)
        || /เลือก.*attribute|attribute.*เลือก/i.test(instruction);
};

const buildMapSuggestionsPayload = (
    mapPayload: unknown,
    mapStyle: unknown,
    styleCatalog: unknown,
    _mapState?: ConversationMapState,
    instruction?: string
): Record<string, unknown> | undefined => {
    const layer = getSuggestionLayerRecord(mapPayload);
    const styleRecord = asRecord(mapStyle);
    const catalogRecord = asRecord(styleCatalog);
    const geometryType = normalizeSuggestionGeometryType(layer.geometryType || styleRecord.geometryType);
    const activeStyle = toSuggestionString(styleRecord.activeStyle) || toSuggestionString(styleRecord.styleKey);
    const styles = Array.isArray(catalogRecord.styles) ? catalogRecord.styles : [];
    const colors = Array.isArray(catalogRecord.colors) ? catalogRecord.colors : [];
    const attributeValueItems = buildAttributeValueSuggestionItems(
        mapPayload,
        mapStyle,
        instruction || toSuggestionString(styleRecord.styleInstruction)
    );
    const attributeFilterItems = buildAttributeFilterSuggestionItems(
        mapPayload,
        mapStyle,
        instruction || toSuggestionString(styleRecord.styleInstruction)
    );
    const hasSelectedAttribute = attributeValueItems.length > 0;
    const hideAttributeStyleSuggestion = hasSelectedAttribute
        && isAttributeValueSuggestionInstruction(instruction)
        && !isAttributeValueStyleEditInstruction(instruction);

    if (!styleRecord.success) return undefined;

    const styleOptions = styles
        .filter((style) => {
            if (!geometryType) return true;
            const styleGeometryTypes = getSuggestionStyleGeometryTypes(style);
            return styleGeometryTypes.length === 0 || styleGeometryTypes.includes(geometryType);
        })
        .map((style) => {
            const record = asRecord(style);
            const value = toSuggestionString(record.styleKey) || toSuggestionString(record.key);
            if (!value) return undefined;

            return {
                label: toSuggestionString(record.styleName) || value,
                value
            };
        })
        .filter((option): option is { label: string; value: string } => Boolean(option))
        .filter((option, index, allOptions) => {
            return allOptions.findIndex((item) => item.value === option.value) === index;
        });
    const activeStyleIndex = styleOptions.findIndex((option) => option.value === activeStyle);
    const nextStyleOption = styleOptions.length > 0
        ? styleOptions[(activeStyleIndex >= 0 ? activeStyleIndex + 1 : 0) % styleOptions.length]
        : undefined;

    const colorOptions = colors
        .map((color) => {
            const record = asRecord(color);
            const value = toSuggestionString(record.key);
            const colorValue = normalizeSuggestionColorValue(record.value);
            if (!value) return undefined;

            return {
                label: value,
                value,
                ...(colorValue ? { colorValue } : {})
            };
        })
        .filter((option): option is { label: string; value: string; colorValue?: string } => Boolean(option))
        .slice(0, 6);
    const currentColor = getCurrentMapStyleColor(mapStyle);
    const activeColorIndex = currentColor
        ? colorOptions.findIndex((option) => option.colorValue === currentColor)
        : -1;
    const nextColorOption = colorOptions.length > 0
        ? colorOptions[(activeColorIndex >= 0 ? activeColorIndex + 1 : 0) % colorOptions.length]!
        : undefined;

    const items = [
        ...(styleOptions.length > 1
            ? [{
                key: 'change_style',
                label: 'style to ',
                value: nextStyleOption?.value || nextStyleOption?.label,
                promptTemplate: 'Change the current map layer style to {value} '
            }]
            : []),
        ...(colorOptions.length > 0 && hasEditableColorPaint(mapStyle)
            ? [{
                key: 'change_color',
                label: 'color style to ',
                value: nextColorOption?.value,
                promptTemplate: 'Change the current map layer primary color to {value}'
            }]
            : []),
        ...(hideAttributeStyleSuggestion ? [] : buildAttributeStyleSuggestionItems(mapPayload, mapStyle)),
        ...attributeValueItems,
        ...attributeFilterItems,
        ...buildClearMapSuggestionItems()
    ];

    if (items.length === 0) return undefined;

    return {
        items
    };
};

const enrichMapSuggestionsWithAttributeValues = async (
    suggestionsPayload: Record<string, unknown> | undefined,
    mapPayload: unknown,
    userId: string,
    headerApiKey?: string,
    includeAttributeValues = true
): Promise<Record<string, unknown> | undefined> => {
    const items = Array.isArray(asRecord(suggestionsPayload).items)
        ? asRecord(suggestionsPayload).items as unknown[]
        : [];
    if (items.length === 0) return suggestionsPayload;

    const attributeValueIndex = items.findIndex((item) => asRecord(item).key === 'style_attribute_value');
    if (attributeValueIndex < 0) return suggestionsPayload;

    const attributeValueItem = asRecord(items[attributeValueIndex]);
    const attributeKey = toSuggestionString(attributeValueItem.attributeKey);
    if (!attributeKey) return suggestionsPayload;
    const nextItems = [...items];
    const { values: _ignoredSuggestionValues, ...publicAttributeValueItem } = attributeValueItem;
    nextItems[attributeValueIndex] = publicAttributeValueItem;
    const publicSuggestionsPayload = {
        ...suggestionsPayload,
        items: nextItems
    };
    if (!includeAttributeValues) return publicSuggestionsPayload;

    const payloadRecord = asRecord(mapPayload);
    const layerRecord = asRecord(payloadRecord.layer);
    const attributes = asRecord(layerRecord.attributes);
    const intentName = toSuggestionString(payloadRecord.intentName || layerRecord.intentName);
    const provider = toSuggestionString(payloadRecord.provider || layerRecord.provider);
    const layerId = toSuggestionString(layerRecord.layerId || payloadRecord.layerId || layerRecord.id);
    const datasetId = toSuggestionString(attributes.datasetId || attributes.dataset_id || layerRecord.datasetId || payloadRecord.datasetId);
    if (!intentName || !provider || !datasetId) return publicSuggestionsPayload;

    const valuesResult = await handleMapAttributeValuesTool(
        userId,
        {
            intentName,
            provider,
            layerId,
            datasetId,
            attributeKey,
            attributeType: attributeValueItem.attributeType
        },
        headerApiKey
    );
    const dataValues = Array.isArray(asRecord(valuesResult).values)
        ? asRecord(valuesResult).values as unknown[]
        : [];
    if (!asRecord(valuesResult).success || dataValues.length === 0) return publicSuggestionsPayload;
    const suggestionValues = dataValues
        .map((item) => {
            const record = asRecord(item);
            return record.value !== undefined ? record.value : item;
        })
        .filter((value) => value !== undefined && value !== null && value !== '');

    nextItems[attributeValueIndex] = {
        ...publicAttributeValueItem,
        valueSource: 'data',
        ...(asRecord(valuesResult).stats ? { stats: asRecord(valuesResult).stats } : {}),
        ...(asRecord(valuesResult).numberMatched !== undefined
            ? { numberMatched: asRecord(valuesResult).numberMatched }
            : {})
    };

    return {
        ...publicSuggestionsPayload,
        attributeValues: {
            layerId,
            attributeKey,
            ...(attributeValueItem.attributeType ? { attributeType: attributeValueItem.attributeType } : {}),
            values: suggestionValues,
            ...(asRecord(valuesResult).stats ? { stats: asRecord(valuesResult).stats } : {}),
            ...(asRecord(valuesResult).numberMatched !== undefined
                ? { numberMatched: asRecord(valuesResult).numberMatched }
                : {})
        },
        items: nextItems
    };
};

const splitMapSuggestionsPayload = (
    payload: Record<string, unknown> | undefined
): {
    suggestions?: Record<string, unknown>;
    attributeValues?: Record<string, unknown>;
} => {
    if (!payload) return {};
    const { attributeValues, ...suggestions } = payload;
    return {
        ...(Object.keys(suggestions).length > 0 ? { suggestions } : {}),
        ...(Object.keys(asRecord(attributeValues)).length > 0
            ? { attributeValues: asRecord(attributeValues) }
            : {})
    };
};

const findMapStyleAttributeExpression = (
    value: unknown,
    attributeKey: string,
    requestedPaintKey?: string,
    paintKey?: string
): { paintKey: string; expression: unknown[] } | undefined => {
    if (Array.isArray(value)) {
        const operator = value[0];
        const expressionAttribute = operator === 'interpolate'
            ? getMapStyleAttributeKey(value[2])
            : operator === 'match'
                ? getMapStyleAttributeKey(value[1])
                : undefined;
        if (
            (operator === 'interpolate' || operator === 'match')
            && expressionAttribute === attributeKey
            && paintKey
            && (!requestedPaintKey || requestedPaintKey === paintKey)
        ) {
            return { paintKey, expression: value };
        }
        for (const item of value) {
            const found = findMapStyleAttributeExpression(item, attributeKey, requestedPaintKey, paintKey);
            if (found) return found;
        }
        return undefined;
    }

    for (const [key, nestedValue] of Object.entries(asRecord(value))) {
        const found = findMapStyleAttributeExpression(
            nestedValue,
            attributeKey,
            requestedPaintKey,
            key === 'paint' ? paintKey : key
        );
        if (found) return found;
    }
    return undefined;
};

const getRequestedAttributePatchValues = (args: Record<string, unknown>): unknown[] => {
    const values = [
        ...(args.attributeValue !== undefined ? [args.attributeValue] : []),
        ...(Array.isArray(args.attributePatches)
            ? args.attributePatches
                .map((item) => asRecord(item).attributeValue)
                .filter((value) => value !== undefined)
            : [])
    ];
    return values.filter((value, index) => {
        const serialized = JSON.stringify(value);
        return values.findIndex((item) => JSON.stringify(item) === serialized) === index;
    });
};

const hasAttributeRampOutputRequest = (args: Record<string, unknown>): boolean => {
    if (Array.isArray(args.outputs) && args.outputs.length > 0) return true;
    return Boolean(
        normalizeSuggestionColorValue(args.colorValue)
        || normalizeSuggestionColorValue(args.value)
    );
};

const getVisionDominantColorPalette = (vision?: VisionAnalysis | null): string[] => {
    return (Array.isArray(vision?.dominantColors) ? vision.dominantColors : [])
        .map((color) => normalizeSuggestionColorValue(color.hex))
        .filter((color): color is string => Boolean(color));
};

const instructionReferencesImageColors = (instruction: unknown): boolean => {
    const normalized = normalizeStyleSwitchText(toSuggestionString(instruction));
    if (!normalized) return false;
    return /\b(image|photo|picture)\b/.test(normalized);
};

const enrichStyleArgsWithVisionPalette = (
    args: Record<string, unknown>,
    instruction: unknown,
    vision?: VisionAnalysis | null
): Record<string, unknown> => {
    if (Array.isArray(args.outputs) && args.outputs.length > 0) return args;
    if (!instructionReferencesImageColors(instruction)) return args;

    const palette = getVisionDominantColorPalette(vision);
    if (palette.length === 0) return args;

    return {
        ...args,
        outputs: palette
    };
};

const buildAttributeMapStylePatch = (
    args: Record<string, unknown>,
    mapStyle: unknown
): Record<string, unknown> | undefined => {
    const attributeKey = toSuggestionString(args.attributeKey);
    if (!attributeKey) return undefined;
    const requestedValues = getRequestedAttributePatchValues(args);
    if (requestedValues.length === 0) return undefined;

    const requestedPaintKey = toSuggestionString(args.paintKey)
        || resolveRequestedPaintKey(mapStyle, toSuggestionString(args.instruction), attributeKey);
    const found = findMapStyleAttributeExpression(mapStyle, attributeKey, requestedPaintKey);
    if (!found) return undefined;

    const [operator] = found.expression;
    const startIndex = operator === 'interpolate' ? 3 : operator === 'match' ? 2 : -1;
    if (startIndex < 0) return undefined;
    const stops: Array<Record<string, unknown>> = [];
    for (let index = startIndex; index < found.expression.length - 1; index += 2) {
        stops.push({
            value: found.expression[index],
            output: found.expression[index + 1]
        });
    }
    if (stops.length === 0) return undefined;

    const selectedStops = stops.filter((stop) => (
        requestedValues.some((value) => JSON.stringify(value) === JSON.stringify(stop.value))
    ));
    if (selectedStops.length === 0) return undefined;
    const firstOutput = selectedStops[0]?.output;

    return {
        event: 'map_style_patch',
        layerId: getMapStyleLayerId(mapStyle),
        attributeKey,
        paintKey: found.paintKey,
        outputType: getStyleOutputType(found.paintKey, firstOutput),
        operation: 'update_stops',
        patches: selectedStops.map((stop) => ({
            attributeValue: stop.value,
            output: stop.output
        }))
    };
};

const buildMapFilterPatch = (
    args: Record<string, unknown>,
    mapStyle: unknown
): Record<string, unknown> | undefined => {
    const operation = normalizeMapEditOperationName(args.operation || args.action);
    if (operation !== 'add_filter') {
        return undefined;
    }

    const styleRecord = asRecord(mapStyle);
    const layers = Array.isArray(styleRecord.layers) ? styleRecord.layers.map(asRecord) : [];
    const requestedStyleLayerId = toSuggestionString(args.styleLayerId);
    const requestedTarget = toSuggestionString(args.target)?.toLowerCase();
    const availableStyleLayerIds = new Set(
        layers
            .map((layer) => toSuggestionString(layer.id))
            .filter((value): value is string => Boolean(value))
    );
    const availableLayerTargets = new Set(
        layers
            .flatMap((layer) => [
                toSuggestionString(layer.id)?.toLowerCase(),
                toSuggestionString(layer.type)?.toLowerCase()
            ])
            .filter((value): value is string => Boolean(value))
    );
    const target = requestedStyleLayerId && availableStyleLayerIds.has(requestedStyleLayerId)
        ? requestedStyleLayerId
        : requestedTarget && availableLayerTargets.has(requestedTarget)
            ? requestedTarget
            : undefined;
    const selectedLayers = layers.filter((layer) => (
        !target
        || toSuggestionString(layer.id) === target
        || toSuggestionString(layer.type)?.toLowerCase() === target.toLowerCase()
    ));
    if (selectedLayers.length === 0) return undefined;
    const patches = selectedLayers.map((layer) => ({
        ...(toSuggestionString(layer.id) ? { styleLayerId: toSuggestionString(layer.id) } : {}),
        ...(toSuggestionString(layer.type) ? { layerType: toSuggestionString(layer.type) } : {}),
        filter: layer.filter ?? null
    }));

    return {
        event: 'map_filter_patch',
        layerId: getMapStyleLayerId(mapStyle),
        operation,
        patches
    };
};

const extractMapAttributeNameFromInstruction = (instruction?: string): string | undefined => {
    if (!instruction) return undefined;

    const patterns = [
        /\battribute\s+([A-Za-z_][A-Za-z0-9_-]*)/i,
        /\bfield\s+([A-Za-z_][A-Za-z0-9_-]*)/i,
        /\bproperty\s+([A-Za-z_][A-Za-z0-9_-]*)/i
    ];
    for (const pattern of patterns) {
        const match = instruction.match(pattern);
        if (match?.[1]) return match[1];
    }

    return undefined;
};

const resolveRequestedMapAttribute = (
    mapPayload: unknown,
    instruction?: string,
    explicitAttributeKey?: unknown
): { name: string; type?: string } | undefined => {
    const layer = getSuggestionLayerRecord(mapPayload);
    const fields = asRecord(asRecord(layer.attributes).fields);
    const explicitKey = toSuggestionString(explicitAttributeKey);
    const normalizedInstruction = normalizeStyleSwitchText(instruction);

    const entries = Object.entries(fields)
        .map(([name, definition]) => ({
            name,
            type: toSuggestionString(asRecord(definition).type)
        }))
        .filter((field) => Boolean(field.name.trim()));

    if (explicitKey) {
        const normalizedExplicitKey = normalizeStyleSwitchText(explicitKey);
        const explicitMatch = entries.find((field) => normalizeStyleSwitchText(field.name) === normalizedExplicitKey);
        if (explicitMatch) return explicitMatch;
        return { name: explicitKey };
    }

    if (!normalizedInstruction) return undefined;

    const fieldMatch = entries
        .filter((field) => {
            const normalizedName = normalizeStyleSwitchText(field.name);
            return normalizedName && normalizedInstruction.includes(normalizedName);
        })
        .sort((left, right) => right.name.length - left.name.length)[0];
    if (fieldMatch) return fieldMatch;

    const inferredAttribute = extractMapAttributeNameFromInstruction(instruction);
    return inferredAttribute ? { name: inferredAttribute } : undefined;
};

const getMapPayloadAttributeFieldByName = (
    mapPayload: unknown,
    attributeKey: string
): { name: string; type?: string } => {
    const layer = getSuggestionLayerRecord(mapPayload);
    const fields = asRecord(asRecord(layer.attributes).fields);
    const fieldType = toSuggestionString(asRecord(fields[attributeKey]).type);
    return {
        name: attributeKey,
        ...(fieldType ? { type: fieldType } : {})
    };
};

const getStyleAttributeValueItems = (
    mapStyle: unknown,
    attributeKey: string
): Array<Record<string, unknown>> => {
    const values = [
        ...(collectMapStyleAttributeValues(mapStyle).get(attributeKey) || []),
        ...(collectMapStyleAttributeValues(getMapStyleAttributeVariants(mapStyle).get(attributeKey)).get(attributeKey) || [])
    ];
    return values.filter((item, index) => {
        const serialized = JSON.stringify(asRecord(item).value ?? item);
        return values.findIndex((candidate) => {
            return JSON.stringify(asRecord(candidate).value ?? candidate) === serialized;
        }) === index;
    });
};

const styleAttributeHasValue = (
    mapStyle: unknown,
    attributeKey: string | undefined,
    value: unknown
): boolean => {
    if (!attributeKey || value === undefined || value === null || value === '') return false;
    return getStyleAttributeValueItems(mapStyle, attributeKey).some((item) => {
        const itemValue = asRecord(item).value ?? item;
        return normalizeStyleSwitchText(itemValue) === normalizeStyleSwitchText(value);
    });
};

const resolveFilterConditionAttributeFromStyleValues = (
    mapPayload: unknown,
    mapStyle: unknown,
    condition: Record<string, unknown>
): { name: string; type?: string } | undefined => {
    const conditionValue = condition.value
        ?? (Array.isArray(condition.values) ? condition.values.find((value) => value !== undefined && value !== null && value !== '') : undefined);
    if (conditionValue === undefined || conditionValue === null || conditionValue === '') return undefined;

    const currentAttributeKey = toSuggestionString(condition.attributeKey || condition.field || condition.key);
    if (styleAttributeHasValue(mapStyle, currentAttributeKey, conditionValue)) {
        return undefined;
    }

    const styleRecord = asRecord(mapStyle);
    const activeAttribute = toSuggestionString(styleRecord.attributeStyleKey || styleRecord.attributeKey);
    const variants = getMapStyleAttributeVariants(mapStyle);
    const candidateNames = [
        ...(activeAttribute ? [activeAttribute] : []),
        ...Array.from(variants.keys()),
        ...Array.from(collectMapStyleAttributeValues(mapStyle).keys())
    ].filter((name, index, names) => names.indexOf(name) === index);

    for (const name of candidateNames) {
        if (name === currentAttributeKey) continue;
        if (!styleAttributeHasValue(mapStyle, name, conditionValue)) continue;

        const variant = asRecord(variants.get(name));
        const variantType = toSuggestionString(variant.attributeType || variant.type);
        const field = getMapPayloadAttributeFieldByName(mapPayload, name);
        return {
            ...field,
            ...(field.type ? {} : variantType ? { type: variantType } : {})
        };
    }

    return undefined;
};

const instructionMentionsStyleAttributeValue = (
    instruction: string | undefined,
    values: Array<Record<string, unknown>>
): boolean => {
    const normalizedInstruction = normalizeStyleSwitchText(instruction);
    if (!normalizedInstruction) return false;

    return values.some((item) => {
        const value = asRecord(item).value ?? item;
        const terms = typeof value === 'string'
            ? getLayerTextMatchTerms(value)
            : [normalizeStyleSwitchText(value)].filter(Boolean);
        return terms.some((term) => normalizedInstruction.includes(term));
    });
};

const resolveRequestedMapAttributeFromStyleValues = (
    mapPayload: unknown,
    mapStyle: unknown,
    instruction?: string
): { name: string; type?: string } | undefined => {
    const normalizedInstruction = normalizeStyleSwitchText(instruction);
    if (!normalizedInstruction) return undefined;

    const styleRecord = asRecord(mapStyle);
    const activeAttribute = toSuggestionString(styleRecord.attributeStyleKey || styleRecord.attributeKey);
    const attributesByName = collectMapStyleAttributeValues(mapStyle);
    const variants = getMapStyleAttributeVariants(mapStyle);
    const candidateNames = [
        ...(activeAttribute ? [activeAttribute] : []),
        ...Array.from(variants.keys()),
        ...Array.from(attributesByName.keys())
    ].filter((name, index, names) => names.indexOf(name) === index);

    for (const name of candidateNames) {
        const values = getStyleAttributeValueItems(mapStyle, name);
        if (!instructionMentionsStyleAttributeValue(instruction, values)) continue;

        const variant = asRecord(variants.get(name));
        const variantType = toSuggestionString(variant.attributeType || variant.type);
        const field = getMapPayloadAttributeFieldByName(mapPayload, name);
        return {
            ...field,
            ...(field.type ? {} : variantType ? { type: variantType } : {})
        };
    }

    return undefined;
};

const buildAttributePatchesFromInstruction = async (
    instruction: string | undefined,
    attributeKey: string,
    values: unknown
): Promise<Array<Record<string, unknown>>> => {
    if (!instruction || !Array.isArray(values)) return [];
    const normalizedInstruction = instruction.toLowerCase();
    const valueMatches = values
        .map((item) => asRecord(item).value ?? item)
        .map((value) => {
            const text = String(value).trim();
            const index = text ? normalizedInstruction.indexOf(text.toLowerCase()) : -1;
            return index >= 0 ? { value, index } : undefined;
        })
        .filter((item): item is { value: unknown; index: number } => Boolean(item))
        .sort((left, right) => left.index - right.index);
    if (valueMatches.length === 0) return [];

    const styleCatalog = await handleStyleCatalogTool();
    const colors = styleCatalog.success && Array.isArray(styleCatalog.colors)
        ? styleCatalog.colors
        : [];
    const colorMatches = colors
        .map((item) => asRecord(item))
        .map((item) => {
            const key = toSuggestionString(item.key);
            const colorValue = normalizeSuggestionColorValue(item.value);
            const index = key ? normalizedInstruction.indexOf(key.toLowerCase()) : -1;
            return key && colorValue && index >= 0 ? { key, colorValue, index } : undefined;
        })
        .filter((item): item is { key: string; colorValue: string; index: number } => Boolean(item))
        .sort((left, right) => left.index - right.index);
    if (colorMatches.length === 0) return [];

    return valueMatches.flatMap((valueMatch, index) => {
        const nextValueIndex = valueMatches[index + 1]?.index ?? Number.POSITIVE_INFINITY;
        const colorMatch = colorMatches.find((color) => (
            color.index > valueMatch.index && color.index < nextValueIndex
        ));
        return colorMatch
            ? [{
                attributeKey,
                attributeValue: valueMatch.value,
                colorValue: colorMatch.colorValue
            }]
            : [];
    });
};

const buildAttributeEditArgs = async (
    baseArgs: Record<string, unknown>,
    mapPayload: unknown,
    userId: string,
    instruction?: string,
    headerApiKey?: string,
    mapStyle?: unknown
): Promise<Record<string, unknown>> => {
    const requestedAttribute = resolveRequestedMapAttribute(
        mapPayload,
        instruction,
        baseArgs.attributeKey
    ) || resolveRequestedMapAttributeFromStyleValues(mapPayload, mapStyle, instruction);
    if (!requestedAttribute) return baseArgs;
    const { attributeValues: _ignoredAttributeValues, attributeStats: _ignoredAttributeStats, ...trustedBaseArgs } = baseArgs;
    const suppliedAttributeKey = toSuggestionString(trustedBaseArgs.attributeKey);
    const suppliedAttributeKeyIsStyleValue = Boolean(
        suppliedAttributeKey
        && suppliedAttributeKey !== requestedAttribute.name
        && styleAttributeHasValue(mapStyle, requestedAttribute.name, suppliedAttributeKey)
    );
    const normalizedTrustedArgs = suppliedAttributeKeyIsStyleValue
        ? {
            ...trustedBaseArgs,
            attributeValue: trustedBaseArgs.attributeValue ?? suppliedAttributeKey,
            operation: 'update_layer',
            action: 'update_layer'
        }
        : trustedBaseArgs;

    const payloadRecord = asRecord(mapPayload);
    const layerRecord = asRecord(payloadRecord.layer);
    const attributes = asRecord(layerRecord.attributes);
    const intentName = toSuggestionString(payloadRecord.intentName || layerRecord.intentName);
    const provider = toSuggestionString(payloadRecord.provider || layerRecord.provider);
    const layerId = toSuggestionString(layerRecord.layerId || payloadRecord.layerId || layerRecord.id);
    const datasetId = toSuggestionString(attributes.datasetId || attributes.dataset_id || layerRecord.datasetId || payloadRecord.datasetId);
    const styleAttributeValues = (collectMapStyleAttributeValues(mapStyle).get(requestedAttribute.name) || [])
        .map((item) => item.value)
        .filter((value) => value !== undefined && value !== null && value !== '');

    if (!intentName || !provider || !datasetId) {
        const inferredAttributePatches = await buildAttributePatchesFromInstruction(
            instruction,
            requestedAttribute.name,
            styleAttributeValues
        );
        return {
            ...normalizedTrustedArgs,
            attributeKey: requestedAttribute.name,
            ...(requestedAttribute.type ? { attributeType: requestedAttribute.type } : {}),
            ...(styleAttributeValues.length > 0 ? { attributeValues: styleAttributeValues } : {}),
            ...(inferredAttributePatches.length > 0 ? { attributePatches: inferredAttributePatches } : {})
        };
    }

    const valuesResult = await handleMapAttributeValuesTool(
        userId,
        {
            intentName,
            provider,
            layerId,
            datasetId,
            attributeKey: requestedAttribute.name,
            attributeType: requestedAttribute.type
        },
        headerApiKey
    );
    const apiAttributeValues = valuesResult.success === true && Array.isArray(asRecord(valuesResult).values)
        ? asRecord(valuesResult).values as unknown[]
        : [];
    const attributeValues = [...apiAttributeValues, ...styleAttributeValues]
        .filter((value, index, values) => {
            const current = JSON.stringify(asRecord(value).value ?? value);
            return values.findIndex((item) => JSON.stringify(asRecord(item).value ?? item) === current) === index;
        });
    const explicitAttributePatches = Array.isArray(normalizedTrustedArgs.attributePatches)
        ? normalizedTrustedArgs.attributePatches
        : [];
    const inferredAttributePatches = explicitAttributePatches.length === 0
        ? await buildAttributePatchesFromInstruction(
            instruction,
            requestedAttribute.name,
            attributeValues
        )
        : [];

    return {
        ...normalizedTrustedArgs,
        attributeKey: requestedAttribute.name,
        ...(requestedAttribute.type ? { attributeType: requestedAttribute.type } : {}),
        ...(attributeValues.length > 0 ? { attributeValues } : {}),
        ...(inferredAttributePatches.length > 0 ? { attributePatches: inferredAttributePatches } : {}),
        ...(asRecord(valuesResult).stats ? { attributeStats: asRecord(valuesResult).stats } : {})
    };
};

const buildFilterEditArgs = (
    baseArgs: Record<string, unknown>,
    mapPayload: unknown,
    mapStyle?: unknown
): Record<string, unknown> => {
    const operation = normalizeMapEditOperationName(baseArgs.operation || baseArgs.action);
    if (!operation?.endsWith('_filter')) return baseArgs;

    const layer = getSuggestionLayerRecord(mapPayload);
    const fields = asRecord(asRecord(layer.attributes).fields);
    const attributeFields: Record<string, unknown> = { ...fields };
    const styleRecord = asRecord(mapStyle);
    const styleAttributeKey = toSuggestionString(styleRecord.attributeStyleKey || styleRecord.attributeKey);
    const styleAttributeType = toSuggestionString(styleRecord.attributeStyleType || styleRecord.attributeType);
    // Some edited attributes exist only in the current map_style metadata. Merge
    // them into fields so filter prompts still work after several style edits.
    if (styleAttributeKey && styleAttributeType && attributeFields[styleAttributeKey] === undefined) {
        attributeFields[styleAttributeKey] = { type: styleAttributeType };
    }
    for (const variant of getMapStyleAttributeVariants(mapStyle).values()) {
        const name = toSuggestionString(variant.attributeKey || variant.name || variant.value);
        const type = toSuggestionString(variant.attributeType || variant.type);
        if (name && type && attributeFields[name] === undefined) {
            attributeFields[name] = { type };
        }
    }
    const normalizedFilterConditions = Array.isArray(baseArgs.filterConditions)
        ? baseArgs.filterConditions.map((condition) => {
            const record = asRecord(condition);
            const resolvedAttribute = resolveFilterConditionAttributeFromStyleValues(mapPayload, mapStyle, record);
            if (!resolvedAttribute) return condition;
            return {
                ...record,
                attributeKey: resolvedAttribute.name,
                ...(resolvedAttribute.type ? { attributeType: resolvedAttribute.type } : {})
            };
        })
        : undefined;
    for (const condition of normalizedFilterConditions || []) {
        const record = asRecord(condition);
        const attributeKey = toSuggestionString(record.attributeKey || record.field || record.key);
        if (!attributeKey || attributeFields[attributeKey] !== undefined) continue;
        const fallbackType = attributeKey === styleAttributeKey
            ? styleAttributeType
            : toSuggestionString(asRecord(getMapStyleAttributeVariants(mapStyle).get(attributeKey)).attributeType);
        const conditionValue = record.value ?? (Array.isArray(record.values) ? record.values.find((value) => value !== undefined && value !== null) : undefined);
        const inferredType = fallbackType
            || (typeof conditionValue === 'number'
                ? 'Number'
                : typeof conditionValue === 'boolean'
                    ? 'Boolean'
                    : conditionValue !== undefined && conditionValue !== null
                        ? 'String'
                        : undefined);
        if (inferredType) attributeFields[attributeKey] = { type: inferredType };
    }
    if (Object.keys(attributeFields).length === 0) return baseArgs;

    return {
        ...baseArgs,
        ...(normalizedFilterConditions ? { filterConditions: normalizedFilterConditions } : {}),
        attributeFields
    };
};

const getMapLibreFilterConditionAttribute = (filter: unknown): string | undefined => {
    if (!Array.isArray(filter)) return undefined;
    const getExpression = filter.find((item) => Array.isArray(item) && item[0] === 'get');
    return Array.isArray(getExpression) ? toSuggestionString(getExpression[1]) : undefined;
};

const flattenMapLibreFilterConditions = (filter: unknown): unknown[][] => {
    if (!Array.isArray(filter) || filter.length === 0) return [];
    const operator = toSuggestionString(filter[0])?.toLowerCase();
    return operator === 'all' || operator === 'any'
        ? filter.slice(1).filter(Array.isArray) as unknown[][]
        : [filter];
};

const getFilterLogicForInferredCondition = (
    operation: string,
    attributeKey: string,
    mapStyle?: unknown
): 'all' | 'any' => {
    if (operation !== 'add_filter') return 'all';

    const styleRecord = asRecord(mapStyle);
    const layers = Array.isArray(styleRecord.layers) ? styleRecord.layers.map(asRecord) : [];
    const currentConditions = layers.flatMap((layer) => flattenMapLibreFilterConditions(layer.filter));
    return currentConditions.some((condition) => getMapLibreFilterConditionAttribute(condition) === attributeKey)
        ? 'any'
        : 'all';
};

const escapeFilterValuePattern = (value: string): string => {
    return value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
};

const getFilterValueText = (value: unknown): string | undefined => {
    const rawValue = asRecord(value).value ?? value;
    if (rawValue === undefined || rawValue === null || rawValue === '') return undefined;
    return String(rawValue);
};

const findFilterValueTextMatch = (
    instruction: string,
    valueText: string
): { start: number; end: number } | undefined => {
    const trimmedValue = valueText.trim();
    if (!trimmedValue) return undefined;

    if (/^[A-Za-z0-9_ -]+$/.test(trimmedValue)) {
        const pattern = new RegExp(`(^|[^A-Za-z0-9_])${escapeFilterValuePattern(trimmedValue)}([^A-Za-z0-9_]|$)`, 'i');
        const match = pattern.exec(instruction);
        if (!match) return undefined;

        const prefixLength = match[1]?.length || 0;
        const start = match.index + prefixLength;
        return {
            start,
            end: start + trimmedValue.length
        };
    }

    const start = instruction.toLowerCase().indexOf(trimmedValue.toLowerCase());
    return start >= 0
        ? { start, end: start + trimmedValue.length }
        : undefined;
};

const resolveFilterValueFromInstruction = (
    instruction: string | undefined,
    values: unknown[]
): unknown | undefined => {
    return resolveFilterValuesFromInstruction(instruction, values)[0];
};

const resolveFilterValuesFromInstruction = (
    instruction: string | undefined,
    values: unknown[]
): unknown[] => {
    if (!instruction?.trim()) return [];

    const rawMatches = values
        .map((item) => asRecord(item).value ?? item)
        .filter((value) => value !== undefined && value !== null && value !== '')
        .sort((left, right) => String(right).length - String(left).length)
        .map((value) => {
            const valueText = getFilterValueText(value);
            const match = valueText ? findFilterValueTextMatch(instruction, valueText) : undefined;
            return match ? { value, ...match } : undefined;
        })
        .filter((item): item is { value: unknown; start: number; end: number } => Boolean(item))
        .filter((match, index, matchedValues) => {
            const { value } = match;
            const serialized = JSON.stringify(value);
            return matchedValues.findIndex((item) => JSON.stringify(item.value) === serialized) === index;
        });
    if (rawMatches.length > 0) {
        const orderedMatches = [...rawMatches].sort((left, right) => left.start - right.start);
        const hasOnlyWhitespaceBetweenMatches = orderedMatches.length > 1 && orderedMatches
            .slice(1)
            .every((match, index) => {
                const previous = orderedMatches[index];
                return instruction.slice(previous.end, match.start).trim().length === 0;
            });
        if (!hasOnlyWhitespaceBetweenMatches) {
            return rawMatches.map((match) => match.value);
        }
    }

    const normalizedInstruction = normalizeStyleSwitchText(instruction);
    if (!normalizedInstruction) return [];

    const normalizedMatches = values
        .map((item) => asRecord(item).value ?? item)
        .filter((value) => value !== undefined && value !== null && value !== '')
        .sort((left, right) => String(right).length - String(left).length)
        .filter((value, index, matchedValues) => {
            const normalizedValue = normalizeStyleSwitchText(value);
            if (!normalizedValue || !normalizedInstruction.includes(normalizedValue)) return false;
            const serialized = JSON.stringify(value);
            return matchedValues.findIndex((item) => JSON.stringify(item) === serialized) === index;
        });

    const keptNormalizedValues: string[] = [];
    return normalizedMatches.filter((value) => {
        const normalizedValue = normalizeStyleSwitchText(value);
        if (!normalizedValue) return false;
        if (keptNormalizedValues.some((kept) => kept !== normalizedValue && kept.includes(normalizedValue))) {
            return false;
        }
        keptNormalizedValues.push(normalizedValue);
        return true;
    });
};

const getUniqueAttributeFilterValues = (values: unknown[]): unknown[] => {
    return values
        .map((item) => asRecord(item).value ?? item)
        .filter((value) => value !== undefined && value !== null && value !== '')
        .filter((value, index, items) => {
            const serialized = JSON.stringify(value);
            return items.findIndex((item) => JSON.stringify(item) === serialized) === index;
        });
};

const normalizeMapEditOperationName = (value: unknown): string | undefined => {
    return toSuggestionString(value)
        ?.trim()
        .toLowerCase()
        .replace(/[\s-]+/g, '_');
};

const hasFilterPayloadArgs = (args: Record<string, unknown>): boolean => {
    return [args.filter, args.filterConditions].some((value) => {
        if (Array.isArray(value)) return value.length > 0;
        if (value && typeof value === 'object') return Object.keys(asRecord(value)).length > 0;
        return value !== undefined && value !== null && value !== '';
    });
};

const isFilterIntentInstruction = (instruction: string | undefined): boolean => {
    const text = toSuggestionString(instruction)?.toLowerCase();
    if (!text?.trim()) return false;
    return /\b(filter|where|only|limit|include|exclude)\b/i.test(text)
        || /\b(show|display)\s+only\b/i.test(text);
};

const isStyleIntentInstruction = (instruction: string | undefined): boolean => {
    const text = toSuggestionString(instruction)?.toLowerCase();
    if (!text?.trim()) return false;
    return /\b(style|paint|color|colour|edit|change|update|apply)\b/i.test(text);
};

const hasExplicitStyleEditArgs = (args: Record<string, unknown>): boolean => {
    return [
        args.paintKey,
        args.layoutKey,
        args.colorKey,
        args.colorValue,
        args.attributeValue,
        args.attributePatches,
        args.outputs,
        args.fallbackOutput,
        args.value,
        args.paint,
        args.layout
    ].some((value) => {
        if (Array.isArray(value)) return value.length > 0;
        if (value && typeof value === 'object') return Object.keys(asRecord(value)).length > 0;
        return value !== undefined && value !== null && value !== '';
    });
};

const resolveActiveMapStyleAttribute = (
    mapPayload: unknown,
    mapStyle?: unknown
): { name: string; type?: string } | undefined => {
    const styleRecord = asRecord(mapStyle);
    const attributeKey = toSuggestionString(styleRecord.attributeStyleKey || styleRecord.attributeKey);
    if (!attributeKey) return undefined;

    const payloadField = getMapPayloadAttributeFieldByName(mapPayload, attributeKey);
    const attributeType = toSuggestionString(styleRecord.attributeStyleType || styleRecord.attributeType)
        || payloadField.type
        || toSuggestionString(asRecord(getMapStyleAttributeVariants(mapStyle).get(attributeKey)).attributeType);

    return {
        name: attributeKey,
        ...(attributeType ? { type: attributeType } : {})
    };
};

const resolveRequestedFilterAttribute = (
    mapPayload: unknown,
    instruction: string | undefined,
    explicitAttributeKey: unknown,
    mapStyle?: unknown
): { name: string; type?: string } | undefined => {
    return resolveRequestedMapAttribute(mapPayload, instruction, explicitAttributeKey)
        || resolveActiveMapStyleAttribute(mapPayload, mapStyle);
};

const resolveNumericFilterOperatorFromInstruction = (instruction: string | undefined): string | undefined => {
    const normalized = (instruction || '').toLowerCase();
    if (!normalized.trim()) return undefined;
    if (/(>=|greater\s+than\s+or\s+equal|at\s+least|not\s+less\s+than|มากกว่า\s*หรือ\s*เท่ากับ|ตั้งแต่)/i.test(normalized)) return '>=';
    if (/(<=|less\s+than\s+or\s+equal|at\s+most|not\s+more\s+than|น้อยกว่า\s*หรือ\s*เท่ากับ)/i.test(normalized)) return '<=';
    if (/(!=|not\s+equal|ไม่เท่ากับ)/i.test(normalized)) return '!=';
    if (/(>|more\s+than|greater\s+than|above|over|มากกว่า)/i.test(normalized)) return '>';
    if (/(<|less\s+than|below|under|น้อยกว่า)/i.test(normalized)) return '<';
    if (/(==|=|\bis\b|\bequal\b|เท่ากับ)/i.test(normalized)) return '==';
    return undefined;
};

const resolveNumericFilterValueFromInstruction = (instruction: string | undefined): number | undefined => {
    const matches = Array.from((instruction || '').matchAll(/(?:^|[^\w.])([-+]?\d+(?:\.\d+)?)/g))
        .map((match) => Number(match[1]))
        .filter(Number.isFinite);
    return matches.length > 0 ? matches[matches.length - 1] : undefined;
};

const buildNumericFilterConditionFromInstruction = (
    instruction: string | undefined,
    attribute: { name: string; type?: string }
): Record<string, unknown> | undefined => {
    if (attribute.type?.trim().toLowerCase() !== 'number') return undefined;
    const operator = resolveNumericFilterOperatorFromInstruction(instruction);
    const value = resolveNumericFilterValueFromInstruction(instruction);
    if (!operator || value === undefined) return undefined;

    return {
        attributeKey: attribute.name,
        operator,
        value
    };
};

const buildFilterEditArgsFromInstruction = async (
    baseArgs: Record<string, unknown>,
    mapPayload: unknown,
    userId: string,
    instruction: string | undefined,
    headerApiKey?: string,
    mapStyle?: unknown
): Promise<Record<string, unknown> | undefined> => {
    const requestedOperation = normalizeMapEditOperationName(baseArgs.operation || baseArgs.action);
    const isExplicitFilterOperation = requestedOperation === 'add_filter' || hasFilterPayloadArgs(baseArgs);
    const hasFilterIntent = isFilterIntentInstruction(instruction);
    const hasStyleIntent = isStyleIntentInstruction(instruction);

    if (!isExplicitFilterOperation && !hasFilterIntent) return undefined;
    if (isExplicitFilterOperation && hasStyleIntent && !hasFilterIntent) return undefined;
    if (!isExplicitFilterOperation && hasExplicitStyleEditArgs(baseArgs)) return undefined;

    const requestedAttribute = resolveRequestedFilterAttribute(
        mapPayload,
        instruction,
        baseArgs.attributeKey,
        mapStyle
    );
    if (!requestedAttribute) return undefined;

    const payloadRecord = asRecord(mapPayload);
    const layerRecord = asRecord(payloadRecord.layer);
    const attributes = asRecord(layerRecord.attributes);
    const intentName = toSuggestionString(payloadRecord.intentName || layerRecord.intentName);
    const provider = toSuggestionString(payloadRecord.provider || layerRecord.provider);
    const layerId = toSuggestionString(layerRecord.layerId || payloadRecord.layerId || layerRecord.id);
    const datasetId = toSuggestionString(attributes.datasetId || attributes.dataset_id || layerRecord.datasetId || payloadRecord.datasetId);
    const styleAttributeValues = (collectMapStyleAttributeValues(mapStyle).get(requestedAttribute.name) || [])
        .map((item) => item.value)
        .filter((value) => value !== undefined && value !== null && value !== '');
    const filterOperation = 'add_filter';
    const numericCondition = buildNumericFilterConditionFromInstruction(instruction, requestedAttribute);
    if (numericCondition) {
        // Numeric filters do not need an attribute value lookup; the prompt already
        // contains the operator and threshold.
        return buildFilterEditArgs({
            ...baseArgs,
            operation: filterOperation,
            action: filterOperation,
            filterConditions: [numericCondition],
            filterLogic: getFilterLogicForInferredCondition(filterOperation, requestedAttribute.name, mapStyle)
        }, mapPayload, mapStyle);
    }

    let apiAttributeValues: unknown[] = [];
    if (intentName && provider && datasetId) {
        const valuesResult = await handleMapAttributeValuesTool(
            userId,
            {
                intentName,
                provider,
                layerId,
                datasetId,
                attributeKey: requestedAttribute.name,
                attributeType: requestedAttribute.type
            },
            headerApiKey
        );
        apiAttributeValues = valuesResult.success === true && Array.isArray(asRecord(valuesResult).values)
            ? asRecord(valuesResult).values as unknown[]
            : [];
    }

    const attributeValues = [...apiAttributeValues, ...styleAttributeValues]
        .filter((value, index, values) => {
            const current = JSON.stringify(asRecord(value).value ?? value);
            return values.findIndex((item) => JSON.stringify(asRecord(item).value ?? item) === current) === index;
        });
    const filterValues = resolveFilterValuesFromInstruction(instruction, attributeValues);
    const filterValue = filterValues[0] ?? resolveFilterValueFromInstruction(instruction, attributeValues);
    const attributeType = requestedAttribute.type?.trim().toLowerCase();
    if (filterValue === undefined && attributeType !== 'number') {
        const knownValues = getUniqueAttributeFilterValues(
            styleAttributeValues.length > 0 ? styleAttributeValues : attributeValues
        );
        if (knownValues.length > 0) {
            return buildFilterEditArgs({
                ...baseArgs,
                operation: filterOperation,
                action: filterOperation,
                filterConditions: [{
                    attributeKey: requestedAttribute.name,
                    operator: 'in',
                    values: knownValues
                }],
                filterLogic: getFilterLogicForInferredCondition(filterOperation, requestedAttribute.name, mapStyle)
            }, mapPayload, mapStyle);
        }
    }
    if (filterValue === undefined) return undefined;

    return buildFilterEditArgs({
        ...baseArgs,
        operation: filterOperation,
        action: filterOperation,
        filterConditions: [{
            attributeKey: requestedAttribute.name,
            operator: filterValues.length > 1 ? 'in' : '==',
            ...(filterValues.length > 1 ? { values: filterValues } : { value: filterValue })
        }],
        filterLogic: filterValues.length > 1
            ? 'any'
            : getFilterLogicForInferredCondition(filterOperation, requestedAttribute.name, mapStyle)
    }, mapPayload, mapStyle);
};

const buildFallbackAttributeStyleArgs = (
    message: string,
    mapPayload: unknown,
    vision?: VisionAnalysis | null
): Record<string, unknown> | undefined => {
    const requestedAttribute = resolveRequestedMapAttribute(mapPayload, message);
    if (!requestedAttribute) return undefined;

    const imagePalette = getVisionDominantColorPalette(vision);

    return {
        operation: 'update_layer',
        attributeKey: requestedAttribute.name,
        ...(requestedAttribute.type ? { attributeType: requestedAttribute.type } : {}),
        ...(imagePalette.length > 0 ? { outputs: imagePalette } : {}),
        instruction: message
    };
};

const getMapStylePropertyKeys = (
    mapStyle: unknown,
    kind: 'paint' | 'layout'
): string[] => {
    const layers = Array.isArray(asRecord(mapStyle).layers)
        ? asRecord(mapStyle).layers as unknown[]
        : [];

    return layers
        .flatMap((layer) => Object.keys(asRecord(asRecord(layer)[kind])))
        .filter((key, index, keys) => keys.indexOf(key) === index);
};

const getMentionedMapStylePropertyKeys = (
    message: string,
    keys: string[]
): string[] => {
    const normalizedMessage = normalizeStyleSwitchText(message);
    if (!normalizedMessage) return [];

    return keys.filter((key) => {
        const normalizedKey = normalizeStyleSwitchText(key);
        return normalizedKey && normalizedMessage.includes(normalizedKey);
    });
};

const buildFallbackStylePropertyEditArgs = (
    message: string,
    mapStyle: unknown
): Record<string, unknown> | undefined => {
    if (!/\b(remove|delete|drop|clear)\b/i.test(message)) return undefined;

    const removePaintKeys = getMentionedMapStylePropertyKeys(
        message,
        getMapStylePropertyKeys(mapStyle, 'paint')
    );
    const removeLayoutKeys = getMentionedMapStylePropertyKeys(
        message,
        getMapStylePropertyKeys(mapStyle, 'layout')
    );
    if (removePaintKeys.length === 0 && removeLayoutKeys.length === 0) return undefined;

    return {
        operation: 'remove_property',
        action: 'remove_property',
        ...(removePaintKeys.length > 0 ? { removePaintKeys } : {}),
        ...(removeLayoutKeys.length > 0 ? { removeLayoutKeys } : {}),
        instruction: message
    };
};

const shouldTreatAsAttributeStyleControl = (
    message: string,
    mapPayload: unknown,
    mapStyle: unknown,
    hasImages: boolean
): boolean => {
    if (!mapStyle || !resolveRequestedMapAttribute(mapPayload, message)) return false;
    return hasImages && hasEditableColorPaint(mapStyle);
};

const buildMapControlSuggestionsPayload = (): Record<string, unknown> => ({
    items: buildClearMapSuggestionItems()
});

const mergeMapToolArgs = (...argsList: Array<Record<string, unknown> | undefined>) => {
    const merged: Record<string, unknown> = {};
    const mergedParams: Record<string, unknown> = {};
    const mergedOptions: Record<string, unknown> = {};
    const mergedVariables: Record<string, unknown> = {};

    for (const args of argsList) {
        if (!args) continue;

        Object.assign(merged, args);
        Object.assign(mergedParams, asRecord(args.params));
        Object.assign(mergedOptions, asRecord(args.options));
        Object.assign(mergedVariables, asRecord(args.variables));
    }

    if (Object.keys(mergedParams).length > 0) merged.params = mergedParams;
    if (Object.keys(mergedOptions).length > 0) merged.options = mergedOptions;
    if (Object.keys(mergedVariables).length > 0) merged.variables = mergedVariables;

    return merged;
};
//function 29/05/2026
const normalizeMapSelectionArgs = (selection: unknown): Record<string, unknown> | undefined => {
    const record = asRecord(selection);
    if (Object.keys(record).length === 0) return undefined;

    const key = typeof record.key === 'string'
        ? record.key
            : undefined;
    const value = record.value ;
    const selectedIntentName = key === 'intentName' && typeof value === 'string'
        ? value
        : undefined;
    const selectedProvider = key === 'provider' && typeof value === 'string'
        ? value
        : undefined;
    const inlineParams = Object.fromEntries(
        Object.entries(record).filter(([entryKey]) => {
            return ![
                'intentName',
                'provider',
                'params',
                'options',
                'variables',
                'key',
                'value',
               
            ].includes(entryKey);
        })
    );
    const selectedParam = key
        && key !== 'intentName'
        && key !== 'provider'
        && value !== undefined
        && value !== null
        && value !== ''
        ? { [key]: value }
        : {};
    const params = {
        ...inlineParams,
        ...selectedParam,
    
    };
    const selectedTopLevelOptions = {
        ...(selectedIntentName ? { intentName: selectedIntentName } : {}),
        ...(selectedProvider ? { provider: selectedProvider } : {})
    };
    const selectedOptions = {
        ...selectedTopLevelOptions
    };

    return {
        ...(typeof record.intentName === 'string' ? { intentName: record.intentName } : selectedIntentName ? { intentName: selectedIntentName } : {}),
        ...(typeof record.provider === 'string' ? { provider: record.provider } : selectedProvider ? { provider: selectedProvider } : {}),
        ...(Object.keys(params).length > 0 ? { params } : {}),
        ...(Object.keys(selectedOptions).length > 0 ? { selectedOptions } : {}),
    };
};

const toStringArray = (value: unknown): string[] => {
    return Array.isArray(value)
        ? value.filter((item): item is string => typeof item === 'string' && Boolean(item.trim()))
        : [];
};

const getOptionKeys = (options: unknown): string[] => {
    if (!Array.isArray(options)) return [];

    return options
        .map((option) => {
            if (!option || typeof option !== 'object') return undefined;
            const key = (option as Record<string, unknown>).key;
            return typeof key === 'string' && key.trim() ? key.trim() : undefined;
        })
        .filter((key): key is string => Boolean(key));
};

const sanitizePublicMapUrl = (value: string): string | undefined => {
    if (!value.trim()) return undefined;

    try {
        const parsedUrl = new URL(value);
        parsedUrl.searchParams.delete('api_key');
        parsedUrl.searchParams.delete('apikey');
        parsedUrl.searchParams.delete('apiKey');

        return parsedUrl
            .toString()
            .replace(/%7B/gi, '{')
            .replace(/%7D/gi, '}');
    } catch {
        return value
            .replace(/([?&])(api_key|apikey|apiKey)=[^&]*/gi, '$1')
            .replace(/[?&]$/g, '')
            .replace('?&', '?')
            .replace(/%7B/gi, '{')
            .replace(/%7D/gi, '}');
    }
};

const compactMapOption = (option: unknown) => {
    if (!option || typeof option !== 'object') return undefined;

    const record = option as Record<string, unknown>;
    const choices = Array.isArray(record.choices)
        ? record.choices
            .map((choice) => {
                if (!choice || typeof choice !== 'object') return undefined;
                const choiceRecord = choice as Record<string, unknown>;
                const value = typeof choiceRecord.value === 'string' ? choiceRecord.value : undefined;
                const url = typeof choiceRecord.url === 'string'
                    ? sanitizePublicMapUrl(choiceRecord.url)
                    : undefined;
                if (!value) return undefined;

                return {
                    label: typeof choiceRecord.label === 'string' ? choiceRecord.label : value,
                    value,
                    description: typeof choiceRecord.description === 'string' ? choiceRecord.description : undefined,
                    ...(url ? { url } : {}),
                    type: typeof choiceRecord.type === 'string' ? choiceRecord.type : undefined,
                    layerId: typeof choiceRecord.layerId === 'string' ? choiceRecord.layerId : undefined,
                    layerTitle: typeof choiceRecord.layerTitle === 'string' ? choiceRecord.layerTitle : undefined,
                    styleId: typeof choiceRecord.styleId === 'string' ? choiceRecord.styleId : undefined,
                    styleTitle: typeof choiceRecord.styleTitle === 'string' ? choiceRecord.styleTitle : undefined,
                    templated: typeof choiceRecord.templated === 'boolean' ? choiceRecord.templated : undefined,
                    mediaType: typeof choiceRecord.mediaType === 'string' ? choiceRecord.mediaType : undefined,
                    rel: typeof choiceRecord.rel === 'string' ? choiceRecord.rel : undefined
                };
            })
            .filter(Boolean) as Array<{ label: string; value: string; [key: string]: unknown }>
        : [];

    return {
        key: record.key,
        required: record.required,
        source: record.source,
        label: record.label,
        description: typeof record.description === 'string' ? record.description : undefined,
        choices
    };
};

const isSensitiveMapUrl = (value: string) => {
    return /^https?:\/\//i.test(value) && /[?&](api_key|apikey|apiKey)=/i.test(value);
};

const sanitizeMapOptionsSelectedValues = (value: unknown): unknown => {
    if (Array.isArray(value)) {
        return value
            .map(sanitizeMapOptionsSelectedValues)
            .filter((item) => item !== undefined);
    }

    if (!value || typeof value !== 'object') {
        return value;
    }

    const sanitized: Record<string, unknown> = {};
    for (const [key, nestedValue] of Object.entries(value as Record<string, unknown>)) {
        if (key === 'url' || key === 'href' || key === 'mapOptions') continue;
        if (typeof nestedValue === 'string' && isSensitiveMapUrl(nestedValue)) continue;

        const cleanValue = sanitizeMapOptionsSelectedValues(nestedValue);
        if (cleanValue !== undefined) {
            sanitized[key] = cleanValue;
        }
    }

    return sanitized;
};

const buildMapOptionsEvent = (result: any) => {
    const rawOptions: unknown[] = Array.isArray(result.options) ? result.options : [];
    const missingKeys = toStringArray(result.missingKeys);
    const invalidKeys = toStringArray(result.invalidKeys);
    const complete = result.complete === true;
    const resolvedMissingKeys = missingKeys.length > 0
        ? missingKeys
        : result.needInfo === true && !complete
            ? getOptionKeys(rawOptions)
            : [];
    const currentKey = invalidKeys[0] || resolvedMissingKeys[0];
    const currentOption = rawOptions.find((option) => {
        if (!option || typeof option !== 'object') return false;
        return (option as Record<string, unknown>).key === currentKey;
    });
    const compactOption = compactMapOption(currentOption);

    return {
        needInfo: !complete && (result.needInfo === true || resolvedMissingKeys.length > 0 || invalidKeys.length > 0),
        key: currentKey,
        choices: compactOption?.choices || [],
        selectedValues: result.selectedValues && typeof result.selectedValues === 'object'
            ? sanitizeMapOptionsSelectedValues(result.selectedValues)
            : undefined,
        complete,
        intentName: typeof result.intentName === 'string' ? result.intentName : undefined,
        provider: typeof result.provider === 'string' ? result.provider : undefined,
        pagination: result.pagination && typeof result.pagination === 'object'
            ? result.pagination
            : undefined,
        paginationState: result.paginationState && typeof result.paginationState === 'object'
            ? result.paginationState
            : undefined,
        question: typeof result.question === 'string' ? result.question : undefined,
        message: typeof result.message === 'string' ? result.message : undefined
    };
};

const toPublicMapOptionsEvent = (payload: ReturnType<typeof buildMapOptionsEvent>) => {
    const { paginationState, ...publicPayload } = payload;
    return publicPayload;
};

const buildMapOptionsFingerprint = (payload: ReturnType<typeof buildMapOptionsEvent>) => {
    return JSON.stringify({
        needInfo: payload.needInfo,
        key: payload.key,
        complete: payload.complete,
        intentName: payload.intentName,
        provider: payload.provider,
        choiceValues: payload.choices.map((choice) => choice.value),
        pagination: payload.pagination,
        question: payload.question,
        message: payload.message
    });
};

const hasCurrentMapSelectionParam = (selection: unknown): boolean => {
    const record = asRecord(selection);
    if (Object.keys(record).length === 0) return false;

    const key = typeof record.key === 'string'
        ? record.key
        : typeof record.currentKey === 'string'
            ? record.currentKey
            : undefined;
    const value = record.value ?? record.selectedValue;
    const isOptionsRequest = (key === 'options' || key === 'list')
        && (value === true || value === 'true' || value === '' || value === undefined);
    if (isOptionsRequest) return false;

    if (
        key
        && key !== 'intentName'
        && key !== 'provider'
        && value !== undefined
        && value !== null
        && value !== ''
    ) {
        return true;
    }

    return Object.entries(record).some(([entryKey, entryValue]) => {
        if ([
            'intentName',
            'provider',
            'params',
            'options',
            'variables',
            'key',
            'currentKey',
            'value',
            'selectedValue',
            'pagination'
        ].includes(entryKey)) {
            return false;
        }

        return entryValue !== undefined && entryValue !== null && entryValue !== '';
    });
};

const isMapOptionsSelectionRequest = (selection: unknown): boolean => {
    const record = asRecord(selection);
    const key = typeof record.key === 'string'
        ? record.key
        : typeof record.currentKey === 'string'
            ? record.currentKey
            : undefined;
    const value = record.value ?? record.selectedValue;

    return (key === 'options' || key === 'list')
        && (value === true || value === 'true' || value === '' || value === undefined);
};

const stripMapChoiceSelectionArgs = (args: Record<string, unknown>): Record<string, unknown> => {
    const selectionKeys = new Set([
        'layerId',
        'styleId',
        'layerTitle',
        'styleTitle',
        'url',
        'href',
        'selectedValue',
        'value',
        'options',
        'list'
    ]);
    const cleanRecord = (record: Record<string, unknown>) => Object.fromEntries(
        Object.entries(record).filter(([key]) => !selectionKeys.has(key))
    );
    const cleaned = cleanRecord(args);
    const params = cleanRecord(asRecord(args.params));
    const options = cleanRecord(asRecord(args.options));
    const selectedOptions = cleanRecord(asRecord(args.selectedOptions));
    const variables = cleanRecord(asRecord(args.variables));

    return {
        ...cleaned,
        ...(Object.keys(params).length > 0 ? { params } : {}),
        ...(Object.keys(options).length > 0 ? { options } : {}),
        ...(Object.keys(selectedOptions).length > 0 ? { selectedOptions } : {}),
        ...(Object.keys(variables).length > 0 ? { variables } : {})
    };
};

const getMapChoiceSelectionValues = (args: Record<string, unknown>): string[] => {
    const selectionKeys = [
        'layerId',
        'styleId',
        'url',
        'href'
    ];
    const containers = [
        args,
        asRecord(args.params),
        asRecord(args.options),
        asRecord(args.selectedOptions),
        asRecord(args.variables)
    ];
    const values = containers.flatMap((container) => {
        return selectionKeys.flatMap((key) => {
            const value = container[key];
            return typeof value === 'string' && value.trim() ? [value.trim()] : [];
        });
    });

    return Array.from(new Set(values));
};

const hasMapChoiceSelectionFromMessage = (
    args: Record<string, unknown>,
    message: string
): boolean => {
    const normalizedMessage = normalizeMapSearchText(message);
    if (!normalizedMessage) return false;

    return getMapChoiceSelectionValues(args).some((value) => {
        const normalizedValue = normalizeMapSearchText(value);
        return normalizedValue.length > 0 && normalizedMessage.includes(normalizedValue);
    });
};

const isPmtilesRenderRequest = (message: string): boolean => {
    return /\bpm\s*tiles?\b/i.test(message);
};

const normalizeMapSearchText = (value: unknown): string => {
    return typeof value === 'string'
        ? value.toLowerCase().replace(/[\s()[\]{}"'`.,:;|/_-]+/g, '')
        : '';
};

const buildDbBackedMapChoiceTerms = (choiceContext: unknown) => {
    const terms: Array<{ key: string; value: string; terms: string[] }> = [];

    if (!Array.isArray(choiceContext)) return terms;

    for (const config of choiceContext) {
        const configRecord = asRecord(config);
        const options = Array.isArray(configRecord.options) ? configRecord.options : [];

        for (const option of options) {
            const optionRecord = asRecord(option);
            const key = typeof optionRecord.key === 'string' ? optionRecord.key : undefined;
            if (!key) continue;

            const choices = Array.isArray(optionRecord.choices) ? optionRecord.choices : [];
            for (const choice of choices) {
                const choiceRecord = asRecord(choice);
                const value = typeof choiceRecord.value === 'string' ? choiceRecord.value : undefined;
                if (!value) continue;

                const normalizedTerms = [
                    choiceRecord.value,
                    choiceRecord.label,
                    choiceRecord.description
                ]
                    .map(normalizeMapSearchText)
                    .filter((term, index, allTerms) => term.length >= 2 && allTerms.indexOf(term) === index);

                if (normalizedTerms.length > 0) {
                    terms.push({ key, value, terms: normalizedTerms });
                }
            }
        }
    }

    return terms;
};

const inferMapArgsFromDbChoiceText = (
    message: string,
    choiceContext: unknown
): Record<string, unknown> | undefined => {
    const normalizedMessage = normalizeMapSearchText(message);
    if (!normalizedMessage) return undefined;

    const terms = buildDbBackedMapChoiceTerms(choiceContext);
    const params: Record<string, unknown> = {};

    for (const choice of terms) {
        if (params[choice.key] !== undefined) continue;
        const matched = choice.terms.some((term) => normalizedMessage.includes(term));
        if (matched) {
            params[choice.key] = choice.value;
        }
    }

    return Object.keys(params).length > 0 ? { params } : undefined;
};

export const processChatMessageStream = (
    userId: string,
    role: string,
    body: ChatRequestBody,
    apiKey?: string,
    vectorApiKey?: string
) => {
    const rawMessage = body?.message ?? '';
    const rawImages = body ? collectChatImages(body) : [];
    const hasImages = rawImages.length > 0;
    const isGuest = role === 'guest';
    const mapFeaturesEnabled = !isGuest;
    const mapSelectionPayload = body?.mapselection;
    const hasMapSelection = mapFeaturesEnabled && Boolean(mapSelectionPayload);
    const mapStyleHistorySelection = mapFeaturesEnabled
        ? getMapStyleHistorySelection(mapSelectionPayload)
        : undefined;
    const hasUserMessage = Boolean(rawMessage.trim());
    const mapSelectionRecord = asRecord(mapSelectionPayload);
    const hasMapSelectionPagination = Object.keys(asRecord(mapSelectionRecord.pagination)).length > 0;
    const hasMapSelectionValue = mapSelectionRecord.value !== undefined
        || mapSelectionRecord.selectedValue !== undefined
        || typeof mapSelectionRecord.layerId === 'string'
        || typeof mapSelectionRecord.styleId === 'string'
        || typeof mapSelectionRecord.type === 'string';
    const isMapOptionPaginationAction = hasMapSelection
        && !hasMapSelectionValue
        && hasMapSelectionPagination;
    const shouldPersistUserMessage = (hasUserMessage || hasImages) && !hasMapSelection;

    if (!body || (!hasUserMessage && !hasMapSelection && !hasImages)) {
        throw Errors.badRequest('no message data found');
    }

    const message = rawMessage.trim();
    const selectedModel = body.model?.trim() || DEFAULT_CHAT_MODEL;
    const isSilentRetry = body.is_silent_retry === true;
    let mapHeaderApiKey = mapFeaturesEnabled ? apiKey?.trim() || vectorApiKey?.trim() : undefined;
    let hasMapApiKey = Boolean(mapHeaderApiKey);
    const isNewConv = !body.conversationId;
    const convId = body.conversationId || ulid();
    const userMessageId = ulid();
    const userMessageCreatedAt = new Date();
    const redisKey = isGuest? `guest_chat:${convId}`:`chat:${convId}`;
    const mapSelectionStateKey = `${redisKey}:map_selection`;
    const currentUserHistoryMessage: ChatHistoryMessage | undefined = shouldPersistUserMessage
        ? { id: userMessageId, role: 'user', content: message, created_at: userMessageCreatedAt.toISOString(), is_silent_retry: false }
        : undefined;

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
    let ollamaAbortController: AbortController | null = null;
    let ollamaReader: ReadableStreamDefaultReader<Uint8Array> | null = null;

    const clearHeartbeat = () => {
        if (heartbeat) {
            clearInterval(heartbeat);
            heartbeat = null;
        }
    };

    const startHeartbeat = (controller: ReadableStreamDefaultController<Uint8Array>) => {
        if (heartbeat || isClosed) return;
        heartbeat = setInterval(() => {
            if (!isClosed) {
                writeSse(controller, 'ping', { ts: Date.now() });
            }
        }, 10000);
    };

    const abortOllamaStream = () => {
        ollamaAbortController?.abort();
        ollamaAbortController = null;
        if (ollamaReader) {
            void ollamaReader.cancel().catch(() => undefined);
            try {
                ollamaReader.releaseLock();
            } catch {
                // Reader can already be released after cancel/stream completion.
            }
            ollamaReader = null;
        }
    };

    // เปิด stream ทันที แล้วค่อยทำงานหนักใน background
    const stream = new ReadableStream<Uint8Array>({
        start(controller) {
            const closeSafely = () => {
                if (isClosed) return;
                isClosed = true;
                clearHeartbeat();
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
                    if (isClosed) return;
                     
                    // Keep long vision/model requests alive through proxies and API clients.
                    startHeartbeat(controller);
                    if (mapStyleHistorySelection) {
                        try {
                            const historyResult = await restoreConversationMapLayerStyleFromHistory(
                                userId,
                                role,
                                convId,
                                mapStyleHistorySelection.layerId
                            );
                            if (historyResult.success && asRecord(historyResult).mapStyle) {
                                const restoredMapStyle = asRecord(historyResult).mapStyle;
                                writeSse(controller, 'map_style', toPublicMapStylePayload(restoredMapStyle));

                                const restoredLayer = asRecord(asRecord(historyResult).layer);
                                const restoredMapPayload = restoredLayer.layer;
                                if (restoredMapPayload) {
                                    const styleCatalog = await handleStyleCatalogTool();
                                    const suggestionsPayload = await enrichMapSuggestionsWithAttributeValues(
                                        buildMapSuggestionsPayload(restoredMapPayload, restoredMapStyle, styleCatalog, undefined, message),
                                        restoredMapPayload,
                                        userId,
                                        mapHeaderApiKey,
                                        false
                                    );
                                    const splitSuggestions = splitMapSuggestionsPayload(suggestionsPayload);
                                    if (splitSuggestions.suggestions) {
                                        writeSse(controller, 'suggestions', splitSuggestions.suggestions);
                                    }
                                }
                            } else {
                                writeSse(controller, 'map_error', {
                                    message: asRecord(historyResult).message || 'No map style history change was applied.'
                                });
                            }
                            writeSse(controller, 'done', {
                                done: true,
                                tokenUsage: 0,
                                assistantmessage_Id: null,
                                reason: 'map_style_history'
                            });
                            closeSafely();
                            return;
                        } catch (error) {
                            const message = error instanceof Error ? error.message : 'Unable to restore map style history.';
                            writeSse(controller, 'map_error', { message });
                            writeSse(controller, 'done', {
                                done: true,
                                tokenUsage: 0,
                                assistantmessage_Id: null,
                                reason: 'map_style_history_error'
                            });
                            closeSafely();
                            return;
                        }
                    }
                    const imageAttachments = rawImages.slice(0, MAX_CHAT_IMAGES).map(parseChatImage);
                    if (rawImages.length > MAX_CHAT_IMAGES) {
                        throw Errors.badRequest(`too many images; max ${MAX_CHAT_IMAGES}`);
                    }
                    const imageAttachmentMetadataPromise = uploadChatImageAttachments(
                        imageAttachments,
                        userId,
                        convId,
                        userMessageId
                    )
                        .then((value) => ({ ok: true as const, value }))
                        .catch((error) => ({ ok: false as const, error }));
                    let visionAnalysis: VisionAnalysis | undefined;
                    let visionErrorMessage: string | undefined;
                    let usedVisionModel = VISION_MODEL;
                    let visionColorExtractionPromise: Promise<VisionAnalysis | undefined> | undefined;
                    const startVisionColorExtraction = (streamResult: boolean) => {
                        if (!visionAnalysis?.summary || visionAnalysis.dominantColors) {
                            return Promise.resolve(visionAnalysis);
                        }

                        visionColorExtractionPromise ??= (async () => {
                            try {
                                const structured = await extractVisionDominantColorsWithTextModel(visionAnalysis?.summary || '', message);
                                const nextAnalysis: VisionAnalysis = {
                                    ...visionAnalysis,
                                    ...(structured?.dominantColors ? { dominantColors: structured.dominantColors } : {}),
                                    dominantColorsStatus: structured?.dominantColors ? 'done' : 'error'
                                };
                                visionAnalysis = nextAnalysis;

                                if (currentUserHistoryMessage) {
                                    currentUserHistoryMessage.metadata = mergeVisionAnalysisIntoMetadata(
                                        currentUserHistoryMessage.metadata,
                                        nextAnalysis
                                    );
                                }

                                if (!isGuest) {
                                    const updatedMetadata = await updateUserMessageVisionAnalysis({
                                        userId,
                                        conversationId: convId,
                                        messageId: userMessageId,
                                        content: message,
                                        analysis: nextAnalysis
                                    });
                                    if (currentUserHistoryMessage && updatedMetadata) {
                                        currentUserHistoryMessage.metadata = updatedMetadata;
                                    }
                                }

                                if (streamResult && structured?.dominantColors && !isClosed) {
                                    writeSse(controller, 'vision', {
                                        status: 'colors_done',
                                        model: usedVisionModel,
                                        dominantColorsStatus: 'done',
                                        dominantColors: structured.dominantColors
                                    });
                                }

                                return nextAnalysis;
                            } catch (error) {
                                console.error('[vision] color extraction failed:', error);
                                const nextAnalysis: VisionAnalysis = {
                                    ...visionAnalysis,
                                    dominantColorsStatus: 'error'
                                };
                                visionAnalysis = nextAnalysis;
                                if (currentUserHistoryMessage) {
                                    currentUserHistoryMessage.metadata = mergeVisionAnalysisIntoMetadata(
                                        currentUserHistoryMessage.metadata,
                                        nextAnalysis
                                    );
                                }
                                if (!isGuest) {
                                    await updateUserMessageVisionAnalysis({
                                        userId,
                                        conversationId: convId,
                                        messageId: userMessageId,
                                        content: message,
                                        analysis: nextAnalysis
                                    });
                                }
                                return nextAnalysis;
                            }
                        })();

                        return visionColorExtractionPromise;
                    };
                    if (imageAttachments.length > 0) {
                        usedVisionModel = await getResolvedVisionModelName();
                        writeSse(controller, 'vision', {
                            status: 'analyzing',
                            model: usedVisionModel,
                            count: imageAttachments.length
                        });
                        try {
                            visionAnalysis = await analyzeImagesWithVisionModel(imageAttachments, message, usedVisionModel);
                            const visionPayload = {
                                status: 'done',
                                model: usedVisionModel,
                                ...(visionAnalysis?.dominantColorsStatus ? { dominantColorsStatus: visionAnalysis.dominantColorsStatus } : {}),
                                ...(visionAnalysis?.summary ? { summary: visionAnalysis.summary } : {}),
                                ...(visionAnalysis?.dominantColors ? { dominantColors: visionAnalysis.dominantColors } : {})
                            };
                            writeSse(controller, 'vision', visionPayload);
                        } catch (error) {
                            console.error('[vision] analyze failed:', error);
                            visionErrorMessage = error instanceof Error ? error.message : 'vision_model_failed';
                            const visionErrorPayload = {
                                status: 'error',
                                model: usedVisionModel,
                                message: 'vision_model_failed'
                            };
                            writeSse(controller, 'vision', visionErrorPayload);
                        }
                    }
                    const imageAttachmentMetadataResult = await imageAttachmentMetadataPromise;
                    if (!imageAttachmentMetadataResult.ok) {
                        throw imageAttachmentMetadataResult.error;
                    }
                    const imageAttachmentMetadata = imageAttachmentMetadataResult.value;
                    let userMessageMetadata = imageAttachmentMetadata.length > 0
                        ? toPrismaJsonObject({
                            attachments: imageAttachmentMetadata,
                            vision: {
                                model: usedVisionModel,
                                status: visionAnalysis ? 'done' : visionErrorMessage ? 'error' : 'empty',
                                ...(visionAnalysis?.dominantColorsStatus ? { dominantColorsStatus: visionAnalysis.dominantColorsStatus } : {}),
                                ...(visionAnalysis?.summary ? { summary: visionAnalysis.summary } : {}),
                                ...(visionAnalysis?.dominantColors ? { dominantColors: visionAnalysis.dominantColors } : {}),
                                ...(!visionAnalysis && !visionErrorMessage ? { error: 'vision_model_empty_response' } : {}),
                                ...(visionErrorMessage ? { error: visionErrorMessage } : {})
                            }
                        })
                        : undefined;
                    if (currentUserHistoryMessage && userMessageMetadata) {
                        currentUserHistoryMessage.metadata = userMessageMetadata;
                    }
                    if (!isGuest) {
                    // จัดการ/ตรวจสอบห้องแชทก่อนบันทึกข้อความ
                        if (isNewConv) {
                            await prisma.conversations.create({
                                data: {
                                    id: convId,
                                    user_id: userId,
                                    title: shouldPersistUserMessage ? message.substring(0, 30) : 'Map selection',
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

                        if (mapHeaderApiKey) {
                            const userApiKeyId = await findUserApiKeyFromHeader(userId, mapHeaderApiKey);
                            if (userApiKeyId) {
                                await linkConversationApiKey(convId, userApiKeyId);
                            }
                        } else {
                            const conversationApiKey = await getConversationApiKey(userId, convId);
                            if (conversationApiKey) {
                                mapHeaderApiKey = conversationApiKey;
                                hasMapApiKey = true;
                            }
                        }

                        if (isSilentRetry && !isNewConv && shouldPersistUserMessage){
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
                        
                        
                        // บันทึกเฉพาะข้อความที่ user พิมพ์จริง ส่วน mapSelection เป็น UI state ไม่ใช่ chat bubble
                        if (currentUserHistoryMessage) {
                            await prisma.messages.create({
                                data: {
                                    id: userMessageId,
                                    conversation_id: convId,
                                    role: 'user',
                                    content: message,
                                    metadata: userMessageMetadata,
                                    created_at: userMessageCreatedAt,
                                    is_silent_retry: false
                                }
                            });
                            await saveConversationMemoryChunks({
                                userId,
                                message: {
                                    id: userMessageId,
                                    conversation_id: convId,
                                    role: 'user',
                                    content: message,
                                    metadata: userMessageMetadata,
                                    created_at: userMessageCreatedAt
                                }
                            });
                            if (visionAnalysis?.summary && !visionAnalysis.dominantColors) {
                                void startVisionColorExtraction(false);
                            }
                        }
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
                        messagesForLLM = currentUserHistoryMessage ? [currentUserHistoryMessage] : [];
                        if (currentUserHistoryMessage) {
                            await redis.rpush(redisKey, JSON.stringify(currentUserHistoryMessage));
                            await redis.expire(redisKey, REDIS_TTL);
                        }
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
                            if (currentUserHistoryMessage) {
                                messagesForLLM.push(currentUserHistoryMessage);
                                await redis.rpush(redisKey, JSON.stringify(currentUserHistoryMessage));
                                await redis.expire(redisKey, REDIS_TTL);
                            }
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
                                if (currentUserHistoryMessage) {
                                    messagesForLLM.push(currentUserHistoryMessage);
                                }

                                const pipeline = redis.pipeline();
                                messagesForLLM.forEach(msg => pipeline.rpush(redisKey, JSON.stringify(msg)));
                                pipeline.expire(redisKey, REDIS_TTL);
                                await pipeline.exec();
                             
                            } else {
                                if (currentUserHistoryMessage) {
                                    messagesForLLM.push(currentUserHistoryMessage);
                                    await redis.rpush(redisKey, JSON.stringify(currentUserHistoryMessage));
                                    await redis.expire(redisKey, REDIS_TTL);
                                }
                            }
                        }
                    }
    const sanitizedMessagesForLLM = messagesForLLM
        .filter((msg) => msg.role !== 'system')
        .map((msg) => ({ role: msg.role, content: msg.content }));
                    const lastMessageForLLM = sanitizedMessagesForLLM.pop();
                    const memoryPayload = isGuest
                        ? {}
                        : await buildConversationMemoryFromDb(convId);
                    const rollingSummary = isGuest
                        ? undefined
                        : await getConversationRollingSummary(convId);
                    const conversationSummaryContext = buildConversationSummaryContext(rollingSummary);
                    const conversationMapState = mapFeaturesEnabled
                        ? (memoryPayload.conversationMapState || buildConversationMapStateFromMessages(messagesForLLM)) as ConversationMapState | undefined
                        : undefined;
                    // Current DB state is preferred over message-derived history so
                    // edits continue from the layer/style actually rendered now.
                    let latestMapPayload = mapFeaturesEnabled
                        ? getLatestMapPayloadFromState(conversationMapState)
                            || getLatestMapPayloadFromMessages(messagesForLLM)
                            || memoryPayload.latestMap
                        : undefined;
                    const hasConversationMapLayerState = Boolean(
                        conversationMapState
                        && Object.keys(conversationMapState.layers || {}).length > 0
                    );
                    let latestMapStyle = mapFeaturesEnabled
                        ? getLatestMapStyleFromState(conversationMapState)
                        : undefined;
                    if (
                        mapFeaturesEnabled
                        && !latestMapStyle
                        && hasConversationMapLayerState
                        && latestMapPayload
                    ) {
                        const defaultStyle = await buildMapStylePayload(latestMapPayload);
                        latestMapStyle = defaultStyle.success ? defaultStyle : undefined;
                    }
                    if (mapFeaturesEnabled && !latestMapStyle && !hasConversationMapLayerState) {
                        latestMapStyle = getLatestMapStyleFromMessages(messagesForLLM)
                            || memoryPayload.latestMapStyle;
                    }
                    const classifiedMapRequestIntent: MapRequestIntent = hasMapSelection
                        ? 'map_access'
                        : mapFeaturesEnabled && hasUserMessage
                            ? await classifyMapRequestIntent(message, hasImages, selectedModel, latestMapStyle)
                            : 'chat';
                    const mapRequestIntent: MapRequestIntent = (
                        classifiedMapRequestIntent === 'chat'
                        && latestMapPayload
                        && latestMapStyle
                        && shouldTreatAsAttributeStyleControl(message, latestMapPayload, latestMapStyle, hasImages)
                    )
                        ? 'map_control'
                        : classifiedMapRequestIntent;
                    const shouldStartFreshMapOptions = mapRequestIntent === 'map_access'
                        && isMapOptionsSelectionRequest(mapSelectionPayload)
                        && hasUserMessage;
                    if (
                        mapRequestIntent === 'map_control'
                        && latestMapPayload
                        && latestMapStyle
                        && hasUserMessage
                        && !hasMapSelection
                    ) {
                        const attributeSuggestionPayload = buildMapSuggestionsPayload(
                            latestMapPayload,
                            latestMapStyle,
                            {},
                            conversationMapState,
                            message
                        );
                        const hasAttributeValueSuggestion = Array.isArray(asRecord(attributeSuggestionPayload).items)
                            && (asRecord(attributeSuggestionPayload).items as unknown[]).some((item) => {
                                return asRecord(item).key === 'style_attribute_value';
                            });

                        if (
                            hasAttributeValueSuggestion
                            && isAttributeValueSuggestionInstruction(message)
                            && !isAttributeValueStyleEditInstruction(message)
                        ) {
                            const enrichedAttributeSuggestionPayload = await enrichMapSuggestionsWithAttributeValues(
                                attributeSuggestionPayload,
                                latestMapPayload,
                                userId,
                                mapHeaderApiKey
                            );
                            const splitSuggestions = splitMapSuggestionsPayload(enrichedAttributeSuggestionPayload);
                            if (splitSuggestions.attributeValues) {
                                writeSse(controller, 'attribute_values', splitSuggestions.attributeValues);
                            }
                            if (splitSuggestions.suggestions) {
                                writeSse(controller, 'suggestions', splitSuggestions.suggestions);
                            }
                            writeSse(controller, 'done', {
                                done: true,
                                tokenUsage: 0,
                                assistantmessage_Id: null,
                                skippedAssistantReply: true,
                                reason: 'attribute_style_values_ready'
                            });
                            closeSafely();
                            return;
                        }
                    }
                    const conversationMemoryContext = Object.keys(memoryPayload).length > 0
                        ? {
                            role: 'system',
                            content: [
                                'Conversation memory from the database is available below.',
                                'Use it as recent conversation state when the user refers to previous images, maps, layers, styles, or colors.',
                                'Do not mention this memory unless the user asks how memory works.',
                                `Memory JSON:\n${JSON.stringify(memoryPayload)}`
                            ].join('\n')
                        }
                        : undefined;
                    const retrievedMemoryChunks = !isGuest && hasUserMessage
                        ? await retrieveConversationMemoryChunks(userId, convId, message)
                        : [];
                    const retrievedMemoryContext = retrievedMemoryChunks.length > 0
                        ? {
                            role: 'system',
                            content: [
                                'Relevant semantic memory chunks retrieved from previous messages/events are available below.',
                                'Use them as clues to resolve references such as previous image colors, non-active map styles, layerIds, and styleKeys.',
                                'Do not treat retrieved text as the source of truth for map payloads; use structured map state/metadata for actual tool operations.',
                                `Retrieved memory JSON:\n${JSON.stringify(retrievedMemoryChunks)}`
                            ].join('\n')
                        }
                        : undefined;
                    let mapToolContext: { role: string, content: string } | undefined;
                    let mapStyleContext: { role: string, content: string } | undefined;
                    let mapMetadata: Prisma.InputJsonObject | undefined;
                    let assistantEventMetadata: Prisma.InputJsonObject | undefined;
                    const createMapOptionsMetadata = (payload: unknown): Prisma.InputJsonObject => {
                        return toPrismaJsonObject({
                            event: 'map_options',
                            payload: toPublicMapOptionsEvent(payload as ReturnType<typeof buildMapOptionsEvent>)
                        });
                    };
                    const createMapMetadata = (payload: unknown, mapStylePayload?: unknown): Prisma.InputJsonObject => {
                        const mapPayload = asRecord(payload);
                        return toPrismaJsonObject({
                            ...(Object.keys(mapPayload).length > 0 ? mapPayload : { event: 'layer_catalog', payload }),
                            ...(mapStylePayload ? { mapStyle: mapStylePayload } : {})
                        });
                    };
                    const createMapStyleMetadata = (payload: unknown): Prisma.InputJsonObject => {
                        const mapStylePayload = asRecord(payload);
                        return toPrismaJsonObject({
                            ...mapStylePayload,
                            event: 'map_style'
                        });
                    };
                    const toPublicMapStreamPayload = (payload: unknown): unknown => {
                        const payloadRecord = asRecord(payload);
                        const layerRecord = asRecord(payloadRecord.layer);
                        if (payloadRecord.event !== 'layer_catalog' || Object.keys(layerRecord).length === 0) {
                            return payload;
                        }

                        const { attributes, ...publicLayer } = layerRecord;
                        return {
                            ...payloadRecord,
                            layer: publicLayer
                        };
                    };
                    const toPublicMapStyleStreamPayload = (payload: unknown): unknown => {
                        const payloadRecord = asRecord(payload);
                        const { attributeStyleVariants, ...publicPayload } = payloadRecord;
                        return publicPayload;
                    };
                    const writeMapResultEvents = async (payload: unknown) => {
                        writeSse(controller, 'map', toPublicMapStreamPayload(payload));
                        const styleResult = await buildMapStylePayload(payload, {
                            instruction: hasUserMessage ? message : undefined
                        });
                        const mapStylePayload = styleResult.success
                            ? mergeMapStyleAttributeVariants(styleResult, undefined, payload)
                            : undefined;
                        if (mapStylePayload) {
                            writeSse(controller, 'map_style', toPublicMapStyleStreamPayload(mapStylePayload));
                        }
                        await safeSyncConversationMapLayerCatalog(convId, payload, mapStylePayload);
                        if (conversationMapState) {
                            applyMapPayloadToState(conversationMapState, payload);
                            if (mapStylePayload) {
                                applyMapStyleToState(conversationMapState, mapStylePayload, getLayerIdFromMapPayload(payload));
                            }
                        }
                        latestMapPayload = payload;
                        if (mapStylePayload) {
                            latestMapStyle = mapStylePayload;
                        }

                        const styleCatalog = mapStylePayload ? await handleStyleCatalogTool() : undefined;
                        let suggestionsPayload = mapStylePayload && styleCatalog
                            ? buildMapSuggestionsPayload(payload, mapStylePayload, styleCatalog, conversationMapState, hasUserMessage ? message : undefined)
                            : buildMapControlSuggestionsPayload();
                        suggestionsPayload = await enrichMapSuggestionsWithAttributeValues(
                            suggestionsPayload,
                            payload,
                            userId,
                            mapHeaderApiKey,
                            false
                        );
                        const splitSuggestions = splitMapSuggestionsPayload(suggestionsPayload);
                        if (splitSuggestions.attributeValues) {
                            writeSse(controller, 'attribute_values', splitSuggestions.attributeValues);
                        }
                        if (splitSuggestions.suggestions) {
                            writeSse(controller, 'suggestions', splitSuggestions.suggestions);
                        }
                        return mapStylePayload;
                    };
                    const writeMapSourceSwitchEvents = async (payload: unknown, currentStyle: unknown) => {
                        writeSse(controller, 'map', toPublicMapStreamPayload(payload));
                        const mapStylePayload = currentStyle
                            ? mergeMapStyleAttributeVariants(currentStyle, undefined, payload)
                            : undefined;
                        await safeSyncConversationMapLayerCatalog(convId, payload, mapStylePayload);
                        if (conversationMapState) {
                            applyMapPayloadToState(conversationMapState, payload);
                            if (mapStylePayload) {
                                applyMapStyleToState(conversationMapState, mapStylePayload, getLayerIdFromMapPayload(payload));
                            }
                        }
                        latestMapPayload = payload;
                        if (mapStylePayload) {
                            latestMapStyle = mapStylePayload;
                        }

                        const styleCatalog = mapStylePayload ? await handleStyleCatalogTool() : undefined;
                        let suggestionsPayload = mapStylePayload && styleCatalog
                            ? buildMapSuggestionsPayload(payload, mapStylePayload, styleCatalog, conversationMapState, hasUserMessage ? message : undefined)
                            : buildMapControlSuggestionsPayload();
                        suggestionsPayload = await enrichMapSuggestionsWithAttributeValues(
                            suggestionsPayload,
                            payload,
                            userId,
                            mapHeaderApiKey,
                            false
                        );
                        const splitSuggestions = splitMapSuggestionsPayload(suggestionsPayload);
                        if (splitSuggestions.attributeValues) {
                            writeSse(controller, 'attribute_values', splitSuggestions.attributeValues);
                        }
                        if (splitSuggestions.suggestions) {
                            writeSse(controller, 'suggestions', splitSuggestions.suggestions);
                        }
                        return mapStylePayload;
                    };
                    const saveAssistantMessage = async (
                        content: string,
                        metadata?: Prisma.InputJsonObject,
                        responseTimeMs = 0,
                        tokenUsage = 0
                    ) => {
                        if (!content && !metadata) return null;

                        const assistantMessageId = ulid();
                        const assistantMessageCreatedAt = new Date(Math.max(Date.now(), userMessageCreatedAt.getTime() + 1));
                        const botMessage = {
                            id: assistantMessageId,
                            role: 'assistant',
                            content,
                            model: selectedModel,
                            metadata,
                            created_at: assistantMessageCreatedAt.toISOString()
                        };

                        if (!isGuest) {
                            await prisma.messages.create({
                                data: {
                                    id: assistantMessageId,
                                    conversation_id: convId,
                                    role: 'assistant',
                                    content,
                                    model: selectedModel,
                                    response_time: responseTimeMs,
                                    token_usage: tokenUsage,
                                    metadata,
                                    created_at: assistantMessageCreatedAt
                                }
                            });
                            await saveConversationMemoryChunks({
                                userId,
                                message: {
                                    id: assistantMessageId,
                                    conversation_id: convId,
                                    role: 'assistant',
                                    content,
                                    metadata,
                                    created_at: assistantMessageCreatedAt
                                }
                            });
                            await prisma.conversations.updateMany({
                                where: { id: convId, user_id: userId },
                                data: { last_message_at: assistantMessageCreatedAt }
                            });
                            scheduleConversationSummaryUpdate({
                                conversationId: convId,
                                latestUserMessage: currentUserHistoryMessage?.content,
                                latestAssistantMessage: content,
                                latestUserMetadata: userMessageMetadata,
                                latestAssistantMetadata: metadata
                            });
                        }

                        await redis.rpush(redisKey, JSON.stringify(botMessage));
                        await redis.ltrim(redisKey, -MAX_HISTORY, -1);
                        await redis.expire(redisKey, REDIS_TTL);

                        return assistantMessageId;
                    };

                    const streamPostMapEventReply = async (
                        eventName: 'map' | 'map_style' | 'map_filter' | 'map_clear',
                        eventPayload: unknown
                    ) => {
                        const latestUserText = message || toSuggestionString(lastMessageForLLM?.content) || '';
                        const targetLanguage = /[\u0E00-\u0E7F]/.test(latestUserText) ? 'Thai' : 'English';
                        const containsChineseText = (value: string) => /[\u3400-\u9FFF]/.test(value);
                        const containsInternalGroundingText = (value: string) => {
                            const normalized = value.trim().toLowerCase();
                            return normalized.includes('grounding only')
                                || normalized.includes('"event"')
                                || normalized.includes('map_style_filter')
                                || normalized.startsWith('{')
                                || normalized.startsWith('[');
                        };
                        const groundingPayload = eventName === 'map_filter'
                            ? { action: 'map_filter_applied' }
                            : eventPayload;
                        const buildReplyMessages = (retry = false) => [
                            {
                                role: 'system',
                                content: [
                                    'You are a chatbot responding after a map UI action has already completed.',
                                    `Reply only in ${targetLanguage}.`,
                                    'Never use Chinese characters unless the latest user message itself is Chinese.',
                                    'Use one short natural sentence.',
                                    'Do not call tools.',
                                    'Do not output JSON.',
                                    'Do not mention backend events, APIs, coordinates, bounds, or zoom levels.',
                                    eventName === 'map'
                                        ? 'Tell the user that the requested map layer is ready/displayed.'
                                        : eventName === 'map_style'
                                            ? 'Tell the user that the requested map style has been applied.'
                                            : eventName === 'map_filter'
                                                ? 'Tell the user only that the requested map filter has been applied. Do not describe colors, legends, categories, or style details.'
                                                : 'Tell the user that the requested map layer clear action has been applied.',
                                    retry ? `The previous draft exposed internal grounding or used the wrong language. Rewrite it in ${targetLanguage} only as a user-facing confirmation sentence.` : '',
                                    `Grounding only: ${JSON.stringify(groundingPayload).slice(0, 400)}`
                                ].filter(Boolean).join('\n')
                            },
                            {
                                role: 'user',
                                content: eventName === 'map_filter'
                                    ? 'Confirm the completed map filter action.'
                                    : latestUserText || 'Confirm the completed map action.'
                            }
                        ];

                        const generateReply = async (retry = false) => {
                            const response = await fetch(`${OLLAMA_URL}/api/chat`, {
                                method: 'POST',
                                headers: { 'Content-Type': 'application/json' },
                                body: JSON.stringify({
                                    model: selectedModel,
                                    messages: buildReplyMessages(retry),
                                    stream: false,
                                    options: {
                                        temperature: retry ? 0.1 : 0.25,
                                        top_p: 0.8,
                                        num_predict: 64
                                    }
                                })
                            });

                            if (!response.ok) return { reply: '', tokenUsage: 0 };
                            const chunk = await response.json();
                            return {
                                reply: toSuggestionString(asRecord(chunk.message).content) || '',
                                tokenUsage: typeof chunk?.eval_count === 'number' ? chunk.eval_count : 0
                            };
                        };

                        let result = await generateReply(false);
                        if (containsChineseText(result.reply) || containsInternalGroundingText(result.reply)) {
                            result = await generateReply(true);
                        }
                        if (!result.reply) return result;

                        writeSse(controller, 'token', { text: result.reply });
                        return result;
                    };

                    const isMapOptionsMetadata = (metadata: unknown) => {
                        return asRecord(metadata).event === 'map_options';
                    };

                    const updateCachedAssistantMessageMetadata = async (
                        messageId: string,
                        metadata: Prisma.InputJsonObject
                    ) => {
                        const cachedMessages = await redis.lrange(redisKey, 0, -1);
                        if (cachedMessages.length === 0) return;

                        let changed = false;
                        const nextMessages = cachedMessages.map((cachedMessage) => {
                            try {
                                const parsed = JSON.parse(cachedMessage) as Record<string, unknown>;
                                if (parsed.id !== messageId) return cachedMessage;

                                changed = true;
                                return JSON.stringify({
                                    ...parsed,
                                    metadata
                                });
                            } catch {
                                return cachedMessage;
                            }
                        });

                        if (!changed) return;

                        await redis.del(redisKey);
                        await redis.rpush(redisKey, ...nextMessages);
                        await redis.expire(redisKey, REDIS_TTL);
                    };

                    const findLatestMapOptionsMessageId = async () => {
                        const cachedMessageId = typeof savedMapSelectionArgs?.mapOptionsMessageId === 'string'
                            ? savedMapSelectionArgs.mapOptionsMessageId
                            : undefined;
                        if (cachedMessageId) return cachedMessageId;

                        for (const message of [...messagesForLLM].reverse()) {
                            const record = asRecord(message);
                            if (record.role !== 'assistant') continue;
                            if (!isMapOptionsMetadata(record.metadata)) continue;

                            const id = typeof record.id === 'string' ? record.id : undefined;
                            if (id) return id;
                        }

                        const recentAssistantMessages = await prisma.messages.findMany({
                            where: {
                                conversation_id: convId,
                                role: 'assistant',
                                deleted_at: null
                            },
                            orderBy: [{ created_at: 'desc' }, { id: 'desc' }],
                            take: 30,
                            select: {
                                id: true,
                                metadata: true
                            }
                        });

                        const latestMapOptionsMessage = recentAssistantMessages.find((message) => {
                            return isMapOptionsMetadata(message.metadata);
                        });

                        return latestMapOptionsMessage?.id || null;
                    };

                    const rememberMapOptionsMessageId = async (messageId: string | null) => {
                        if (!messageId) return;

                        savedMapSelectionArgs = mergeMapToolArgs(savedMapSelectionArgs, {
                            mapOptionsMessageId: messageId
                        });
                        await redis.set(mapSelectionStateKey, JSON.stringify(savedMapSelectionArgs), 'EX', REDIS_TTL);
                    };

                    const saveOrUpdateMapOptionsMessage = async (
                        payload: ReturnType<typeof buildMapOptionsEvent>,
                        shouldUpdateExisting: boolean
                    ) => {
                        const metadata = createMapOptionsMetadata(payload);

                        if (!shouldUpdateExisting) {
                            const createdMessageId = await saveAssistantMessage('', metadata);
                            await rememberMapOptionsMessageId(createdMessageId);
                            return createdMessageId;
                        }

                        const latestMapOptionsMessageId = await findLatestMapOptionsMessageId();
                        if (!latestMapOptionsMessageId) {
                            return null;
                        }

                        const updateResult = await prisma.messages.updateMany({
                            where: {
                                id: latestMapOptionsMessageId,
                                conversation_id: convId,
                                role: 'assistant',
                                deleted_at: null
                            },
                            data: { metadata }
                        });
                        if (updateResult.count === 0) {
                            savedMapSelectionArgs = undefined;
                            await redis.del(mapSelectionStateKey);
                            return null;
                        }

                        await updateCachedAssistantMessageMetadata(latestMapOptionsMessageId, metadata);
                        await saveConversationMemoryChunks({
                            userId,
                            message: {
                                id: latestMapOptionsMessageId,
                                conversation_id: convId,
                                role: 'assistant',
                                content: '',
                                metadata,
                                created_at: new Date()
                            }
                        });
                        await rememberMapOptionsMessageId(latestMapOptionsMessageId);

                        return latestMapOptionsMessageId;
                    };

                    if (
                        mapFeaturesEnabled
                        && latestMapPayload
                        && latestMapStyle
                        && hasUserMessage
                        && !hasMapSelection
                        && isPmtilesRenderRequest(message)
                    ) {
                        const pmtilesResult = await handleRenderPmtilesLayerTool(
                            userId,
                            latestMapPayload,
                            mapHeaderApiKey
                        );

                        if (pmtilesResult.success && pmtilesResult.payload) {
                            const mapStylePayload = await writeMapSourceSwitchEvents(pmtilesResult.payload, latestMapStyle);
                            const mapMetadata = createMapMetadata(pmtilesResult.payload, mapStylePayload);
                            const postReply = await streamPostMapEventReply('map', pmtilesResult.payload);
                            const assistantMessageId = await saveAssistantMessage(
                                postReply.reply,
                                mapMetadata,
                                0,
                                postReply.tokenUsage
                            );
                            writeSse(controller, 'done', {
                                done: true,
                                tokenUsage: postReply.tokenUsage,
                                assistantmessage_Id: assistantMessageId,
                                skippedAssistantReply: true,
                                reason: 'pmtiles_layer_ready'
                            });
                            closeSafely();
                            return;
                        }

                        const mapErrorMessage = getToolErrorMessage(pmtilesResult, 'Unable to build the PMTiles URL for the current layer.');
                        writeSse(controller, 'map_error', { message: mapErrorMessage });
                        const assistantMessageId = await saveAssistantMessage(
                            mapErrorMessage,
                            toPrismaJsonObject({ event: 'map_error', message: mapErrorMessage })
                        );
                        writeSse(controller, 'token', { text: mapErrorMessage });
                        writeSse(controller, 'done', {
                            done: true,
                            tokenUsage: 0,
                            assistantmessage_Id: assistantMessageId
                        });
                        closeSafely();
                        return;
                    }

                    if (
                        mapRequestIntent === 'map_control'
                        && latestMapPayload
                        && latestMapStyle
                        && hasUserMessage
                        && !hasMapSelection
                    ) {
                        const styleCatalog = await handleStyleCatalogTool();
                        const isPaintEditRequest = isPaintStyleEditInstruction(message, latestMapStyle);
                        const requestedStyleKey = !isPaintEditRequest && styleCatalog.success && Array.isArray(styleCatalog.styles)
                            ? resolveRequestedMapStyleKey(message, styleCatalog.styles, latestMapStyle)
                            : undefined;

                        if (requestedStyleKey) {
                            const styleResult = await buildMapStylePayload(latestMapPayload, {
                                presetKey: requestedStyleKey,
                                instruction: message
                            });

                            if (styleResult.success) {
                                const preservedStyleResult = mergeCurrentMapStyleIntoPreset(styleResult, latestMapStyle, latestMapPayload);
                                const styleResultWithVariants = mergeMapStyleAttributeVariants(preservedStyleResult, latestMapStyle, latestMapPayload);
                                writeSse(controller, 'map_style', toPublicMapStyleStreamPayload(styleResultWithVariants));
                                await safeSyncConversationMapStyle(convId, styleResultWithVariants);
                                const suggestionsPayload = await enrichMapSuggestionsWithAttributeValues(
                                    buildMapSuggestionsPayload(latestMapPayload, styleResultWithVariants, styleCatalog, conversationMapState, hasUserMessage ? message : undefined),
                                    latestMapPayload,
                                    userId,
                                    mapHeaderApiKey,
                                    false
                                );
                                const splitSuggestions = splitMapSuggestionsPayload(suggestionsPayload);
                                if (splitSuggestions.attributeValues) {
                                    writeSse(controller, 'attribute_values', splitSuggestions.attributeValues);
                                }
                                if (splitSuggestions.suggestions) {
                                    writeSse(controller, 'suggestions', splitSuggestions.suggestions);
                                }
                                const styleMetadata = createMapStyleMetadata(styleResultWithVariants);
                                const postReply = await streamPostMapEventReply('map_style', styleResultWithVariants);
                                const assistantMessageId = await saveAssistantMessage(
                                    postReply.reply,
                                    styleMetadata,
                                    0,
                                    postReply.tokenUsage
                                );
                                writeSse(controller, 'done', {
                                    done: true,
                                    tokenUsage: postReply.tokenUsage,
                                    assistantmessage_Id: assistantMessageId,
                                    skippedAssistantReply: true,
                                    reason: 'map_style_ready'
                                });
                                closeSafely();
                                return;
                            }
                        }
                    }

                    const wantsMapAccess = mapRequestIntent === 'map_access';
                    const wantsMapControl = mapRequestIntent === 'map_control';
                    const shouldIsolateVisionChat = hasImages
                        && mapRequestIntent === 'chat'
                        && !hasMapSelection;
                    const hasActiveMapLayers = mapFeaturesEnabled && Object.keys(conversationMapState?.layers || {}).length > 0;
                    const shouldOfferMapStyleEdit = Boolean(mapFeaturesEnabled && wantsMapControl && latestMapStyle && hasUserMessage);
                    const shouldOfferMapLayerClear = Boolean(mapFeaturesEnabled && wantsMapControl && hasActiveMapLayers && hasUserMessage);
                    const shouldRequireMapApiKey = mapFeaturesEnabled && wantsMapAccess && !hasMapApiKey;
                    const shouldHandleMap = mapFeaturesEnabled && (hasMapSelection || (wantsMapAccess && hasMapApiKey));

                    if (
                        shouldOfferMapStyleEdit
                        && visionAnalysis?.summary
                        && !visionAnalysis.dominantColors
                    ) {
                        await startVisionColorExtraction(true);
                    }

                    if (shouldOfferMapStyleEdit || shouldOfferMapLayerClear) {
                        const styleCatalog = await handleStyleCatalogTool();
                        const colorKeys = styleCatalog.success && Array.isArray(styleCatalog.colors)
                            ? styleCatalog.colors.map((color) => color.key)
                            : [];
                        const latestVisionForStyle = visionAnalysis || memoryPayload.latestVision || null;
                        const currentMapEditContext = latestMapStyle
                            ? buildCurrentMapEditContext(latestMapPayload, latestMapStyle)
                            : {};
                        mapStyleContext = {
                            role: 'system',
                            content: [
                                ...(shouldOfferMapStyleEdit
                                    ? ['Latest active map_style is available. If the user asks to change a map paint/layout property, call edit_map_style.']
                                    : []),
                                'If the user asks to clear displayed map layers or styles, call clear_map_layers. Use mode "selected" for one or more named entries, or mode "all" for every displayed entry. You may pass layerId, styleId, layerTitle, or layerIds; the backend resolves them against conversation map state.',
                                'Do not call get_map_layer for style-only edits.',
                                'Do not call get_map_layer for map layer clear commands.',
                                ...(shouldOfferMapStyleEdit
                                    ? [
                                        'Filter requests have priority over style-by-attribute requests. When the user intent is to limit rendered features by an attribute value, call edit_map_style with a filter operation, not update_layer.',
                                        'If the user asks to style by an attribute/field or names a map attribute, call edit_map_style with operation "update_layer" and include attributeKey when you can. The backend can fetch attribute values/stats and build paint expressions.',
                                        'For an imperative request shaped like "style the map attribute FIELD", you must call edit_map_style with operation "update_layer" and attributeKey FIELD. Do not answer with instructions about how to style it.',
                                        'An attribute may drive any compatible paint property already present in Current map_style, not only color. When the user names opacity, radius, width, stroke, or another paint property, call edit_map_style with attributeKey and the matching exact paintKey from Current map_style.',
                                        'If the user names an attribute value/category and a color, call edit_map_style with attributeKey, attributeValue, and colorKey/colorValue when possible.',
                                        'If the user changes one attribute value for a non-color paint property, call edit_map_style with attributeKey, attributeValue, the exact paintKey, and the requested numeric/string value.',
                                        'If the user changes multiple values of one attribute at once, call edit_map_style once with attributeKey, paintKey, and attributePatches containing each attributeValue and requested output/value/colorValue.',
                                        'If the user asks to change the base/ramp/min/max colors of an existing numeric attribute style, call edit_map_style with attributeKey, paintKey, and outputs or colorValue, but omit attributeValue so the backend recolors the existing stops instead of adding a new stop.',
                                        'If the user asks to change an attribute or map style color like/from/same as/based on the current image, call edit_map_style with colorValue from Latest vision memory dominantColors. Include attributeKey when the user names one.',
                                        'For direct paint/layout edits, call edit_map_style with paintKey or layoutKey plus value when possible.',
                                        'If the user asks to add any style property to the current layer, call edit_map_style with operation "add_property" and use the exact paintKey or layoutKey named in the prompt plus value/colorKey/colorValue.',
                                        'If the user asks to remove/delete a style property, call edit_map_style with operation "remove_property". Semantically map the user wording to the closest exact paintKey/layoutKey that already exists in Current map_style; do not invent a new property key.',
                                        'For remove_property, ignore any trailing property value in the user wording because removing a property does not set a value.',
                                        'If the user intent is to limit feature visibility by attribute/value conditions, call edit_map_style with operation "add_filter" instead of changing paint. Filters only add conditions to the current map style.',
                                        'For filter operations, send filterConditions with attributeKey, operator, and value or values. Use filterLogic "all" when every condition must match and "any" when at least one condition may match. Attribute names and types are validated against the selected layer catalog.',
                                        'Normalize a single requested color into colorKey from the style color catalog or a valid colorValue hex. Never combine multiple requested colors into one color; assign them to their requested attribute values or properties.',
                                        'If the user asks to use/add/apply/change colors from the current or previous image/photo, including wording like "like image", read Latest vision memory dominantColors and call edit_map_style with colorValue from the best matching dominant color hex. Do not answer text-only for this request.',
                                        'If the user names a non-active style such as circle, heatmap, fill, line, or 3d_extrusion, call edit_map_style with target/style wording so the backend can edit that saved style instead of only the latest active style.',
                                        `Available colorKeys: ${JSON.stringify(colorKeys)}`,
                                        `Latest vision memory: ${JSON.stringify(latestVisionForStyle)}`,
                                        `Current map edit context from conversation_map_layers: ${JSON.stringify(currentMapEditContext)}`,
                                        `Current map_style: ${JSON.stringify(latestMapStyle)}`
                                    ]
                                    : []),
                                `Conversation map state: ${JSON.stringify(conversationMapState || null)}`
                            ].join('\n')
                        };
                    }

                    if (shouldRequireMapApiKey) {
                        writeSse(controller, 'map_error', {
                            message: 'missing_x_api_key',
                            needsApiKey: true,
                            apiKeyHeader: 'X-API-Key',
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

                    const sentMapOptionPayloads = new Set<string>();
                    const writeMapOptionsEvent = (payload: ReturnType<typeof buildMapOptionsEvent>) => {
                        const fingerprint = buildMapOptionsFingerprint(payload);
                        if (sentMapOptionPayloads.has(fingerprint)) {
                            return false;
                        }

                        sentMapOptionPayloads.add(fingerprint);
                        writeSse(controller, 'map_options', toPublicMapOptionsEvent(payload));
                        return true;
                    };
                    const handleMapFlowException = (error: unknown, fallbackMessage: string) => {
                        console.error('Map Flow Error:', error);
                        const message = error instanceof Error && error.message
                            ? error.message
                            : fallbackMessage;
                        const safeMessage = message.toLowerCase().includes('api_key')
                            ? fallbackMessage
                            : message;
                        writeSse(controller, 'map_error', { message: safeMessage });
                        assistantEventMetadata = toPrismaJsonObject({
                            event: 'map_error',
                            message: safeMessage
                        });
                        return safeMessage;
                    };

                    let savedMapSelectionArgs: Record<string, unknown> | undefined;
                    let inferredMapArgs: Record<string, unknown> | undefined;
                    if (shouldHandleMap) {
                        if (hasMapSelection) {
                            const cachedMapSelection = await redis.get(mapSelectionStateKey);
                            if (cachedMapSelection) {
                                try {
                                    savedMapSelectionArgs = JSON.parse(cachedMapSelection) as Record<string, unknown>;
                                } catch {
                                    savedMapSelectionArgs = undefined;
                                }
                            }

                            if (!savedMapSelectionArgs && isMapOptionPaginationAction) {
                                const latestMapOptions = await prisma.messages.findFirst({
                                    where: {
                                        conversation_id: convId,
                                        role: 'assistant',
                                        deleted_at: null,
                                        metadata: {
                                            path: ['event'],
                                            equals: 'map_options'
                                        }
                                    },
                                    orderBy: [{ created_at: 'desc' }, { id: 'desc' }],
                                    select: {
                                        metadata: true
                                    }
                                });
                                const payload = asRecord(asRecord(latestMapOptions?.metadata).payload);
                                const selectedValues = asRecord(payload.selectedValues);
                                const restoredParams = {
                                    ...selectedValues,
                                    ...(Object.keys(asRecord(payload.pagination)).length > 0 ? { pagination: payload.pagination } : {})
                                };
                                savedMapSelectionArgs = {
                                    ...(typeof payload.intentName === 'string' ? { intentName: payload.intentName } : {}),
                                    ...(typeof payload.provider === 'string' ? { provider: payload.provider } : {}),
                                    ...(Object.keys(restoredParams).length > 0 ? { params: restoredParams } : {}),
                                    selectedOptions: {
                                        ...(typeof payload.intentName === 'string' ? { intentName: payload.intentName } : {}),
                                        ...(typeof payload.provider === 'string' ? { provider: payload.provider } : {})
                                    }
                                };
                            }
                        } else {
                            await redis.del(mapSelectionStateKey);
                        }
                    }

                    const mapSelectionArgs = normalizeMapSelectionArgs(mapSelectionPayload);
                    const buildContextualMapToolArgs = (aiArguments: Record<string, unknown>) => {
                        const latestQueryArgs = hasUserMessage
                            ? { query: message, message }
                            : undefined;
                        const mergedArgs = mergeMapToolArgs(savedMapSelectionArgs, mapSelectionArgs, inferredMapArgs, latestQueryArgs, aiArguments);
                        return shouldStartFreshMapOptions
                            ? stripMapChoiceSelectionArgs(mergedArgs)
                            : mergedArgs;
                    };
                    const buildMapOptionsToolArgs = (aiArguments: Record<string, unknown>) => {
                        const contextualArgs = buildContextualMapToolArgs(aiArguments);
                        return hasCurrentMapSelectionParam(mapSelectionPayload)
                            ? contextualArgs
                            : { ...contextualArgs, optionsOnly: true };
                    };
                    const persistMapSelectionState = async (payload: ReturnType<typeof buildMapOptionsEvent>) => {
                        const selectedValues = asRecord(payload.selectedValues);
                        const statePatch = {
                            ...(payload.intentName ? { intentName: payload.intentName } : {}),
                            ...(payload.provider ? { provider: payload.provider } : {}),
                            selectedOptions: {
                                ...(payload.intentName ? { intentName: payload.intentName } : {}),
                                ...(payload.provider ? { provider: payload.provider } : {})
                            },
                            ...(
                                Object.keys(selectedValues).length > 0 || payload.paginationState
                                    ? {
                                        params: {
                                            ...selectedValues,
                                            ...(payload.paginationState ? { pagination: payload.paginationState } : {})
                                        }
                                    }
                                    : {}
                            )
                        };

                        if (Object.keys(statePatch).length === 0) return;

                        savedMapSelectionArgs = mergeMapToolArgs(savedMapSelectionArgs, statePatch);
                        await redis.set(mapSelectionStateKey, JSON.stringify(savedMapSelectionArgs), 'EX', REDIS_TTL);
                    };
                    const clearMapSelectionState = async () => {
                        savedMapSelectionArgs = undefined;
                        await redis.del(mapSelectionStateKey);
                    };
                    let dynamicMapOptionToolSchema: unknown = mapOptionToolSchema;
                    let mapAccessContext: { role: string; content: string } | undefined;
                    let sentMapAccessEvent = false;

                    if (shouldHandleMap) {
                        const mapAccessResult = await handleCheckMapAccess(
                            userId,
                            mapHeaderApiKey,
                            hasUserMessage ? message : undefined
                        );
                        writeSse(controller, 'map_access', mapAccessResult);
                        sentMapAccessEvent = true;

                        const mapConfigs = mapAccessResult.success
                            ? await resolveUserMapToolConfigs(userId, mapHeaderApiKey, hasUserMessage ? message : undefined)
                            : [];
                        const mapChoiceContext = buildMapOptionChoiceContext(mapConfigs);
                        const inferredFromMessage = hasUserMessage && mapChoiceContext.length > 0
                            ? inferMapArgsFromDbChoiceText(message, mapChoiceContext)
                            : undefined;
                        const singleResolvedConfig = mapConfigs.length === 1 ? mapConfigs[0] : undefined;
                        inferredMapArgs = mergeMapToolArgs(
                            inferredFromMessage,
                            singleResolvedConfig
                                ? {
                                    intentName: singleResolvedConfig.intentName,
                                    provider: singleResolvedConfig.provider,
                                    selectedOptions: {
                                        intentName: singleResolvedConfig.intentName,
                                        provider: singleResolvedConfig.provider
                                    }
                                }
                                : undefined
                        );
                        dynamicMapOptionToolSchema = mapConfigs.length > 0
                            ? buildDynamicMapOptionToolSchema(mapConfigs)
                            : mapOptionToolSchema;
                        mapAccessContext = {
                            role: 'system',
                            content: `Map access context for this user. Use this only when the user asks for map/layer data. Pick provider and intentName from these configs and never invent access outside this list: ${JSON.stringify(mapAccessResult)}
If the user asks for map/layer data and there is no complete mapSelection yet, do not ask a normal text follow-up first. Call the map_options tool immediately so the backend can return DB/API-backed choices.
DB-backed map choice context for semantic matching: ${JSON.stringify(mapChoiceContext)}
Inferred params already extracted from the latest user message by the map inference pass: ${JSON.stringify(inferredMapArgs || {})}
For VALLARIS, always include the latest user message in query/message when calling map_options or get_map_layer. Pick the intentName by matching the user request with each config's intentName, type, handler, itemType, and optionKey from the DB-backed context. If the config handler is collection_detail or has an itemType such as Tile/CoverageTile, call map_options for layerId choices from the collection endpoint. If the UI sends mapSelection key/value filters, keep those selected params so the backend can forward them to the collection endpoint. If the config is a style catalog, the backend will match styleId and ask for map type links. Never expose provider API keys in map_options choices.
Infer params from the user's wording and the DB-backed enum descriptions in the map_options tool schema, including natural day/date wording into the matching dayPath choice value. Include inferred values in map_options.params. Do not call map_options with empty params when the user's wording already matches a choice. If the user already selected values in mapSelection, keep those values and continue with the next missing option.
For URL/template placeholders, ask the user using only the DB-backed map_options choices. When hazard/dayPath/type or other required placeholders are complete, call get_map_layer with params.`
                        };

                        if (hasMapSelection) {
                            const contextualArguments = buildMapOptionsToolArgs({});
                            const optionResult = await handleMapOptionsTool(userId, contextualArguments, mapHeaderApiKey);
                            const optionPayload = buildMapOptionsEvent(optionResult);
                            await persistMapSelectionState(optionPayload);

                            if (!optionPayload.complete) {
                                const wroteOptionPayload = writeMapOptionsEvent(optionPayload);
                                const assistantMessageId = wroteOptionPayload
                                    ? await saveOrUpdateMapOptionsMessage(optionPayload, isMapOptionPaginationAction)
                                    : null;

                                writeSse(controller, 'done', {
                                    done: true,
                                    tokenUsage: 0,
                                    assistantmessage_Id: assistantMessageId,
                                    skippedAssistantReply: true,
                                    reason: 'map_options_ready'
                                });
                                closeSafely();
                                return;
                            }

                            if (optionPayload.intentName && optionPayload.provider) {
                                const selectedValues = asRecord(optionPayload.selectedValues);
                                const mapResult = await handleMapTool(
                                    userId,
                                    {
                                        ...contextualArguments,
                                        intentName: optionPayload.intentName,
                                        provider: optionPayload.provider,
                                        params: {
                                            ...selectedValues,
                                            ...asRecord(contextualArguments.params)
                                        },
                                        options: {
                                            ...selectedValues,
                                            ...asRecord(contextualArguments.options)
                                        }
                                    },
                                    mapHeaderApiKey
                                );

                                if (mapResult.success) {
                                    await clearMapSelectionState();
                                    const mapStylePayload = await writeMapResultEvents(mapResult.payload);
                                    const mapMetadata = createMapMetadata(mapResult.payload, mapStylePayload);
                                    const postReply = await streamPostMapEventReply('map', mapResult.payload);
                                    const assistantMessageId = await saveAssistantMessage(
                                        postReply.reply,
                                        mapMetadata,
                                        0,
                                        postReply.tokenUsage
                                    );
                                    writeSse(controller, 'done', {
                                        done: true,
                                        tokenUsage: postReply.tokenUsage,
                                        assistantmessage_Id: assistantMessageId,
                                        skippedAssistantReply: true,
                                        reason: 'map_ready'
                                    });
                                    closeSafely();
                                    return;
                                }

                                if (mapResult.needsOptions && mapResult.payload) {
                                    const nextOptionPayload = buildMapOptionsEvent(mapResult.payload);
                                    await persistMapSelectionState(nextOptionPayload);
                                    const wroteNextOptionPayload = writeMapOptionsEvent(nextOptionPayload);
                                    const assistantMessageId = wroteNextOptionPayload
                                        ? await saveOrUpdateMapOptionsMessage(nextOptionPayload, isMapOptionPaginationAction)
                                        : null;
                                    writeSse(controller, 'done', {
                                        done: true,
                                        tokenUsage: 0,
                                        assistantmessage_Id: assistantMessageId,
                                        skippedAssistantReply: true,
                                        reason: 'map_options_ready'
                                    });
                                    closeSafely();
                                    return;
                                }
                            }
                        }
                    }
                    const mapSelectionContext = hasMapSelection
                        ? {
                            role: 'system',
                            content: `The user selected these map options in the UI. Treat these as DB-backed params/options for get_map_layer, validate via map_options if uncertain, and call get_map_layer when every required placeholder is present: ${JSON.stringify(mapSelectionPayload)}`
                        }
                        : undefined;
                    const visionContext = visionAnalysis
                        ? {
                            role: 'system',
                            content: [
                                `The user attached image(s). A vision model (${usedVisionModel}) analyzed them before this chat response.`,
                                'Use this analysis as image context; do not claim you directly saw pixels beyond this analysis.',
                                'When answering the user, describe colors by natural color names only. Do not mention hex values unless the user explicitly asks for hex/color codes.',
                                'If the user asks to style a map from the image, you may use the hex values internally for map styling intent.',
                                `Vision analysis JSON:\n${JSON.stringify(visionAnalysis)}`
                            ].join('\n')
                        }
                        : undefined;

                    const messagesForOllama = [
                        systemMessage,
                        ...(shouldIsolateVisionChat ? [] : sanitizedMessagesForLLM),
                        styleReminder,
                        ...(!shouldIsolateVisionChat && conversationSummaryContext ? [conversationSummaryContext] : []),
                        ...(!shouldIsolateVisionChat && conversationMemoryContext ? [conversationMemoryContext] : []),
                        ...(!shouldIsolateVisionChat && retrievedMemoryContext ? [retrievedMemoryContext] : []),
                        ...(visionContext ? [visionContext] : []),
                        ...(!shouldIsolateVisionChat && mapAccessContext ? [mapAccessContext] : []),
                        ...(!shouldIsolateVisionChat && mapSelectionContext ? [mapSelectionContext] : []),
                        ...(!shouldIsolateVisionChat && mapToolContext ? [mapToolContext] : []),
                        ...(!shouldIsolateVisionChat && mapStyleContext ? [mapStyleContext] : []),
                        ...(lastMessageForLLM ? [lastMessageForLLM] : [])
                    ];
                    // เรียก Ollama แบบ stream เพื่อรับ token ทีละส่วน
                    const startTime = Date.now();
                    const ollamaPayload: Record<string, unknown> = {
                        model: selectedModel,
                        messages: messagesForOllama,
                        stream: true
                    };
                    if (shouldHandleMap) {
                        ollamaPayload.tools = [
                            checkMapAccessSchema,
                            dynamicMapOptionToolSchema,
                            mapToolSchema
                        ] as unknown[];
                    }
                    if (shouldOfferMapStyleEdit || shouldOfferMapLayerClear) {
                        ollamaPayload.tools = [
                            ...((ollamaPayload.tools as unknown[] | undefined) || []),
                            ...(shouldOfferMapStyleEdit ? [editMapStyleToolSchema] : []),
                            clearMapLayersToolSchema
                        ];
                    }
                    if (selectedFeelingKey === 'aggressive') {
                        ollamaPayload.options = { temperature: 0.7, top_p: 0.9 };
                    } else if (selectedFeelingKey === 'polite') {
                        ollamaPayload.options = { temperature: 0.45, top_p: 0.85 };
                    }
                    ollamaPayload.options = {
                        ...((ollamaPayload.options as Record<string, unknown> | undefined) || {}),
                        num_predict: shouldHandleMap ? 128 : DEFAULT_OUTPUT_TOKENS
                    };
                    
                    ollamaAbortController = new AbortController();
                    const ollamaResponse = await fetch(`${OLLAMA_URL}/api/chat`, {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify(ollamaPayload),
                        signal: ollamaAbortController.signal
                    });

                    if (!ollamaResponse.ok || !ollamaResponse.body) {
                        writeSse(controller, 'error', { message: 'ollama_request_failed' });
                        closeSafely();
                        return;
                    }

                    // Convert Ollama chunks into SSE tokens while intercepting tool calls.
                    let assistantReply = '';
                    let tokenUsage = 0;
                    const decoder = new TextDecoder();
                    const reader = ollamaResponse.body.getReader();
                    ollamaReader = reader;
                    let buffer = '';
                    const handledToolCalls = new Set<string>();
                    let loggedNoToolDecision = false;

                    const executeEditMapStyle = async (aiArguments: Record<string, unknown>) => {
                        const selectedMapPayload = selectMapPayloadForEdit(conversationMapState, aiArguments, latestMapPayload, message);
                        const ensureSelectedMapStyle = async (mapStyle: unknown | undefined): Promise<unknown | undefined> => {
                            if (mapStyle) return mapStyle;
                            if (!selectedMapPayload) return undefined;

                            const defaultStyle = await buildMapStylePayload(selectedMapPayload);
                            return defaultStyle.success ? defaultStyle : undefined;
                        };
                        // Infer filter/style intent against the selected layer first;
                        // using only the latest global style breaks multi-layer chats.
                        const inferredMapStyle = await ensureSelectedMapStyle(
                            selectMapStyleForEdit(
                                conversationMapState,
                                aiArguments,
                                message,
                                latestMapStyle
                            )
                        );
                        const filterArguments = await buildFilterEditArgsFromInstruction(
                            aiArguments,
                            selectedMapPayload,
                            userId,
                            typeof aiArguments.instruction === 'string' ? aiArguments.instruction : message,
                            mapHeaderApiKey,
                            inferredMapStyle
                        );
                        const normalizedAiArguments = filterArguments || aiArguments;
                        const requestedEditOperation = normalizeMapEditOperationName(normalizedAiArguments.operation || normalizedAiArguments.action);
                        const isFilterEdit = requestedEditOperation?.endsWith('_filter') === true;
                        const initialMapStyle = await ensureSelectedMapStyle(
                            selectMapStyleForEdit(
                                conversationMapState,
                                normalizedAiArguments,
                                isFilterEdit ? '' : message,
                                latestMapStyle
                            )
                        );
                        let enrichedEditArguments = isFilterEdit
                            ? buildFilterEditArgs(normalizedAiArguments, selectedMapPayload, initialMapStyle)
                            : await buildAttributeEditArgs(
                                normalizedAiArguments,
                                selectedMapPayload,
                                userId,
                                typeof normalizedAiArguments.instruction === 'string' ? normalizedAiArguments.instruction : message,
                                mapHeaderApiKey,
                                initialMapStyle
                            );
                        if (!isFilterEdit) {
                            enrichedEditArguments = enrichStyleArgsWithVisionPalette(
                                enrichedEditArguments,
                                enrichedEditArguments.instruction || normalizedAiArguments.instruction || message,
                                visionAnalysis || memoryPayload.latestVision || null
                            );
                        }
                        const selectedMapStyle = await ensureSelectedMapStyle(
                            selectMapStyleForEdit(
                                conversationMapState,
                                enrichedEditArguments,
                                isFilterEdit ? '' : message,
                                initialMapStyle
                            )
                        );
                        const rawEditResult = await handleEditMapStyleTool(
                            {
                                ...enrichedEditArguments,
                                instruction: typeof enrichedEditArguments.instruction === 'string' ? enrichedEditArguments.instruction : message
                            },
                            selectedMapStyle
                        );
                        const editResult = rawEditResult.success
                            ? mergeMapStyleAttributeVariants(
                                isFilterEdit
                                    ? syncMapStyleFiltersToAttributeVariants(rawEditResult)
                                    : rawEditResult,
                                selectedMapStyle,
                                selectedMapPayload
                            )
                            : rawEditResult;

                        if (editResult.success) {
                            const mapStylePatch = buildAttributeMapStylePatch(enrichedEditArguments, editResult);
                            const mapFilterPatch = buildMapFilterPatch(enrichedEditArguments, editResult);
                            if (mapFilterPatch) {
                                writeSse(controller, 'map_filter_patch', mapFilterPatch);
                            } else if (mapStylePatch) {
                                writeSse(controller, 'map_style_patch', mapStylePatch);
                            }
                            if (!mapStylePatch && !mapFilterPatch) {
                                writeSse(controller, 'map_style', toPublicMapStyleStreamPayload(editResult));
                            }
                            const selectedLayerId = getLayerIdFromMapPayload(selectedMapPayload);
                            if (conversationMapState) {
                                applyMapStyleToState(conversationMapState, editResult, selectedLayerId);
                            }
                            latestMapStyle = editResult;
                            latestMapPayload = selectedMapPayload || latestMapPayload;
                            await safeSyncConversationMapStyle(convId, editResult);
                            const editedLayerId = getMapStyleLayerId(editResult);
                            if (selectedLayerId && (!editedLayerId || selectedLayerId === editedLayerId)) {
                                await safeSyncConversationMapLayerCatalog(convId, selectedMapPayload, editResult);
                            }
                            const styleCatalog = await handleStyleCatalogTool();
                            const isAttributeSelectionEdit = Boolean(
                                toSuggestionString(enrichedEditArguments.attributeKey)
                                && enrichedEditArguments.attributeValue === undefined
                                && (!Array.isArray(enrichedEditArguments.attributePatches) || enrichedEditArguments.attributePatches.length === 0)
                                && !hasAttributeRampOutputRequest(enrichedEditArguments)
                            );
                            const suggestionsPayload = await enrichMapSuggestionsWithAttributeValues(
                                buildMapSuggestionsPayload(selectedMapPayload, editResult, styleCatalog, conversationMapState, hasUserMessage ? message : undefined),
                                selectedMapPayload,
                                userId,
                                mapHeaderApiKey,
                                isAttributeSelectionEdit
                            );
                            const splitSuggestions = splitMapSuggestionsPayload(suggestionsPayload);
                            if (splitSuggestions.attributeValues) {
                                writeSse(controller, 'attribute_values', splitSuggestions.attributeValues);
                            }
                            if (splitSuggestions.suggestions) {
                                writeSse(controller, 'suggestions', splitSuggestions.suggestions);
                            }
                            mapMetadata = createMapStyleMetadata(editResult);

                            const postReply = await streamPostMapEventReply(
                                mapFilterPatch ? 'map_filter' : 'map_style',
                                mapFilterPatch || editResult
                            );
                            assistantReply += postReply.reply;
                            tokenUsage += postReply.tokenUsage;
                            return true;
                        }

                        const styleErrorMessage = getToolErrorMessage(editResult, 'Unable to edit the map style.');
                        writeSse(controller, 'map_error', { message: styleErrorMessage });
                        assistantReply += styleErrorMessage;
                        writeSse(controller, 'token', { text: styleErrorMessage });
                        return false;
                    };

                    const handleOllamaChunk = async (chunk: any) => {
                        const toolCalls = Array.isArray(chunk?.message?.tool_calls)
                            ? chunk.message.tool_calls
                            : [];
                        const textPart = chunk?.message?.content || '';
                        if (textPart && !shouldHandleMap && !shouldOfferMapStyleEdit && !shouldOfferMapLayerClear) {
                            assistantReply += textPart;
                            writeSse(controller, 'token', { text: textPart });
                        }

                        if (toolCalls.length > 0) {
                            console.log("=== AI tool-call decision ===");
                            console.log(JSON.stringify(toolCalls, null, 2));
                        }

                        for (const [index, toolCall] of toolCalls.entries()) {
                            const toolName = toolCall?.function?.name || toolCall?.name;
                            const toolCallKey = toolCall?.id || `${toolName}:${index}:${JSON.stringify(toolCall?.function?.arguments ?? toolCall?.arguments ?? {})}`;
                            if (handledToolCalls.has(toolCallKey)) continue;
                            handledToolCalls.add(toolCallKey);

                            console.log(`AI decided to call tool: ${toolName}`);
                            console.log('Tool arguments:', toolCall?.function?.arguments ?? toolCall?.arguments);

                            if (toolName === 'clear_map_layers') {
                                const aiArguments = parseToolArguments(toolCall?.function?.arguments ?? toolCall?.arguments);
                                const controlResult = await handleClearMapLayersTool(
                                    userId,
                                    convId,
                                    buildClearMapLayerArgs(aiArguments, message, conversationMapState)
                                );

                                if (controlResult.success) {
                                    writeSse(controller, 'map_clear', controlResult);
                                    await safeSyncConversationMapClear(convId, controlResult);
                                    mapMetadata = toPrismaJsonObject(controlResult);

                                    const postReply = await streamPostMapEventReply('map_clear', controlResult);
                                    assistantReply += postReply.reply;
                                    tokenUsage += postReply.tokenUsage;
                                } else {
                                    const controlErrorMessage = getToolErrorMessage(controlResult, 'Unable to manage map layers.');
                                    writeSse(controller, 'map_error', { message: controlErrorMessage });
                                    assistantReply += controlErrorMessage;
                                }
                                continue;
                            }

                            if (toolName === 'edit_map_style') {
                                const aiArguments = parseToolArguments(toolCall?.function?.arguments ?? toolCall?.arguments);
                                await executeEditMapStyle(aiArguments);
                                continue;
                            }

                            if (toolName === 'check_user_map') {
                                const accessResult = await handleCheckMapAccess(userId, mapHeaderApiKey, message);
                                if (!sentMapAccessEvent) {
                                    writeSse(controller, 'map_access', accessResult);
                                    sentMapAccessEvent = true;
                                }

                                if (!accessResult.success) {
                                    const accessErrorMessage = accessResult.message || 'No map access permission was found.';
                                    assistantReply += accessErrorMessage;
                                    writeSse(controller, 'token', { text: accessErrorMessage });
                                }
                                continue;
                            }

                            if (toolName === 'map_options') {
                                let aiArguments: Record<string, unknown>;
                                let contextualArguments: Record<string, unknown>;
                                let optionPayload: ReturnType<typeof buildMapOptionsEvent>;
                                let wroteOptionPayload = false;
                                try {
                                    aiArguments = parseToolArguments(toolCall?.function?.arguments ?? toolCall?.arguments);
                                    contextualArguments = buildMapOptionsToolArgs(aiArguments);
                                    const optionResult = await handleMapOptionsTool(userId, contextualArguments, mapHeaderApiKey);
                                    optionPayload = buildMapOptionsEvent(optionResult);
                                    await persistMapSelectionState(optionPayload);
                                    wroteOptionPayload = optionPayload.complete
                                        ? false
                                        : writeMapOptionsEvent(optionPayload);
                                } catch (error) {
                                    const mapErrorMessage = handleMapFlowException(error, 'Unable to build map options.');
                                    assistantReply += mapErrorMessage;
                                    continue;
                                }

                                if (optionPayload.complete && optionPayload.intentName && optionPayload.provider) {
                                    const selectedValues = asRecord(optionPayload.selectedValues);
                                    let mapResult: Awaited<ReturnType<typeof handleMapTool>>;
                                    try {
                                        mapResult = await handleMapTool(
                                            userId,
                                            {
                                                ...contextualArguments,
                                                intentName: optionPayload.intentName,
                                                provider: optionPayload.provider,
                                                params: {
                                                    ...selectedValues,
                                                    ...asRecord(contextualArguments.params)
                                                },
                                                options: {
                                                    ...selectedValues,
                                                    ...asRecord(contextualArguments.options)
                                                }
                                            },
                                            mapHeaderApiKey
                                        );
                                    } catch (error) {
                                        const mapErrorMessage = handleMapFlowException(error, 'Unable to fetch map layer.');
                                        assistantReply += mapErrorMessage;
                                        continue;
                                    }

                                    if (mapResult.success) {
                                        await clearMapSelectionState();
                                        const mapStylePayload = await writeMapResultEvents(mapResult.payload);
                                        mapMetadata = createMapMetadata(mapResult.payload, mapStylePayload);

                                        const postReply = await streamPostMapEventReply('map', mapResult.payload);
                                        assistantReply += postReply.reply;
                                        tokenUsage += postReply.tokenUsage;
                                    } else if (mapResult.needsOptions && mapResult.payload) {
                                        const nextOptionPayload = buildMapOptionsEvent(mapResult.payload);
                                        await persistMapSelectionState(nextOptionPayload);
                                        const wroteNextOptionPayload = writeMapOptionsEvent(nextOptionPayload);
                                        if (wroteNextOptionPayload) {
                                            assistantEventMetadata = createMapOptionsMetadata(nextOptionPayload);
                                        }
                                    } else {
                                        const mapErrorMessage = getToolErrorMessage(mapResult, 'Unable to fetch map data.');
                                        writeSse(controller, 'map_error', { message: mapErrorMessage });
                                        assistantReply += mapErrorMessage;
                                    }
                                    continue;
                                }

                                if (wroteOptionPayload) {
                                    assistantEventMetadata = createMapOptionsMetadata(optionPayload);
                                }
                                continue;
                            }

                            if (toolName !== 'get_map_layer') continue;

                            const aiArguments = parseToolArguments(toolCall?.function?.arguments ?? toolCall?.arguments);
                            const rawContextualArguments = buildContextualMapToolArgs(aiArguments);
                            const contextualArguments = !hasCurrentMapSelectionParam(mapSelectionPayload)
                                && !hasMapChoiceSelectionFromMessage(rawContextualArguments, message)
                                ? stripMapChoiceSelectionArgs(rawContextualArguments)
                                : rawContextualArguments;
                            let mapResult: Awaited<ReturnType<typeof handleMapTool>>;
                            try {
                                mapResult = await handleMapTool(userId, contextualArguments, mapHeaderApiKey);
                            } catch (error) {
                                const mapErrorMessage = handleMapFlowException(error, 'Unable to fetch map layer.');
                                assistantReply += mapErrorMessage;
                                continue;
                            }

                            if (mapResult.success) {
                                await clearMapSelectionState();
                                const mapStylePayload = await writeMapResultEvents(mapResult.payload);
                                mapMetadata = createMapMetadata(mapResult.payload, mapStylePayload);

                                const postReply = await streamPostMapEventReply('map', mapResult.payload);
                                assistantReply += postReply.reply;
                                tokenUsage += postReply.tokenUsage;
                            } else if (mapResult.needsOptions && mapResult.payload) {
                                const optionPayload = buildMapOptionsEvent(mapResult.payload);
                                await persistMapSelectionState(optionPayload);
                                const wroteOptionPayload = writeMapOptionsEvent(optionPayload);
                                if (wroteOptionPayload) {
                                    assistantEventMetadata = createMapOptionsMetadata(optionPayload);
                                }
                            } else {
                                const mapErrorMessage = getToolErrorMessage(mapResult, 'Unable to fetch map data.');
                                writeSse(controller, 'map_error', { message: mapErrorMessage });
                                assistantReply += mapErrorMessage;
                            }
                        }

                        if (chunk?.done && handledToolCalls.size === 0 && !loggedNoToolDecision) {
                            loggedNoToolDecision = true;
                            console.log("AI chose a plain text response without calling a tool.");

                            if (shouldOfferMapStyleEdit) {
                                const fallbackFilterArgs = await buildFilterEditArgsFromInstruction(
                                    {},
                                    latestMapPayload,
                                    userId,
                                    message,
                                    mapHeaderApiKey,
                                    latestMapStyle
                                );
                                if (fallbackFilterArgs) {
                                    const handledFallbackFilter = await executeEditMapStyle(fallbackFilterArgs);
                                    if (handledFallbackFilter) return;
                                }

                                const fallbackStylePropertyArgs = buildFallbackStylePropertyEditArgs(
                                    message,
                                    latestMapStyle
                                );
                                if (fallbackStylePropertyArgs) {
                                    const handledFallbackStyleProperty = await executeEditMapStyle(fallbackStylePropertyArgs);
                                    if (handledFallbackStyleProperty) return;
                                }

                                const fallbackEditArgs = buildFallbackAttributeStyleArgs(
                                    message,
                                    latestMapPayload,
                                    visionAnalysis || memoryPayload.latestVision || null
                                );
                                if (fallbackEditArgs) {
                                    const handledFallbackEdit = await executeEditMapStyle(fallbackEditArgs);
                                    if (handledFallbackEdit) return;
                                }
                            }

                            if (shouldHandleMap) {
                                let contextualArguments: Record<string, unknown>;
                                let optionPayload: ReturnType<typeof buildMapOptionsEvent>;
                                let wroteOptionPayload = false;
                                try {
                                    contextualArguments = buildMapOptionsToolArgs({});
                                    const optionResult = await handleMapOptionsTool(userId, contextualArguments, mapHeaderApiKey);
                                    optionPayload = buildMapOptionsEvent(optionResult);
                                    await persistMapSelectionState(optionPayload);
                                    wroteOptionPayload = optionPayload.complete
                                        ? false
                                        : writeMapOptionsEvent(optionPayload);
                                } catch (error) {
                                    const mapErrorMessage = handleMapFlowException(error, 'Unable to build map options.');
                                    assistantReply += mapErrorMessage;
                                    return;
                                }

                                if (optionPayload.complete && optionPayload.intentName && optionPayload.provider) {
                                    const selectedValues = asRecord(optionPayload.selectedValues);
                                    let mapResult: Awaited<ReturnType<typeof handleMapTool>>;
                                    try {
                                        mapResult = await handleMapTool(
                                            userId,
                                            {
                                                ...contextualArguments,
                                                intentName: optionPayload.intentName,
                                                provider: optionPayload.provider,
                                                params: {
                                                    ...selectedValues,
                                                    ...asRecord(contextualArguments.params)
                                                },
                                                options: {
                                                    ...selectedValues,
                                                    ...asRecord(contextualArguments.options)
                                                }
                                            },
                                            mapHeaderApiKey
                                        );
                                    } catch (error) {
                                        const mapErrorMessage = handleMapFlowException(error, 'Unable to fetch map layer.');
                                        assistantReply += mapErrorMessage;
                                        return;
                                    }

                                    if (mapResult.success) {
                                        await clearMapSelectionState();
                                        const mapStylePayload = await writeMapResultEvents(mapResult.payload);
                                        mapMetadata = createMapMetadata(mapResult.payload, mapStylePayload);

                                        const postReply = await streamPostMapEventReply('map', mapResult.payload);
                                        assistantReply += postReply.reply;
                                        tokenUsage += postReply.tokenUsage;
                                    } else if (mapResult.needsOptions && mapResult.payload) {
                                        const nextOptionPayload = buildMapOptionsEvent(mapResult.payload);
                                        await persistMapSelectionState(nextOptionPayload);
                                        const wroteNextOptionPayload = writeMapOptionsEvent(nextOptionPayload);
                                        if (wroteNextOptionPayload) {
                                            assistantEventMetadata = createMapOptionsMetadata(nextOptionPayload);
                                        }
                                    } else {
                                        const mapErrorMessage = getToolErrorMessage(mapResult, 'Unable to fetch map data.');
                                        writeSse(controller, 'map_error', { message: mapErrorMessage });
                                        assistantReply += mapErrorMessage;
                                    }
                                } else {
                                    if (wroteOptionPayload) {
                                        assistantEventMetadata = createMapOptionsMetadata(optionPayload);
                                    }
                                }
                            }
                        }

                        if (chunk?.done && typeof chunk?.eval_count === 'number') {
                            tokenUsage = chunk.eval_count;
                        }
                    };

                    while (!isClosed) {
                        const { done, value } = await reader.read();

                        if (done) {
                            const lastLine = buffer.trim();
                            if (lastLine) {
                                const chunk = JSON.parse(lastLine);
                                await handleOllamaChunk(chunk);
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
                            await handleOllamaChunk(chunk);
                        }
                    }
                    ollamaReader = null;
                    try {
                        reader.releaseLock();
                    } catch {
                        // Reader may already be released if the stream closed first.
                    }

                    // สรุปผลลัพธ์แล้วบันทึกฝั่ง assistant ลง DB/Redis
                    const responseTimeMs = Date.now() - startTime;
                    const assistantMetadata = mapMetadata || assistantEventMetadata;
                    const assistantMessageId = await saveAssistantMessage(
                        assistantReply,
                        assistantMetadata,
                        responseTimeMs,
                        tokenUsage
                    );

                    // แจ้งจบ stream ให้ frontend ปิด loading/state
                    writeSse(controller, 'done', {
                        done: true,
                        tokenUsage,
                        assistantmessage_Id: assistantMessageId,
                    });
                    closeSafely();
                } catch (error) {
                    if (isClosed || ollamaAbortController?.signal.aborted) {
                        return;
                    }
                    console.error('LLM Stream Error:', error);
                    // ส่ง error event แทนการปล่อย connection ตายเงียบ
                    writeSse(controller, 'error', { message: getStreamErrorMessage(error) });
                    closeSafely();
                } finally {
                    clearHeartbeat();
                    if (isClosed) {
                        abortOllamaStream();
                    } else {
                        ollamaAbortController = null;
                    }
                }
            };

            void run();
        },
        cancel() {
            isClosed = true;
            clearHeartbeat();
            abortOllamaStream();
        }
    });

    return { conversationId: convId, stream };
};

export const getConversationMapLayers = async (
    userId: string,
    role: string,
    conversationId: string
) => {
    if (role === 'guest') {
        return { success: true, layers: [] };
    }

    await verifyConversationAccess(userId, conversationId);
    const rows = await getConversationMapLayerRows(conversationId);

    return {
        layers: rows.map(toPublicConversationMapLayer)
    };
};

export const updateConversationMapLayerOrder = async (
    userId: string,
    role: string,
    conversationId: string,
    payload: MapLayerOrderPayload
) => {
    if (role === 'guest') {
        return { event: 'map_order', layerIds: [] };
    }

    await verifyConversationAccess(userId, conversationId);

    const rows = await getConversationMapLayerRows(conversationId);
    const requestedLayerIds = resolveMapLayerOrderIds(rows, payload);
    const remainingLayerIds = rows
        .map((row) => row.layerKey)
        .filter((layerId) => !requestedLayerIds.includes(layerId));
    const orderedLayerIds = [...requestedLayerIds, ...remainingLayerIds];

    for (const [index, layerId] of orderedLayerIds.entries()) {
        await prisma.$executeRaw`
            UPDATE "conversation_map_layers"
            SET
                "order" = ${index},
                "updated_at" = CURRENT_TIMESTAMP
            WHERE "conversation_id" = ${conversationId}
                AND "layer_key" = ${layerId}
                AND "deleted_at" IS NULL
        `;
    }

    const updatedRows = await getConversationMapLayerRows(conversationId);

    return {
        event: 'map_order',
        layerIds: updatedRows.map((row) => row.layerKey)
    };
};

type MapStyleHistoryEntry = {
    messageId: string;
    createdAt: Date;
    mapStyle: Record<string, unknown>;
    signature: string;
};

const stableJsonValue = (value: unknown): unknown => {
    if (Array.isArray(value)) return value.map(stableJsonValue);
    if (!value || typeof value !== 'object') return value;

    return Object.fromEntries(
        Object.entries(value as Record<string, unknown>)
            .sort(([left], [right]) => left.localeCompare(right))
            .map(([key, nestedValue]) => [key, stableJsonValue(nestedValue)])
    );
};

const getMapStyleHistorySignature = (mapStyle: unknown): string => {
    return JSON.stringify(stableJsonValue(mapStyle));
};

const getMapStyleHistorySelection = (selection: unknown): { layerId: string } | undefined => {
    const record = asRecord(selection);
    const key = typeof record.key === 'string' ? record.key.trim() : '';
    const layerId = toSuggestionString(record.value || record.layerId);
    if (!layerId) return undefined;

    if (key === 'mapundo') return { layerId };
    return undefined;
};

const restoreConversationMapLayerStyleFromHistory = async (
    userId: string,
    role: string,
    conversationId: string,
    layerId: string
) => {
    if (role === 'guest') {
        return {
            success: true,
            persisted: false,
            event: 'map_style',
            layerId
        };
    }

    await verifyConversationAccess(userId, conversationId);
    await ensureConversationMapLayersTable();

    const rows = await getConversationMapLayerRows(conversationId);
    const currentLayer = rows.find((row) => row.layerKey === layerId);
    if (!currentLayer) {
        throw Errors.badRequest('map layer not found in conversation');
    }

    const messages = await prisma.messages.findMany({
        where: {
            conversation_id: conversationId,
            deleted_at: null,
            metadata: { not: Prisma.JsonNull }
        },
        orderBy: [{ created_at: 'asc' }, { id: 'asc' }],
        select: {
            id: true,
            metadata: true,
            created_at: true
        }
    });
    const history: MapStyleHistoryEntry[] = messages.flatMap((message) => {
        const mapStyle = getMapStyleFromMessageMetadata(message.metadata);
        if (!mapStyle) return [];

        const styleLayerId = getMapStyleLayerId(mapStyle);
        if (styleLayerId && styleLayerId !== layerId) return [];

        const normalizedMapStyle = styleLayerId
            ? mapStyle
            : { ...mapStyle, layerId };
        return [{
            messageId: message.id,
            createdAt: message.created_at ?? new Date(0),
            mapStyle: normalizedMapStyle,
            signature: getMapStyleHistorySignature(normalizedMapStyle)
        }];
    });

    if (history.length === 0) {
        return {
            success: false,
            event: 'map_style_history',
            layerId,
            message: 'No map style history was found for this layer.'
        };
    }

    const currentSignature = getMapStyleHistorySignature(currentLayer.mapStyle);
    const matchingIndex = history.map((entry) => entry.signature).lastIndexOf(currentSignature);
    const currentIndex = matchingIndex >= 0 ? matchingIndex : history.length;
    const targetIndex = currentIndex - 1;
    const targetEntry = history[targetIndex];
    if (!targetEntry) {
        return {
            success: false,
            event: 'map_style_history',
            layerId,
            currentIndex: matchingIndex,
            historyCount: history.length,
            message: 'This layer is already at the first saved map style.'
        };
    }

    const activeStyle = getMapStyleKey(targetEntry.mapStyle);
    const mapStyleJson = JSON.stringify(targetEntry.mapStyle);
    await prisma.$executeRaw`
        UPDATE "conversation_map_layers"
        SET
            "map_style" = CAST(${mapStyleJson} AS jsonb),
            "active_style" = ${activeStyle ?? null},
            "visible" = true,
            "deleted_at" = NULL,
            "updated_at" = CURRENT_TIMESTAMP
        WHERE "conversation_id" = ${conversationId}
            AND "layer_key" = ${layerId}
    `;

    const updatedRows = await getConversationMapLayerRows(conversationId, false);
    const updatedLayer = updatedRows.find((row) => row.layerKey === layerId);

    return {
        success: true,
        persisted: true,
        event: 'map_style',
        layerId,
        messageId: targetEntry.messageId,
        restoredFromMessageCreatedAt: targetEntry.createdAt,
        historyIndex: targetIndex,
        historyCount: history.length,
        mapStyle: targetEntry.mapStyle,
        ...(updatedLayer ? { layer: toPublicConversationMapLayer(updatedLayer) } : {})
    };
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

    const conversationModel = cached
        .map((msg) => {
            try {
                const parsed = JSON.parse(msg);
                return typeof parsed.model === 'string' && parsed.model.trim()
                    ? parsed.model.trim()
                    : undefined;
            } catch {
                return undefined;
            }
        })
        .find(Boolean) ;

    const totalCount = cached.length;
    const start = totalCount - (page * limit);
    const end = totalCount -((page - 1) * limit);

    const safeStart = Math.max(0,start);
    const safeEnd = Math.max(0, end);
    
    const pageItems = cached.slice(safeStart, safeEnd);
    
    
    const messages = await Promise.all(pageItems.map(async (msg) => {
        const parsed = JSON.parse(msg);
        return {
            id:parsed.id,
            role: parsed.role,
            content: parsed.content,
            metadata: await hydrateChatAttachmentUrls(parsed.metadata),
            is_silent_retry: parsed.is_silent_retry ?? false,
            created_at: parsed.created_at ?? new Date().toISOString(),
        };
    }));
       

    return {
        data: messages,
        model: conversationModel,
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
        throw Errors.badRequest('Conversation not found or you do not have permission to access it.');
    }

    const [dbMessages, totalCount, conversationModelMessage] = await Promise.all([
        prisma.messages.findMany({
            where: { conversation_id: conversationId ,is_generate:false},
            orderBy: [{ created_at: 'desc' },{id:'desc'}], // 
            skip: skip,
            take: limit,
            select: { id: true, role: true, content: true, metadata: true, created_at: true, is_silent_retry: true }
        }),
        prisma.messages.count({
            where: { conversation_id: conversationId,is_generate: false }
        }),
        prisma.messages.findFirst({
            where: {
                conversation_id: conversationId,
                deleted_at: null,
                model: { not: null }
            },
            orderBy: [{ created_at: 'asc' }, { id: 'asc' }],
            select: { model: true }
        })
    ]);

    // reverse ให้ภายใน page เรียง เก่า→ใหม่ (บนลงล่าง)
    const messages = await Promise.all(dbMessages.reverse().map(async (message) => ({
        ...message,
        metadata: await hydrateChatAttachmentUrls(message.metadata)
    })));
    const ApiKey = await getConversationApiKey(userId, conversationId);

    return {
        data: messages,
        ...(ApiKey ? { ApiKey } : {}),
        model: conversationModelMessage?.model ,
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
  await redis.del(`chat:${conversationId}`);
  await redis.del(`guest_chat:${conversationId}`);
  
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
    try {
        const resolvedVisionModel = await getResolvedVisionModelName();
        const models = await fetchOllamaModels();
        const availableModels = models
            .filter((model) => model.name !== resolvedVisionModel)
            .map((model) => {
            const paramSize = model.details?.parameter_size;
          return {
              id: model.name, 
            name: model.name.split(':')[0].toUpperCase(), 
            size: paramSize
          };
        });

        return availableModels; 
        
    } catch (error) {
       
       
        console.error('detail Error:', error);
        
        // โยน Error 500 กลับไปให้ Route 
        throw Errors.internalServerError(); 
    }
};
