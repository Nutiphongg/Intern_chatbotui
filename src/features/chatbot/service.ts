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
    buildMapOptionChoiceContext
} from '../map/tools';
import type { Prisma } from '@prisma/client';
import {
    retrieveConversationMemoryChunks,
    saveConversationMemoryChunks
} from './memoryChunks';



const OLLAMA_URL = env.OLLAMA_URL;
const DEFAULT_CHAT_MODEL = 'qwen2.5';
const VISION_MODEL = env.VISION_MODEL;
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
    model: string
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
                            'Use "map_control" when the user wants to manage already displayed map state without fetching provider data, such as clearing or hiding existing displayed layers.',
                            'Use "chat" for general discussion, explanations, image analysis, or visual/map style advice that does not require fetching existing provider data.'
                        ].join('\n')
                    },
                    {
                        role: 'user',
                        content: JSON.stringify({
                            message: trimmedMessage,
                            hasImages
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
        console.error('[map-intent] classify failed:', error);
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
    const content = message.content ?? record.response ?? record.content;

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

    return undefined;
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

const structureVisionAnalysisWithTextModel = async (
    visionText: string,
    userMessage: string
): Promise<VisionAnalysis | undefined> => {
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
                        'Schema: {"summary":"short visible description","dominantColors":[{"name":"color name","hex":"#RRGGBB"}]}',
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
    return structured || { summary: visionText.trim() };
};

const analyzeImagesWithVisionModel = async (
    images: ChatImageAttachment[],
    userMessage: string,
    visionModel: string
): Promise<VisionAnalysis | undefined> => {
    if (images.length === 0) return undefined;

    const requestVisionText = async (prompt: string): Promise<string | undefined> => {
        const abortController = new AbortController();
        const timeout = setTimeout(() => abortController.abort(), VISION_REQUEST_TIMEOUT_MS);

        const response = await fetch(`${OLLAMA_URL}/api/chat`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                model: visionModel,
                stream: false,
                options: {
                    temperature: 0.1,
                    top_p: 0.8,
                    num_predict: VISION_OUTPUT_TOKENS
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
        return extractOllamaTextContent(payload);
    };

    const textPrompt = [
        'Describe the attached image in one short paragraph.',
        'Mention the most visible colors by name.',
        'Mention any readable text if visible.',
        'Do not return JSON. Do not return an empty response.',
        `Question: ${userMessage || '(image only)'}`
    ].join('\n');

    const visionText = await requestVisionText(textPrompt);
    return structureVisionAnalysisWithTextModel(visionText || '', userMessage);
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
    return typeof layerId === 'string' && layerId.trim()
        ? layerId.trim()
        : fallbackLayerId;
};

const getLayerIdFromMapPayload = (mapPayload: unknown): string | undefined => {
    const payloadRecord = asRecord(mapPayload);
    const layerRecord = asRecord(payloadRecord.layer);
    const layerId = layerRecord.layerId || payloadRecord.layerId;
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
            const layerId = asRecord(metadata.layer).layerId;
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
    const latestMap = getLatestMapPayloadFromMessages(messages);
    const latestMapStyle = getLatestMapStyleFromMessages(messages);
    const conversationMapState = buildConversationMapStateFromMessages(messages);
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
    const layerId = aiArgs.layerId || asRecord(aiArgs.params).layerId || asRecord(aiArgs.options).layerId;
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
            styleRecord.sourceLayer,
            styleRecord.source_layer,
            styleRecord.title,
            styleRecord.name
        ];
    });

    return [
        layerId,
        layerRecord.layerId,
        layerRecord.title,
        layerRecord.name,
        layerRecord.sourceLayer,
        layerRecord.source_layer,
        layerRecord.geometryType,
        mapPayloadLayer.layerId,
        mapPayloadLayer.title,
        mapPayloadLayer.name,
        mapPayloadLayer.sourceLayer,
        mapPayloadLayer.source_layer,
        mapPayloadRecord.title,
        mapPayloadRecord.name,
        ...styleValues
    ]
        .map(normalizeStyleSwitchText)
        .filter((term, index, allTerms) => term.length >= 3 && allTerms.indexOf(term) === index);
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
            const matchedTerm = terms
                .filter((term) => normalizedRequest.includes(term) || term.includes(normalizedRequest))
                .sort((left, right) => right.length - left.length)[0];
            if (!matchedTerm) return undefined;

            return {
                layerState,
                score: matchedTerm.length,
                isActive: mapState.activeLayerId === layerId
            };
        })
        .filter((item): item is { layerState: ConversationMapLayerState; score: number; isActive: boolean } => Boolean(item))
        .sort((left, right) => right.score - left.score || Number(right.isActive) - Number(left.isActive));

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
            const matchedTerm = terms
                .filter((term) => normalizedRequest.includes(term) || term.includes(normalizedRequest))
                .sort((left, right) => right.length - left.length)[0];
            if (!matchedTerm) return undefined;

            return {
                layerId,
                score: matchedTerm.length,
                isActive: mapState.activeLayerId === layerId
            };
        })
        .filter((item): item is { layerId: string; score: number; isActive: boolean } => Boolean(item))
        .sort((left, right) => right.score - left.score || Number(right.isActive) - Number(left.isActive))
        .map((item) => item.layerId);
};

const selectMapLayerStateForEdit = (
    mapState: ConversationMapState | undefined,
    aiArgs: Record<string, unknown>,
    message?: string
): ConversationMapLayerState | undefined => {
    if (!mapState?.layers) return undefined;

    const requestedLayerId = getRequestedLayerIdFromToolArgs(aiArgs);
    if (requestedLayerId && mapState.layers[requestedLayerId]) {
        return mapState.layers[requestedLayerId];
    }

    const requestedLayerText = [
        message,
        getRequestedLayerTextFromToolArgs(aiArgs)
    ].filter(Boolean).join(' ');
    const requestedLayerState = selectMapLayerStateByText(mapState, requestedLayerText);
    if (requestedLayerState) {
        return requestedLayerState;
    }

    return getLatestLayerState(mapState);
};

const selectMapStyleForEdit = (
    mapState: ConversationMapState | undefined,
    aiArgs: Record<string, unknown>,
    message: string,
    fallbackMapStyle?: unknown
): unknown | undefined => {
    const layerState = selectMapLayerStateForEdit(mapState, aiArgs, message);
    if (!layerState) return fallbackMapStyle;

    const target = typeof aiArgs.target === 'string' ? aiArgs.target : undefined;
    const styleEntries = Object.entries(layerState.styles);
    const requestedStyle = styleEntries.find(([styleKey, mapStyle]) => {
        return mapStyleMatchesRequest(styleKey, mapStyle, message, target);
    });
    if (requestedStyle) return requestedStyle[1];

    if (layerState.activeStyle && layerState.styles[layerState.activeStyle]) {
        return layerState.styles[layerState.activeStyle];
    }

    return layerState.latestMapStyle || fallbackMapStyle;
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
        params.layerIds,
        params.layerId,
        options.layerIds,
        options.layerId
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

const getPrimaryColorFromPaintValue = (value: unknown): string | undefined => {
    const directColor = normalizeSuggestionColorValue(value);
    if (directColor) return directColor;

    if (!Array.isArray(value)) return undefined;

    for (let index = value.length - 1; index >= 0; index -= 1) {
        const color = normalizeSuggestionColorValue(value[index]);
        if (color) return color;
    }

    return undefined;
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

const buildMapSuggestionsPayload = (
    mapPayload: unknown,
    mapStyle: unknown,
    styleCatalog: unknown,
    _mapState?: ConversationMapState
): Record<string, unknown> | undefined => {
    const layer = getSuggestionLayerRecord(mapPayload);
    const styleRecord = asRecord(mapStyle);
    const catalogRecord = asRecord(styleCatalog);
    const geometryType = normalizeSuggestionGeometryType(layer.geometryType || styleRecord.geometryType);
    const activeStyle = toSuggestionString(styleRecord.activeStyle) || toSuggestionString(styleRecord.styleKey);
    const styles = Array.isArray(catalogRecord.styles) ? catalogRecord.styles : [];
    const colors = Array.isArray(catalogRecord.colors) ? catalogRecord.colors : [];

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
                label: 'Change layer style to ',
                value: nextStyleOption?.value || nextStyleOption?.label,
                promptTemplate: 'Change the current map layer style to {value}'
            }]
            : []),
        ...(colorOptions.length > 0 && hasEditableColorPaint(mapStyle)
            ? [{
                key: 'change_color',
                label: 'Change color style to ',
                value: nextColorOption?.value,
                promptTemplate: 'Change the current map layer primary color to {value}'
            }]
            : []),
        ...buildClearMapSuggestionItems()
    ];

    if (items.length === 0) return undefined;

    return {
        items
    };
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

const normalizeMapSelectionArgs = (selection: unknown): Record<string, unknown> | undefined => {
    const record = asRecord(selection);
    if (Object.keys(record).length === 0) return undefined;

    const explicitParams = asRecord(record.params);
    const explicitOptions = asRecord(record.options);
    const explicitVariables = asRecord(record.variables);
    const key = typeof record.key === 'string'
        ? record.key
        : typeof record.currentKey === 'string'
            ? record.currentKey
            : undefined;
    const value = record.value ?? record.selectedValue;
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
                'currentKey',
                'value',
                'selectedValue'
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
        ...explicitParams
    };
    const selectedTopLevelOptions = {
        ...(selectedIntentName ? { intentName: selectedIntentName } : {}),
        ...(selectedProvider ? { provider: selectedProvider } : {})
    };
    const selectedOptions = {
        ...explicitOptions,
        ...selectedTopLevelOptions
    };

    return {
        ...(typeof record.intentName === 'string' ? { intentName: record.intentName } : selectedIntentName ? { intentName: selectedIntentName } : {}),
        ...(typeof record.provider === 'string' ? { provider: record.provider } : selectedProvider ? { provider: selectedProvider } : {}),
        ...(Object.keys(params).length > 0 ? { params } : {}),
        ...(Object.keys(selectedOptions).length > 0 ? { selectedOptions } : {}),
        ...(Object.keys(explicitVariables).length > 0 ? { variables: explicitVariables } : {})
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
    const mapSelectionPayload = body?.mapselection;
    const hasMapSelection = Boolean(mapSelectionPayload);
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

    const isGuest = role === 'guest';
    const message = rawMessage.trim() || (hasImages ? 'ช่วยดูรูปนี้ให้หน่อย' : '');
    const selectedModel = body.model?.trim() || DEFAULT_CHAT_MODEL;
    const isSilentRetry = body.is_silent_retry === true;
    let mapHeaderApiKey = apiKey?.trim() || vectorApiKey?.trim();
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
                    const userMessageMetadata = imageAttachmentMetadata.length > 0
                        ? toPrismaJsonObject({
                            attachments: imageAttachmentMetadata,
                            vision: {
                                model: usedVisionModel,
                                status: visionAnalysis ? 'done' : visionErrorMessage ? 'error' : 'empty',
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
                    const conversationMapState = (memoryPayload.conversationMapState || buildConversationMapStateFromMessages(messagesForLLM)) as ConversationMapState | undefined;
                    const latestMapStyle = getLatestMapStyleFromState(conversationMapState)
                        || getLatestMapStyleFromMessages(messagesForLLM)
                        || memoryPayload.latestMapStyle;
                    const latestMapPayload = getLatestMapPayloadFromState(conversationMapState)
                        || getLatestMapPayloadFromMessages(messagesForLLM)
                        || memoryPayload.latestMap;
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
                    const writeMapResultEvents = async (payload: unknown) => {
                        writeSse(controller, 'map', payload);
                        const styleResult = await buildMapStylePayload(payload, {
                            instruction: hasUserMessage ? message : undefined
                        });
                        const mapStylePayload = styleResult.success ? styleResult : undefined;
                        if (mapStylePayload) {
                            writeSse(controller, 'map_style', mapStylePayload);
                        }

                        const styleCatalog = mapStylePayload ? await handleStyleCatalogTool() : undefined;
                        const suggestionsPayload = mapStylePayload && styleCatalog
                            ? buildMapSuggestionsPayload(payload, mapStylePayload, styleCatalog, conversationMapState)
                            : buildMapControlSuggestionsPayload();
                        if (suggestionsPayload) {
                            writeSse(controller, 'suggestions', suggestionsPayload);
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
                        }

                        await redis.rpush(redisKey, JSON.stringify(botMessage));
                        await redis.ltrim(redisKey, -MAX_HISTORY, -1);
                        await redis.expire(redisKey, REDIS_TTL);

                        return assistantMessageId;
                    };

                    const streamPostMapEventReply = async (
                        eventName: 'map' | 'map_style' | 'map_clear',
                        eventPayload: unknown
                    ) => {
                        const replyMessages = [
                            systemMessage,
                            styleReminder,
                            {
                                role: 'system',
                                content: [
                                    `A "${eventName}" result is already ready and visible in the map UI.`,
                                    'Reply briefly and naturally to the user in the same language as the user.',
                                    'Do not call tools.',
                                    'Do not repeat raw JSON.',
                                    'Do not mention backend events, emitted events, payloads, APIs, coordinates, bounds, or zoom levels unless the user explicitly asks.',
                                    'Do not claim that you still need to fetch or prepare the map.',
                                    eventName === 'map'
                                        ? 'Say that the requested map layer is ready/displayed. Keep it to one short sentence.'
                                        : eventName === 'map_style'
                                            ? 'Say that the requested map style has been applied. Keep it to one short sentence.'
                                            : 'Say that the requested map layer clear action has been applied. Keep it to one short sentence.',
                                    `Internal context for grounding only, not for direct quotation: ${JSON.stringify(eventPayload).slice(0, 600)}`
                                ].join('\n')
                            },
                            ...(lastMessageForLLM ? [lastMessageForLLM] : [])
                        ];

                        const response = await fetch(`${OLLAMA_URL}/api/chat`, {
                            method: 'POST',
                            headers: { 'Content-Type': 'application/json' },
                            body: JSON.stringify({
                                model: selectedModel,
                                messages: replyMessages,
                                stream: true,
                                options: {
                                    temperature: selectedFeelingKey === 'aggressive' ? 0.7 : selectedFeelingKey === 'polite' ? 0.45 : 0.5,
                                    top_p: selectedFeelingKey === 'aggressive' ? 0.9 : 0.85,
                                    num_predict: 96
                                }
                            })
                        });

                        if (!response.ok || !response.body) {
                            return { reply: '', tokenUsage: 0 };
                        }

                        const reader = response.body.getReader();
                        const decoder = new TextDecoder();
                        let buffer = '';
                        let reply = '';
                        let tokenUsage = 0;

                        while (true) {
                            const { done, value } = await reader.read();

                            if (done) {
                                const lastLine = buffer.trim();
                                if (lastLine) {
                                    const chunk = JSON.parse(lastLine);
                                    const textPart = chunk?.message?.content || '';
                                    if (textPart) {
                                        reply += textPart;
                                        writeSse(controller, 'token', { text: textPart });
                                    }
                                    if (typeof chunk?.eval_count === 'number') tokenUsage = chunk.eval_count;
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
                                    reply += textPart;
                                    writeSse(controller, 'token', { text: textPart });
                                }
                                if (typeof chunk?.eval_count === 'number') tokenUsage = chunk.eval_count;
                            }
                        }

                        try {
                            reader.releaseLock();
                        } catch {
                            // Reader may already be released after the stream ends.
                        }

                        return { reply, tokenUsage };
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
                        if (isGuest) return null;

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

                        if (!shouldUpdateExisting || isGuest) {
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

                    if (latestMapPayload && latestMapStyle && hasUserMessage && !hasMapSelection) {
                        const styleCatalog = await handleStyleCatalogTool();
                        const requestedStyleKey = styleCatalog.success && Array.isArray(styleCatalog.styles)
                            ? resolveRequestedMapStyleKey(message, styleCatalog.styles, latestMapStyle)
                            : undefined;

                        if (requestedStyleKey) {
                            const styleResult = await buildMapStylePayload(latestMapPayload, {
                                presetKey: requestedStyleKey,
                                instruction: message
                            });

                            if (styleResult.success) {
                                writeSse(controller, 'map_style', styleResult);
                                const suggestionsPayload = buildMapSuggestionsPayload(latestMapPayload, styleResult, styleCatalog, conversationMapState);
                                if (suggestionsPayload) {
                                    writeSse(controller, 'suggestions', suggestionsPayload);
                                }
                                const styleMetadata = createMapMetadata(latestMapPayload, styleResult);
                                const postReply = await streamPostMapEventReply('map_style', styleResult);
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

                    const hasActiveMapLayers = Object.keys(conversationMapState?.layers || {}).length > 0;
                    const shouldOfferMapStyleEdit = Boolean(latestMapStyle && hasUserMessage);
                    const shouldOfferMapLayerClear = Boolean(hasActiveMapLayers && hasUserMessage);
                    const mapRequestIntent: MapRequestIntent = hasMapSelection
                        ? 'map_access'
                        : hasUserMessage
                            ? await classifyMapRequestIntent(message, hasImages, selectedModel)
                            : 'chat';
                    const wantsMapAccess = mapRequestIntent === 'map_access';
                    const shouldRequireMapApiKey = wantsMapAccess && !hasMapApiKey && !shouldOfferMapStyleEdit;
                    const shouldHandleMap = hasMapSelection || (wantsMapAccess && hasMapApiKey);
                    if (shouldOfferMapStyleEdit || shouldOfferMapLayerClear) {
                        const styleCatalog = await handleStyleCatalogTool();
                        const colorKeys = styleCatalog.success && Array.isArray(styleCatalog.colors)
                            ? styleCatalog.colors.map((color) => color.key)
                            : [];
                        mapStyleContext = {
                            role: 'system',
                            content: [
                                ...(shouldOfferMapStyleEdit
                                    ? ['Latest active map_style is available. If the user asks to change map visual style, color, size, width, opacity, symbol, heatmap, or paint/layout, call edit_map_style.']
                                    : []),
                                'If the user asks to clear displayed map layers, call clear_map_layers. Use mode "selected" with layerId for one layer or layerIds for multiple layers. Use mode "all" for every displayed layer.',
                                'Do not call get_map_layer for style-only edits.',
                                'Do not call get_map_layer for map layer clear commands.',
                                ...(shouldOfferMapStyleEdit
                                    ? [
                                        'Normalize user color language into colorKeys from the style color catalog when possible. If the user asks for a mixed color, send colorKeys plus mix weights, or a valid colorValue hex.',
                                        'If the user asks to use colors from a previous image/photo, read latestVision.dominantColors from conversation memory and call edit_map_style with colorValue from the best matching dominant color hex.',
                                        'If the user names a non-active style such as circle, heatmap, fill, line, or 3d_extrusion, call edit_map_style with target/style wording so the backend can edit that saved style instead of only the latest active style.',
                                        `Available colorKeys: ${JSON.stringify(colorKeys)}`,
                                        `Latest vision memory: ${JSON.stringify(memoryPayload.latestVision || null)}`,
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

                            if (!savedMapSelectionArgs && isMapOptionPaginationAction && !isGuest) {
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
                        return mergeMapToolArgs(savedMapSelectionArgs, mapSelectionArgs, inferredMapArgs, latestQueryArgs, aiArguments);
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
                        inferredMapArgs = hasUserMessage && mapChoiceContext.length > 0
                            ? inferMapArgsFromDbChoiceText(message, mapChoiceContext)
                            : undefined;
                        dynamicMapOptionToolSchema = mapConfigs.length > 0
                            ? buildDynamicMapOptionToolSchema(mapConfigs)
                            : mapOptionToolSchema;
                        mapAccessContext = {
                            role: 'system',
                            content: `Map access context for this user. Use this only when the user asks for map/layer data. Pick provider and intentName from these configs and never invent access outside this list: ${JSON.stringify(mapAccessResult)}
If the user asks for map/layer data and there is no complete mapSelection yet, do not ask a normal text follow-up first. Call the map_options tool immediately so the backend can return DB/API-backed choices.
DB-backed map choice context for semantic matching: ${JSON.stringify(mapChoiceContext)}
Inferred params already extracted from the latest user message by the map inference pass: ${JSON.stringify(inferredMapArgs || {})}
For VALLARIS, always include the latest user message in query/message when calling map_options or get_map_layer. Pick the intentName by matching the user request with each config's intentName, type, handler, itemType, and optionKey from the DB-backed context. If the config handler is collection_detail or has an itemType such as Tile/CoverageTile, call map_options for layerId choices from the collection endpoint. If the config is a style catalog, the backend will match styleId and ask for map type links. Never expose provider API keys in map_options choices.
Infer params from the user's wording and the DB-backed enum descriptions in the map_options tool schema, including natural day/date wording into the matching dayPath choice value. Include inferred values in map_options.params. Do not call map_options with empty params when the user's wording already matches a choice. If the user already selected values in mapSelection, keep those values and continue with the next missing option.
For URL/template placeholders, ask the user using only the DB-backed map_options choices. When hazard/dayPath/type or other required placeholders are complete, call get_map_layer with params.`
                        };

                        if (hasMapSelection) {
                            const contextualArguments = buildContextualMapToolArgs({});
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
                    const mapSelectionContext = mapSelectionPayload
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
                        ...sanitizedMessagesForLLM,
                        styleReminder,
                        ...(conversationMemoryContext ? [conversationMemoryContext] : []),
                        ...(retrievedMemoryContext ? [retrievedMemoryContext] : []),
                        ...(visionContext ? [visionContext] : []),
                        ...(mapAccessContext ? [mapAccessContext] : []),
                        ...(mapSelectionContext ? [mapSelectionContext] : []),
                        ...(mapToolContext ? [mapToolContext] : []),
                        ...(mapStyleContext ? [mapStyleContext] : []),
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

                    // อ่าน stream จาก Ollama แล้วแปลงเป็น SSE token ส่งต่อให้ frontend
                    let assistantReply = '';
                    let tokenUsage = 0;
                    const decoder = new TextDecoder();
                    const reader = ollamaResponse.body.getReader();
                    ollamaReader = reader;
                    let buffer = '';
                    const handledToolCalls = new Set<string>();
                    let loggedNoToolDecision = false;

                    const handleOllamaChunk = async (chunk: any) => {
                        const toolCalls = Array.isArray(chunk?.message?.tool_calls)
                            ? chunk.message.tool_calls
                            : [];
                        const textPart = chunk?.message?.content || '';
                        if (textPart && !shouldHandleMap) {
                            assistantReply += textPart;
                            writeSse(controller, 'token', { text: textPart });
                        }

                        if (toolCalls.length > 0) {
                            console.log("=== สิ่งที่ AI คิดและตอบกลับมา ===");
                            console.log(JSON.stringify(toolCalls, null, 2));
                        }

                        for (const [index, toolCall] of toolCalls.entries()) {
                            const toolName = toolCall?.function?.name || toolCall?.name;
                            const toolCallKey = toolCall?.id || `${toolName}:${index}:${JSON.stringify(toolCall?.function?.arguments ?? toolCall?.arguments ?? {})}`;
                            if (handledToolCalls.has(toolCallKey)) continue;
                            handledToolCalls.add(toolCallKey);

                            console.log(`AI ตัดสินใจเรียก Tool ชื่อ: ${toolName}`);
                            console.log('พร้อมกับแนบข้อมูลมาให้คือ:', toolCall?.function?.arguments ?? toolCall?.arguments);

                            if (toolName === 'clear_map_layers') {
                                const aiArguments = parseToolArguments(toolCall?.function?.arguments ?? toolCall?.arguments);
                                const controlResult = await handleClearMapLayersTool(
                                    userId,
                                    convId,
                                    buildClearMapLayerArgs(aiArguments, message, conversationMapState)
                                );

                                if (controlResult.success) {
                                    writeSse(controller, 'map_clear', controlResult);
                                    mapMetadata = toPrismaJsonObject(controlResult);

                                    const postReply = await streamPostMapEventReply('map_clear', controlResult);
                                    assistantReply += postReply.reply;
                                    tokenUsage += postReply.tokenUsage;
                                } else {
                                    const controlErrorMessage = getToolErrorMessage(controlResult, 'ไม่สามารถจัดการ layer แผนที่ได้ครับ');
                                    writeSse(controller, 'map_error', { message: controlErrorMessage });
                                    assistantReply += controlErrorMessage;
                                }
                                continue;
                            }

                            if (toolName === 'edit_map_style') {
                                const aiArguments = parseToolArguments(toolCall?.function?.arguments ?? toolCall?.arguments);
                                const selectedMapStyle = selectMapStyleForEdit(conversationMapState, aiArguments, message, latestMapStyle);
                                const selectedMapPayload = selectMapPayloadForEdit(conversationMapState, aiArguments, latestMapPayload, message);
                                const editResult = await handleEditMapStyleTool(
                                    {
                                        ...aiArguments,
                                        instruction: typeof aiArguments.instruction === 'string' ? aiArguments.instruction : message
                                    },
                                    selectedMapStyle
                                );

                                if (editResult.success) {
                                    writeSse(controller, 'map_style', editResult);
                                    const styleCatalog = await handleStyleCatalogTool();
                                    const suggestionsPayload = buildMapSuggestionsPayload(selectedMapPayload, editResult, styleCatalog, conversationMapState);
                                    if (suggestionsPayload) {
                                        writeSse(controller, 'suggestions', suggestionsPayload);
                                    }
                                    mapMetadata = toPrismaJsonObject({
                                        ...(asRecord(selectedMapPayload)),
                                        event: asRecord(selectedMapPayload).event || 'map_style',
                                        mapStyle: editResult
                                    });

                                    const postReply = await streamPostMapEventReply('map_style', editResult);
                                    assistantReply += postReply.reply;
                                    tokenUsage += postReply.tokenUsage;
                                } else {
                                    const styleErrorMessage = getToolErrorMessage(editResult, 'ไม่สามารถแก้ style แผนที่ได้ครับ');
                                    writeSse(controller, 'map_error', { message: styleErrorMessage });
                                    assistantReply += styleErrorMessage;
                                    writeSse(controller, 'token', { text: styleErrorMessage });
                                }
                                continue;
                            }

                            if (toolName === 'check_user_map') {
                                const accessResult = await handleCheckMapAccess(userId, mapHeaderApiKey, message);
                                if (!sentMapAccessEvent) {
                                    writeSse(controller, 'map_access', accessResult);
                                    sentMapAccessEvent = true;
                                }

                                if (!accessResult.success) {
                                    const accessErrorMessage = accessResult.message || 'ไม่พบสิทธิ์การใช้งานแผนที่ครับ';
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
                                    contextualArguments = buildContextualMapToolArgs(aiArguments);
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
                                        const mapErrorMessage = getToolErrorMessage(mapResult, 'ไม่สามารถดึงข้อมูลแผนที่ได้ครับ');
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
                            const contextualArguments = buildContextualMapToolArgs(aiArguments);
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
                                const mapErrorMessage = getToolErrorMessage(mapResult, 'ไม่สามารถดึงข้อมูลแผนที่ได้ครับ');
                                writeSse(controller, 'map_error', { message: mapErrorMessage });
                                assistantReply += mapErrorMessage;
                            }
                        }

                        if (chunk?.done && handledToolCalls.size === 0 && !loggedNoToolDecision) {
                            loggedNoToolDecision = true;
                            console.log("AI เลือกที่จะตอบเป็นข้อความธรรมดา (ไม่ได้เรียก Tool)");

                            if (shouldHandleMap) {
                                let contextualArguments: Record<string, unknown>;
                                let optionPayload: ReturnType<typeof buildMapOptionsEvent>;
                                let wroteOptionPayload = false;
                                try {
                                    contextualArguments = buildContextualMapToolArgs({});
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
                                        const mapErrorMessage = getToolErrorMessage(mapResult, 'ไม่สามารถดึงข้อมูลแผนที่ได้ครับ');
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
        throw Errors.badRequest('ไม่พบห้องแชท หรือคุณไม่มีสิทธิ์เข้าถึงข้อมูลนี้');
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
       
       
        console.error('รายละเอียด Error:', error);
        
        // โยน Error 500 กลับไปให้ Route 
        throw Errors.internalServerError(); 
    }
};
