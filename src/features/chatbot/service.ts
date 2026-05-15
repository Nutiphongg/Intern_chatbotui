// src/features/chat/service.ts
import { prisma } from '../setup/prisma';
import { redis } from '../setup/redis';
import { Errors } from '../../lib/errors';
import { ChatRequestBody,EditMessageBody } from './types';
import { ulid } from 'ulid';
import { env } from '../../lib/env';
import {
    mapToolSchema,
    handleMapTool,
    buildMapStylePayload,
    checkMapAccessSchema,
    handleCheckMapAccess,
    mapOptionToolSchema,
    handleMapOptionsTool,
    buildDynamicMapOptionToolSchema,
    resolveUserMapToolConfigs,
    buildMapOptionChoiceContext
} from '../mapv2/toolsv2';
import type { Prisma } from '@prisma/client';



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
const VISION_OUTPUT_TOKENS = 192;
const VISION_REQUEST_TIMEOUT_MS = 45000;
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

let resolvedVisionModelPromise: Promise<string> | undefined;

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

const classifyMapAccessIntent = async (
    message: string,
    hasImages: boolean,
    model: string
): Promise<boolean> => {
    const trimmedMessage = message.trim();
    if (!trimmedMessage) return false;

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
                            'Classify whether the user is asking to access existing map/layer/tile data from an external map API.',
                            'Return only JSON: {"intent":"map_access"} or {"intent":"chat"}.',
                            'Use "map_access" only when the user wants to list, fetch, open, get a URL, get WMS/WMTS/TMS/vector tile, or retrieve map/layer data.',
                            'Use "chat" for general discussion, explanations, image analysis, or visual/map style advice that does not require fetching existing map data.'
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

        if (!response.ok) return false;

        const payload = await response.json() as { message?: { content?: string } };
        const content = payload.message?.content?.trim() || '';
        const jsonMatch = content.match(/\{[\s\S]*\}/);
        const parsed = JSON.parse(jsonMatch?.[0] || content) as { intent?: string };
        return parsed.intent === 'map_access';
    } catch (error) {
        console.error('[map-intent] classify failed:', error);
        return false;
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

const analyzeImagesWithVisionModel = async (
    images: ChatImageAttachment[],
    userMessage: string,
    visionModel: string
): Promise<string | undefined> => {
    if (images.length === 0) return undefined;

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
                    content: [
                        'You are a vision assistant that summarizes image content for the main chatbot.',
                        'Return a concise summary based only on visible image evidence. Do not explain your reasoning.',
                        'Match the user language: answer in Thai when the user asks in Thai, and answer in English when the user asks in English.',
                        'Include important OCR text if visible.',
                        'If the user asks about map styling, include colors, visual theme, and practical map style hints.',
                        `User question: ${userMessage || '(no text question; image only)'}`
                    ].join('\n'),
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

    const payload = await response.json() as { message?: { content?: string } };
    return payload.message?.content?.trim() || undefined;
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
    const error = asRecord(value).error;
    return typeof error === 'string' && error.trim()
        ? error
        : fallback;
};

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
    const selectedParam = key && value !== undefined && value !== null && value !== ''
        ? { [key]: value }
        : {};
    const params = {
        ...inlineParams,
        ...selectedParam,
        ...explicitParams
    };

    return {
        ...(typeof record.intentName === 'string' ? { intentName: record.intentName } : {}),
        ...(typeof record.provider === 'string' ? { provider: record.provider } : {}),
        ...(Object.keys(params).length > 0 ? { params } : {}),
        ...(Object.keys(explicitOptions).length > 0 ? { options: explicitOptions } : {}),
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
        question: typeof result.question === 'string' ? result.question : undefined,
        message: typeof result.message === 'string' ? result.message : undefined
    };
};

const buildMapOptionsFingerprint = (payload: ReturnType<typeof buildMapOptionsEvent>) => {
    return JSON.stringify({
        needInfo: payload.needInfo,
        key: payload.key,
        complete: payload.complete,
        intentName: payload.intentName,
        provider: payload.provider,
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
    const shouldPersistUserMessage = (hasUserMessage || hasImages) && !hasMapSelection;

    if (!body || (!hasUserMessage && !hasMapSelection && !hasImages)) {
        throw Errors.badRequest('no message data found');
    }

    const isGuest = role === 'guest';
    const message = rawMessage.trim() || (hasImages ? 'ช่วยดูรูปนี้ให้หน่อย' : '');
    const selectedModel = body.model?.trim() || DEFAULT_CHAT_MODEL;
    const isSilentRetry = body.is_silent_retry === true;
    const mapHeaderApiKey = apiKey?.trim() || vectorApiKey?.trim();
    const hasMapApiKey = Boolean(mapHeaderApiKey);
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
                     
                    // ส่ง heartbeat กัน proxy/ngrok ตัด connection ตอนโมเดลยังไม่ตอบ token แรก
                    if (!isClosed) {
                        heartbeat = setInterval(() => {
                            if (!isClosed) {
                                writeSse(controller, 'ping', { ts: Date.now() });
                            }
                        }, 10000);
                    }
                    const imageAttachments = rawImages.slice(0, MAX_CHAT_IMAGES).map(parseChatImage);
                    if (rawImages.length > MAX_CHAT_IMAGES) {
                        throw Errors.badRequest(`too many images; max ${MAX_CHAT_IMAGES}`);
                    }
                    let visionSummary: string | undefined;
                    let usedVisionModel = VISION_MODEL;
                    if (imageAttachments.length > 0) {
                        usedVisionModel = await getResolvedVisionModelName();
                        writeSse(controller, 'vision', {
                            status: 'analyzing',
                            model: usedVisionModel,
                            count: imageAttachments.length
                        });
                        try {
                            visionSummary = await analyzeImagesWithVisionModel(imageAttachments, message, usedVisionModel);
                            writeSse(controller, 'vision', {
                                status: 'done',
                                model: usedVisionModel
                            });
                        } catch (error) {
                            console.error('[vision] analyze failed:', error);
                            writeSse(controller, 'vision', {
                                status: 'error',
                                model: usedVisionModel,
                                message: 'vision_model_failed'
                            });
                        }
                    }
                    const imageAttachmentMetadata = await uploadChatImageAttachments(
                        imageAttachments,
                        userId,
                        convId,
                        userMessageId
                    );
                    const userMessageMetadata = imageAttachmentMetadata.length > 0
                        ? toPrismaJsonObject({
                            attachments: imageAttachmentMetadata,
                            ...(visionSummary ? { vision: { model: usedVisionModel, summary: visionSummary } } : {})
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
                    let mapToolContext: { role: string, content: string } | undefined;
                    let mapMetadata: Prisma.InputJsonObject | undefined;
                    let assistantEventMetadata: Prisma.InputJsonObject | undefined;
                    const createMapOptionsMetadata = (payload: unknown): Prisma.InputJsonObject => {
                        return toPrismaJsonObject({
                            event: 'map_options',
                            payload
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
                        writeSse(controller, 'map_style', styleResult);
                        return styleResult;
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

                    const wantsMapAccess = hasMapSelection
                        ? true
                        : hasUserMessage
                            ? await classifyMapAccessIntent(message, hasImages, selectedModel)
                            : false;
                    const shouldRequireMapApiKey = wantsMapAccess && !hasMapApiKey;
                    const shouldHandleMap = hasMapSelection || (wantsMapAccess && hasMapApiKey);

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
                        writeSse(controller, 'map_options', payload);
                        return true;
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
                            ...(Object.keys(selectedValues).length > 0 ? { params: selectedValues } : {})
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
                                    ? await saveAssistantMessage('', createMapOptionsMetadata(optionPayload))
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
                                    const mapSummary = 'นี่คือข้อมูลแผนที่ตามที่คุณต้องการครับ';
                                    const assistantMessageId = await saveAssistantMessage(
                                        mapSummary,
                                        mapMetadata
                                    );
                                    writeSse(controller, 'token', { text: mapSummary });
                                    writeSse(controller, 'done', {
                                        done: true,
                                        tokenUsage: 0,
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
                                        ? await saveAssistantMessage('', createMapOptionsMetadata(nextOptionPayload))
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
                    const visionContext = visionSummary
                        ? {
                            role: 'system',
                            content: `The user attached image(s). A vision model (${usedVisionModel}) analyzed them before this chat response. Use this analysis as image context; do not claim you directly saw pixels beyond this analysis. If the user asks to style a map from the image, convert visual colors/themes into map styling intent.\n\nVision analysis:\n${visionSummary}`
                        }
                        : undefined;

                    const messagesForOllama = [
                        systemMessage,
                        ...sanitizedMessagesForLLM,
                        styleReminder,
                        ...(visionContext ? [visionContext] : []),
                        ...(mapAccessContext ? [mapAccessContext] : []),
                        ...(mapSelectionContext ? [mapSelectionContext] : []),
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
                    if (shouldHandleMap) {
                        ollamaPayload.tools = [
                            checkMapAccessSchema,
                            dynamicMapOptionToolSchema,
                            mapToolSchema
                        ] as unknown[];
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
                                const aiArguments = parseToolArguments(toolCall?.function?.arguments ?? toolCall?.arguments);
                                const contextualArguments = buildContextualMapToolArgs(aiArguments);
                                const optionResult = await handleMapOptionsTool(userId, contextualArguments, mapHeaderApiKey);
                                const optionPayload = buildMapOptionsEvent(optionResult);
                                await persistMapSelectionState(optionPayload);
                                const wroteOptionPayload = optionPayload.complete
                                    ? false
                                    : writeMapOptionsEvent(optionPayload);

                                if (optionPayload.complete && optionPayload.intentName && optionPayload.provider) {
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
                                        mapMetadata = createMapMetadata(mapResult.payload, mapStylePayload);

                                        const mapSummary = 'นี่คือข้อมูลแผนที่ตามที่คุณต้องการครับ';
                                        assistantReply += mapSummary;
                                        writeSse(controller, 'token', { text: mapSummary });
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
                                        writeSse(controller, 'token', { text: mapErrorMessage });
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
                            const mapResult = await handleMapTool(userId, contextualArguments, mapHeaderApiKey);

                            if (mapResult.success) {
                                await clearMapSelectionState();
                                const mapStylePayload = await writeMapResultEvents(mapResult.payload);
                                mapMetadata = createMapMetadata(mapResult.payload, mapStylePayload);

                                const mapSummary = 'นี่คือข้อมูลแผนที่ตามที่คุณต้องการครับ';
                                assistantReply += mapSummary;
                                writeSse(controller, 'token', { text: mapSummary });
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
                                writeSse(controller, 'token', { text: mapErrorMessage });
                            }
                        }

                        if (chunk?.done && handledToolCalls.size === 0 && !loggedNoToolDecision) {
                            loggedNoToolDecision = true;
                            console.log("AI เลือกที่จะตอบเป็นข้อความธรรมดา (ไม่ได้เรียก Tool)");

                            if (shouldHandleMap) {
                                const contextualArguments = buildContextualMapToolArgs({});
                                const optionResult = await handleMapOptionsTool(userId, contextualArguments, mapHeaderApiKey);
                                const optionPayload = buildMapOptionsEvent(optionResult);
                                await persistMapSelectionState(optionPayload);
                                const wroteOptionPayload = optionPayload.complete
                                    ? false
                                    : writeMapOptionsEvent(optionPayload);

                                if (optionPayload.complete && optionPayload.intentName && optionPayload.provider) {
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
                                        mapMetadata = createMapMetadata(mapResult.payload, mapStylePayload);

                                        const mapSummary = 'นี่คือข้อมูลแผนที่ตามที่คุณต้องการครับ';
                                        assistantReply += mapSummary;
                                        writeSse(controller, 'token', { text: mapSummary });
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
                                        writeSse(controller, 'token', { text: mapErrorMessage });
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
                        done :
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

    const allMessages = cached
   
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
    const messages = await Promise.all(dbMessages.reverse().map(async (message) => ({
        ...message,
        metadata: await hydrateChatAttachmentUrls(message.metadata)
    })));

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
