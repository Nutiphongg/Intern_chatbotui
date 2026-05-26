import { createHash } from "node:crypto";
import { ulid } from "ulid";
import { prisma } from "../setup/prisma";
import { env } from "../../lib/env";

type MemoryChunkMessage = {
  id: string;
  conversation_id: string;
  role: string;
  content: string;
  metadata?: unknown;
  created_at?: Date | string | null;
};

type MemoryChunkInput = {
  userId: string;
  message: MemoryChunkMessage;
};

export type RetrievedMemoryChunk = {
  id: string;
  messageId: string | null;
  chunkType: string;
  role: string | null;
  content: string;
  eventType: string | null;
  layerId: string | null;
  styleKey: string | null;
  geometryType: string | null;
  similarity?: number | null;
};

type BuiltMemoryChunk = {
  sourceKey: string;
  chunkType: string;
  role?: string;
  content: string;
  eventType?: string;
  layerId?: string;
  styleKey?: string;
  geometryType?: string;
  metadata?: unknown;
};

const MEMORY_CHUNK_CONTENT_LIMIT = 2000;
const MEMORY_EMBEDDING_MODEL = env.MEMORY_EMBEDDING_MODEL.trim();
const MEMORY_RAG_TOP_K = Math.max(1, Number(env.MEMORY_RAG_TOP_K) || 6);
const MEMORY_RAG_MATCH_THRESHOLD = Math.max(0, Math.min(1, Number(env.MEMORY_RAG_MATCH_THRESHOLD) || 0.2));

const isRecord = (value: unknown): value is Record<string, unknown> => {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
};

const toStringValue = (value: unknown): string | undefined => {
  return typeof value === "string" && value.trim() ? value.trim() : undefined;
};

const truncateContent = (value: string): string => {
  return value.length > MEMORY_CHUNK_CONTENT_LIMIT
    ? `${value.slice(0, MEMORY_CHUNK_CONTENT_LIMIT)}...`
    : value;
};

const createSourceHash = (value: string): string => {
  return createHash("sha256").update(value).digest("hex");
};

const getLayerId = (record: Record<string, unknown>): string | undefined => {
  return toStringValue(record.layerId)
    || toStringValue(record.id)
    || toStringValue(isRecord(record.layer) ? record.layer.layerId : undefined);
};

const getStyleKey = (record: Record<string, unknown>): string | undefined => {
  return toStringValue(record.styleKey)
    || toStringValue(record.activeStyle)
    || toStringValue(record.preset);
};

const getGeometryType = (record: Record<string, unknown>): string | undefined => {
  return toStringValue(record.geometryType)
    || toStringValue(isRecord(record.layer) ? record.layer.geometryType : undefined);
};

const getMapStyleLayerTypes = (mapStyle: Record<string, unknown>): string[] => {
  const layers = Array.isArray(mapStyle.layers) ? mapStyle.layers : [];
  return layers
    .map((layer) => isRecord(layer) ? toStringValue(layer.type) : undefined)
    .filter((value): value is string => Boolean(value));
};

const summarizeMapStyle = (mapStyle: Record<string, unknown>): string => {
  const layerId = getLayerId(mapStyle);
  const styleKey = getStyleKey(mapStyle);
  const geometryType = getGeometryType(mapStyle);
  const layerTypes = getMapStyleLayerTypes(mapStyle);

  return [
    "Map style memory.",
    layerId ? `layerId=${layerId}.` : undefined,
    styleKey ? `styleKey=${styleKey}.` : undefined,
    geometryType ? `geometryType=${geometryType}.` : undefined,
    layerTypes.length > 0 ? `mapLibreLayerTypes=${layerTypes.join(", ")}.` : undefined
  ].filter(Boolean).join(" ");
};

const summarizeLayerCatalog = (metadata: Record<string, unknown>): string => {
  const layer = isRecord(metadata.layer) ? metadata.layer : metadata;
  const title = toStringValue(layer.title) || toStringValue(layer.name);
  const layerId = getLayerId(layer);
  const geometryType = getGeometryType(layer);
  const sourceLayer = toStringValue(layer.sourceLayer);
  const type = toStringValue(layer.type);

  return [
    "Map layer memory.",
    title ? `title=${title}.` : undefined,
    layerId ? `layerId=${layerId}.` : undefined,
    type ? `type=${type}.` : undefined,
    geometryType ? `geometryType=${geometryType}.` : undefined,
    sourceLayer ? `sourceLayer=${sourceLayer}.` : undefined
  ].filter(Boolean).join(" ");
};

const summarizeVision = (vision: Record<string, unknown>): string => {
  const summary = toStringValue(vision.summary);
  const dominantColors = Array.isArray(vision.dominantColors) ? vision.dominantColors : [];
  const colorNames = dominantColors
    .map((color) => isRecord(color) ? toStringValue(color.name) : undefined)
    .filter((value): value is string => Boolean(value));

  return [
    "Vision memory.",
    summary,
    colorNames.length > 0 ? `dominantColors=${colorNames.join(", ")}.` : undefined
  ].filter(Boolean).join(" ");
};

const buildMemoryChunks = (message: MemoryChunkMessage): BuiltMemoryChunk[] => {
  const chunks: BuiltMemoryChunk[] = [];
  const content = message.content?.trim();

  if (content) {
    chunks.push({
      sourceKey: `${message.id}:message`,
      chunkType: "message",
      role: message.role,
      content: truncateContent(content)
    });
  }

  const metadata = isRecord(message.metadata) ? message.metadata : {};
  const event = toStringValue(metadata.event);

  if (event === "layer_catalog" && metadata.layer) {
    const layer = isRecord(metadata.layer) ? metadata.layer : {};
    chunks.push({
      sourceKey: `${message.id}:event:layer_catalog:${getLayerId(layer) || ""}`,
      chunkType: "event",
      content: summarizeLayerCatalog(metadata),
      eventType: "layer_catalog",
      layerId: getLayerId(layer),
      geometryType: getGeometryType(layer),
      metadata: {
        event: "layer_catalog",
        layerId: getLayerId(layer)
      }
    });
  }

  const mapStyle = isRecord(metadata.mapStyle)
    ? metadata.mapStyle
    : event === "map_style"
      ? metadata
      : undefined;
  if (mapStyle) {
    chunks.push({
      sourceKey: `${message.id}:event:map_style:${getLayerId(mapStyle) || ""}:${getStyleKey(mapStyle) || ""}`,
      chunkType: "event",
      content: summarizeMapStyle(mapStyle),
      eventType: "map_style",
      layerId: getLayerId(mapStyle),
      styleKey: getStyleKey(mapStyle),
      geometryType: getGeometryType(mapStyle),
      metadata: {
        event: "map_style",
        layerId: getLayerId(mapStyle),
        styleKey: getStyleKey(mapStyle)
      }
    });
  }

  const vision = isRecord(metadata.vision) ? metadata.vision : undefined;
  if (vision) {
    chunks.push({
      sourceKey: `${message.id}:event:vision`,
      chunkType: "event",
      content: truncateContent(summarizeVision(vision)),
      eventType: "vision",
      metadata: {
        event: "vision"
      }
    });
  }

  if (event === "map_clear") {
    chunks.push({
      sourceKey: `${message.id}:event:map_clear`,
      chunkType: "event",
      content: `Map clear memory. mode=${toStringValue(metadata.mode) || "unknown"}. layerId=${getLayerId(metadata) || "all"}.`,
      eventType: "map_clear",
      layerId: getLayerId(metadata),
      metadata: {
        event: "map_clear",
        mode: metadata.mode
      }
    });
  }

  if (event === "map_options") {
    const payload = isRecord(metadata.payload) ? metadata.payload : metadata;
    const key = toStringValue(payload.key);
    const intentName = toStringValue(payload.intentName);
    const provider = toStringValue(payload.provider);
    chunks.push({
      sourceKey: `${message.id}:event:map_options:${key || ""}:${intentName || ""}`,
      chunkType: "event",
      content: [
        "Map options memory.",
        key ? `key=${key}.` : undefined,
        intentName ? `intentName=${intentName}.` : undefined,
        provider ? `provider=${provider}.` : undefined
      ].filter(Boolean).join(" "),
      eventType: "map_options",
      metadata: {
        event: "map_options",
        key,
        intentName,
        provider
      }
    });
  }

  return chunks.filter((chunk) => chunk.content.trim().length > 0);
};

const fetchEmbedding = async (content: string): Promise<number[] | undefined> => {
  if (!MEMORY_EMBEDDING_MODEL) return undefined;

  const parseEmbedding = (payload: unknown): number[] | undefined => {
    const record = isRecord(payload) ? payload : {};
    const embedding = Array.isArray(record.embedding)
      ? record.embedding
      : Array.isArray(record.embeddings) && Array.isArray(record.embeddings[0])
        ? record.embeddings[0]
        : undefined;
    const values = embedding?.filter((value): value is number => typeof value === "number" && Number.isFinite(value));
    return values && values.length > 0 ? values : undefined;
  };

  const requestBody = JSON.stringify({
    model: MEMORY_EMBEDDING_MODEL,
    prompt: content,
    input: content
  });

  for (const path of ["/api/embeddings", "/api/embed"]) {
    try {
      const response = await fetch(`${env.OLLAMA_URL}${path}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: requestBody
      });
      if (!response.ok) continue;

      const embedding = parseEmbedding(await response.json());
      if (embedding) return embedding;
    } catch {
      // Embeddings are an optimization; storing text chunks without vectors is still useful.
    }
  }

  return undefined;
};

const toVectorLiteral = (embedding: number[]): string => {
  return `[${embedding.join(",")}]`;
};

const updateChunkEmbedding = async (sourceHash: string, embedding: number[]): Promise<void> => {
  try {
    await prisma.$executeRawUnsafe(
      `UPDATE "conversation_memory_chunks" SET "embedding" = $1::vector, "updated_at" = now() WHERE "source_hash" = $2`,
      toVectorLiteral(embedding),
      sourceHash
    );
  } catch (error) {
    console.warn("[memory-rag] embedding update skipped:", error instanceof Error ? error.message : error);
  }
};

export const saveConversationMemoryChunks = async ({
  userId,
  message
}: MemoryChunkInput): Promise<void> => {
  const chunks = buildMemoryChunks(message);
  if (chunks.length === 0) return;

  for (const chunk of chunks) {
    const sourceHash = createSourceHash(chunk.sourceKey);
    try {
      await prisma.$executeRaw`
        INSERT INTO "conversation_memory_chunks" (
          "id",
          "conversation_id",
          "user_id",
          "message_id",
          "source_hash",
          "chunk_type",
          "role",
          "content",
          "event_type",
          "layer_id",
          "style_key",
          "geometry_type",
          "metadata"
        )
        VALUES (
          ${ulid()},
          ${message.conversation_id},
          ${userId},
          ${message.id},
          ${sourceHash},
          ${chunk.chunkType},
          ${chunk.role || null},
          ${chunk.content},
          ${chunk.eventType || null},
          ${chunk.layerId || null},
          ${chunk.styleKey || null},
          ${chunk.geometryType || null},
          ${chunk.metadata ? JSON.stringify(chunk.metadata) : null}::jsonb
        )
        ON CONFLICT ("source_hash")
        DO UPDATE SET
          "content" = EXCLUDED."content",
          "event_type" = EXCLUDED."event_type",
          "layer_id" = EXCLUDED."layer_id",
          "style_key" = EXCLUDED."style_key",
          "geometry_type" = EXCLUDED."geometry_type",
          "metadata" = EXCLUDED."metadata",
          "updated_at" = now()
      `;

      const embedding = await fetchEmbedding(chunk.content);
      if (embedding) {
        await updateChunkEmbedding(sourceHash, embedding);
      }
    } catch (error) {
      console.warn("[memory-rag] chunk save skipped:", error instanceof Error ? error.message : error);
    }
  }
};

const tokenizeQuery = (query: string): string[] => {
  return Array.from(new Set(
    query
      .toLowerCase()
      .split(/[^\p{L}\p{N}_-]+/u)
      .map((token) => token.trim())
      .filter((token) => token.length >= 3)
      .slice(0, 8)
  ));
};

const retrieveByKeyword = async (
  userId: string,
  conversationId: string,
  query: string,
  limit: number
): Promise<RetrievedMemoryChunk[]> => {
  const tokens = tokenizeQuery(query);
  if (tokens.length === 0) return [];

  const clauses = tokens.map((_, index) => `"content" ILIKE $${index + 4}`).join(" OR ");
  const params = [
    userId,
    conversationId,
    limit,
    ...tokens.map((token) => `%${token}%`)
  ];

  try {
    return await prisma.$queryRawUnsafe<RetrievedMemoryChunk[]>(
      `
      SELECT
        "id",
        "message_id" AS "messageId",
        "chunk_type" AS "chunkType",
        "role",
        "content",
        "event_type" AS "eventType",
        "layer_id" AS "layerId",
        "style_key" AS "styleKey",
        "geometry_type" AS "geometryType",
        NULL::float AS "similarity"
      FROM "conversation_memory_chunks"
      WHERE "user_id" = $1
        AND "conversation_id" = $2
        AND (${clauses})
      ORDER BY "created_at" DESC
      LIMIT $3
      `,
      ...params
    );
  } catch (error) {
    console.warn("[memory-rag] keyword retrieval skipped:", error instanceof Error ? error.message : error);
    return [];
  }
};

export const retrieveConversationMemoryChunks = async (
  userId: string,
  conversationId: string,
  query: string,
  limit = MEMORY_RAG_TOP_K
): Promise<RetrievedMemoryChunk[]> => {
  const embedding = await fetchEmbedding(query);
  if (embedding) {
    try {
      const chunks = await prisma.$queryRawUnsafe<RetrievedMemoryChunk[]>(
        `
        SELECT
          "id",
          "message_id" AS "messageId",
          "chunk_type" AS "chunkType",
          "role",
          "content",
          "event_type" AS "eventType",
          "layer_id" AS "layerId",
          "style_key" AS "styleKey",
          "geometry_type" AS "geometryType",
          (1 - ("embedding" <=> $3::vector))::float AS "similarity"
        FROM "conversation_memory_chunks"
        WHERE "user_id" = $1
          AND "conversation_id" = $2
          AND "embedding" IS NOT NULL
          AND (1 - ("embedding" <=> $3::vector)) >= $5
        ORDER BY "embedding" <=> $3::vector
        LIMIT $4
        `,
        userId,
        conversationId,
        toVectorLiteral(embedding),
        limit,
        MEMORY_RAG_MATCH_THRESHOLD
      );

      if (chunks.length > 0) return chunks;
    } catch (error) {
      console.warn("[memory-rag] vector retrieval skipped:", error instanceof Error ? error.message : error);
    }
  }

  return retrieveByKeyword(userId, conversationId, query, limit);
};
