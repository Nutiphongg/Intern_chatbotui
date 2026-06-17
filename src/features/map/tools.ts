import { prisma } from "../setup/prisma";
import { decrypt, hashApiKey } from "../setup/encryption";
import { env } from "../../lib/env";
import { createExpression } from "@maplibre/maplibre-gl-style-spec";
import type {
  MapToolArgs,
  MapOptionInfo,
  MapOptionChoice,
  MapConfigForTools,
  EditMapStyleArgs, 
  EditMapStyleOperation
} from "./type";


type ResolvedUserApiKey = {
  id: string;
  provider: string;
  keyName: string;
  encryptedKey: string;
  iv: string;
  hostId?: string | null;
  host?: {
    id: string;
    provider: string;
    hostname: string;
    baseUrl: string;
    serviceConfig?: unknown;
  } | null;
};

type RuntimeMapConfig<T extends { baseUrl?: string | null; provider: string }> = T & { baseUrl: string };

type StyleCatalogEntry = {
  key: string;
  description?: string;
  layerType?: string;
  styleKey?: string;
  styleName?: string;
  isDefaultStyle?: boolean;
  geometryTypes?: string[];
  renderConfig?: unknown;
};

type StyleColorEntry = {
  key: string;
  value: string;
  role?: string;
  family?: string;
  description?: string;
};

type StylePropertyKind = "paint" | "layout";

let mapLibreStyleSpecCache: Record<string, unknown> | null | undefined;

type ClearMapLayersArgs = {
  mode?: string;
  layerId?: string;
  layerIds?: unknown;
};

type ActiveMapLayer = {
  layerId: string;
  title?: string;
  sourceLayer?: string;
};

let styleCatalogCache: {
  expiresAt: number;
  sourceUrl: string;
  entries: StyleCatalogEntry[];
  colors: StyleColorEntry[];
} | undefined;

const isRecord = (value: unknown): value is Record<string, unknown> => {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
};

const normalizeProvider = (provider?: string): string => {
  return provider?.trim().toUpperCase() || "";
};

const isVallarisProvider = (provider?: string): boolean => {
  return normalizeProvider(provider).includes("VALLARIS");
};

const sameProvider = (left?: string, right?: string): boolean => {
  const normalizedLeft = normalizeProvider(left);
  const normalizedRight = normalizeProvider(right);

  if (!normalizedLeft || !normalizedRight) return false;
  return normalizedLeft === normalizedRight;
};

const STYLE_CATALOG_CACHE_TTL_MS = 5 * 60 * 1000;

const getUniqueProviders = (providers: string[]): string[] => {
  const providersByKey = new Map<string, string>();
  for (const provider of providers) {
    const cleanProvider = provider.trim();
    const normalizedProvider = normalizeProvider(cleanProvider);
    if (!normalizedProvider || providersByKey.has(normalizedProvider)) continue;
    providersByKey.set(normalizedProvider, cleanProvider);
  }

  return Array.from(providersByKey.values());
};

const providerAllowed = (allowedProviders: string[], provider?: string): boolean => {
  return allowedProviders.some((allowedProvider) => sameProvider(allowedProvider, provider));
};

const getConfigType = (layerConfigTemplate: unknown): string | undefined => {
  return toCleanString(pickRecord(layerConfigTemplate).type) || undefined;
};

const withApiKeyHostBaseUrl = <
  T extends { baseUrl?: string | null; provider: string }
>(
  config: T,
  apiKey?: ResolvedUserApiKey
): RuntimeMapConfig<T> => {
  const hostBaseUrl = apiKey?.host?.baseUrl?.trim();
  const legacyBaseUrl = typeof config.baseUrl === "string" ? config.baseUrl.trim() : "";
  const baseUrl = hostBaseUrl || legacyBaseUrl;
  if (!baseUrl) {
    throw new Error(`No map host baseUrl is configured for provider ${config.provider}.`);
  }

  return {
    ...config,
    baseUrl
  };
};

const getAllowedHosts = (apiKeys: ResolvedUserApiKey[]) => {
  const hostsByKey = new Map<string, {
    providerKey: string;
    hostKey: string;
    baseUrl: string;
  }>();

  for (const apiKey of apiKeys) {
    const host = apiKey.host;
    if (!host?.baseUrl || !host.hostname) continue;

    const providerKey = apiKey.provider || host.provider;
    const mapKey = `${normalizeProvider(providerKey)}:${host.hostname}`;
    if (hostsByKey.has(mapKey)) continue;

    hostsByKey.set(mapKey, {
      providerKey,
      hostKey: host.hostname,
      baseUrl: host.baseUrl
    });
  }

  return Array.from(hostsByKey.values());
};

const selectApiKeyForProvider = (
  apiKeys: ResolvedUserApiKey[],
  provider: string
): ResolvedUserApiKey | undefined => {
  const providerKeys = apiKeys.filter((apiKey) => sameProvider(apiKey.provider, provider));
  return providerKeys.find((apiKey) => Boolean(apiKey.host?.baseUrl?.trim()))
    || providerKeys[0];
};

const matchesConfigString = (actual: unknown, expected: unknown): boolean => {
  const cleanExpected = toStringValue(expected);
  if (!cleanExpected) return true;
  const cleanActual = toStringValue(actual);
  return Boolean(cleanActual && cleanActual.toLowerCase() === cleanExpected.toLowerCase());
};

const selectApiKeyForConfig = (
  apiKeys: ResolvedUserApiKey[],
  fallbackProvider: string,
  config: Record<string, unknown>
): ResolvedUserApiKey | undefined => {
  const provider = toStringValue(config.apiKeyProvider || config.provider) || fallbackProvider;
  const keyName = config.apiKeyName || config.keyName;
  const hostKey = config.hostKey || config.hostname || config.hostName;
  const hostId = config.hostId;

  const candidates = apiKeys.filter((apiKey) => sameProvider(apiKey.provider, provider));
  const matched = candidates.find((apiKey) => {
    return matchesConfigString(apiKey.keyName, keyName)
      && matchesConfigString(apiKey.host?.hostname, hostKey)
      && matchesConfigString(apiKey.hostId || apiKey.host?.id, hostId);
  });

  return matched || selectApiKeyForProvider(apiKeys, provider);
};

const getHostServiceApiKey = (
  host: ResolvedUserApiKey["host"],
  keyName: string
): string | undefined => {
  const apiKeys = pickRecord(pickRecord(host?.serviceConfig).apiKeys);
  const keyRecord = pickRecord(apiKeys[keyName]);
  const encryptedKey = toStringValue(keyRecord.encryptedKey);
  const iv = toStringValue(keyRecord.iv);
  if (!encryptedKey || !iv) return undefined;

  return decrypt(encryptedKey, iv);
};

const pickRecord = (value: unknown): Record<string, unknown> => {
  if (isRecord(value)) return value;

  if (typeof value === "string" && value.trim().startsWith("{")) {
    try {
      const parsed = JSON.parse(value);
      return isRecord(parsed) ? parsed : {};
    } catch {
      return {};
    }
  }

  return {};
};

const toRawGithubUrl = (url: string): string => {
  const trimmedUrl = url.trim();
  const match = trimmedUrl.match(/^https:\/\/github\.com\/([^/]+)\/([^/]+)\/blob\/([^/]+)\/(.+)$/);
  if (!match) return trimmedUrl;

  const [, owner, repo, branch, path] = match;
  return `https://raw.githubusercontent.com/${owner}/${repo}/${branch}/${path}`;
};

const inferGeometryTypesFromStyleGroupKey = (key: string): string[] => {
  const normalized = key.toLowerCase();
  const geometryTypes = [
    normalized.includes("point") ? "point" : undefined,
    normalized.includes("line") ? "line" : undefined,
    normalized.includes("polygon") ? "polygon" : undefined,
    normalized.includes("raster") ? "raster" : undefined
  ].filter((item): item is string => Boolean(item));

  return Array.from(new Set(geometryTypes));
};

const getGeometryTypesFromRecord = (
  record: Record<string, unknown>,
  fallbackKey?: string
): string[] | undefined => {
  const explicitGeometryTypes = Array.isArray(record.geometryTypes)
    ? record.geometryTypes.map(normalizeGeometryType).filter((item): item is string => Boolean(item))
    : [];
  if (explicitGeometryTypes.length > 0) return explicitGeometryTypes;

  const inferredGeometryTypes = fallbackKey ? inferGeometryTypesFromStyleGroupKey(fallbackKey) : [];
  return inferredGeometryTypes.length > 0 ? inferredGeometryTypes : undefined;
};

const parseStyleCatalogMetadata = (block: string): Record<string, string> => {
  const metadata: Record<string, string> = {};
  for (const line of block.split(/\r?\n/)) {
    const match = line.match(/^\s*([A-Za-z][\w-]*)\s*:\s*(.+?)\s*$/);
    if (!match) continue;

    metadata[match[1]] = match[2];
  }

  return metadata;
};

const extractJsonObjectFromText = (text: string, startIndex = 0): unknown => {
  const objectStart = text.indexOf("{", startIndex);
  if (objectStart < 0) return undefined;

  let depth = 0;
  let inString = false;
  let escaped = false;

  for (let index = objectStart; index < text.length; index++) {
    const char = text[index];

    if (inString) {
      if (escaped) {
        escaped = false;
      } else if (char === "\\") {
        escaped = true;
      } else if (char === "\"") {
        inString = false;
      }
      continue;
    }

    if (char === "\"") {
      inString = true;
      continue;
    }

    if (char === "{") depth += 1;
    if (char === "}") depth -= 1;

    if (depth === 0) {
      try {
        return JSON.parse(text.slice(objectStart, index + 1));
      } catch {
        return undefined;
      }
    }
  }

  return undefined;
};

const parseStyleCatalogTextColors = (text: string): StyleColorEntry[] => {
  const colors: StyleColorEntry[] = [];
  const colorPattern = /^\s*-\s*([A-Za-z][\w-]*)\s*:\s*(#[0-9A-Fa-f]{3,8})\s*$/gm;

  for (const match of text.matchAll(colorPattern)) {
    colors.push({
      key: match[1],
      value: match[2].toUpperCase(),
      role: "base"
    });
  }

  return colors;
};

const parseStyleCatalogTextEntries = (text: string): StyleCatalogEntry[] => {
  const entries: StyleCatalogEntry[] = [];
  const sectionPattern = /---\s*([\s\S]*?)\s*---\s*([\s\S]*?)(?=\n---|$)/g;

  for (const sectionMatch of text.matchAll(sectionPattern)) {
    const metadata = parseStyleCatalogMetadata(sectionMatch[1]);
    const groupKey = metadata.title?.trim();
    if (!groupKey) continue;

    const groupDescription = metadata.description?.trim();
    const defaultStyle = metadata.defaultStyle?.trim();
    const groupGeometryTypes = getGeometryTypesFromRecord({}, groupKey);
    const body = sectionMatch[2];
    const stylePattern = /###\s*Style Key:\s*([^\r\n]+)([\s\S]*?)(?=\n###\s*Style Key:|\n---|$)/g;

    for (const styleMatch of body.matchAll(stylePattern)) {
      const styleKey = styleMatch[1].trim();
      const styleBlock = styleMatch[2];
      const styleName = styleBlock.match(/\*\*Style Name:\*\*\s*([^\r\n]+)/)?.[1]?.trim();
      const layerType = styleBlock.match(/\*\*Layer Type:\*\*\s*([^\r\n]+)/)?.[1]?.trim();
      const paintLabelIndex = styleBlock.search(/\*\*Paint:\*\*/);
      const paint = paintLabelIndex >= 0
        ? extractJsonObjectFromText(styleBlock, paintLabelIndex)
        : undefined;

      if (!styleKey || !layerType || !isRecord(paint)) continue;

      entries.push({
        key: `${groupKey}:${styleKey}`,
        description: [
          styleName,
          groupDescription
        ].filter(Boolean).join(" | "),
        layerType,
        styleKey,
        styleName,
        isDefaultStyle: styleKey === defaultStyle,
        geometryTypes: groupGeometryTypes,
        renderConfig: {
          layerType,
          paint
        }
      });
    }
  }

  return entries;
};

export const handleStyleCatalogTool = async () => {
  const sourceUrl = toRawGithubUrl(env.STYLE_CATALOG_URL);
  if (!sourceUrl) {
    return {
      success: false,
      styles: [],
      message: "STYLE_CATALOG_URL is not configured."
    };
  }

  if (styleCatalogCache && styleCatalogCache.sourceUrl === sourceUrl && styleCatalogCache.expiresAt > Date.now()) {
    return {
      success: true,
      sourceUrl,
      styles: styleCatalogCache.entries,
      colors: styleCatalogCache.colors
    };
  }

  try {
    const response = await fetch(sourceUrl, {
      headers: {
        Accept: "text/plain"
      }
    });

    if (!response.ok) {
      return {
        success: false,
        sourceUrl,
        styles: [],
        message: `Style catalog request failed: ${response.status} ${response.statusText}`
      };
    }

    const catalogText = await response.text();
    const entries = parseStyleCatalogTextEntries(catalogText);
    const colors = parseStyleCatalogTextColors(catalogText);
    if (entries.length === 0) {
      return {
        success: false,
        sourceUrl,
        styles: [],
        colors,
        message: "Style catalog text did not contain any valid render styles."
      };
    }

    styleCatalogCache = {
      sourceUrl,
      entries,
      colors,
      expiresAt: Date.now() + STYLE_CATALOG_CACHE_TTL_MS
    };

    return {
      success: true,
      sourceUrl,
      styles: entries,
      colors
    };
  } catch (error) {
    console.error("Style Catalog Tool Error:", error);
    return {
      success: false,
      sourceUrl,
      styles: [],
      message: "An error occurred while fetching the style catalog."
    };
  }
};

const normalizeStyleMatchText = (value: unknown): string => {
  return toCleanString(value)?.toLowerCase().replace(/[\s()[\]{}"'`.,:;|/_-]+/g, "") || "";
};

const tokenizeStyleMatchText = (value: unknown): string[] => {
  const text = toCleanString(value)?.toLowerCase() || "";
  return text
    .split(/[\s()[\]{}"'`.,:;|/_-]+/g)
    .map((item) => item.trim())
    .filter((item) => item.length >= 2);
};

const safeMapStyleId = (value: string): string => {
  return value.replace(/[^a-zA-Z0-9_-]+/g, "-").replace(/^-+|-+$/g, "") || "layer";
};

const getMapLayerRecord = (payload: unknown): Record<string, unknown> => {
  const record = pickRecord(payload);
  return pickRecord(record.layer) || record;
};

const getMapControlLayerInfo = (metadata: unknown): ActiveMapLayer | undefined => {
  const record = pickRecord(metadata);
  if (record.event !== "layer_catalog" || !record.layer) return undefined;

  const layer = pickRecord(record.layer);
  const layerId = toStringValue(layer.layerId)
    || toStringValue(layer.styleId)
    || toStringValue(layer.id);
  if (!layerId) return undefined;

  return {
    layerId,
    ...(toStringValue(layer.title) ? { title: toStringValue(layer.title) } : {}),
    ...(toStringValue(layer.styleTitle) ? { title: toStringValue(layer.styleTitle) } : {}),
    ...(toStringValue(layer.sourceLayer) ? { sourceLayer: toStringValue(layer.sourceLayer) } : {})
  };
};

const getActiveMapLayersFromMetadata = (metadataList: unknown[]): ActiveMapLayer[] => {
  const activeLayers = new Map<string, ActiveMapLayer>();

  for (const metadata of metadataList) {
    const record = pickRecord(metadata);

    if (record.event === "layer_catalog") {
      const layer = getMapControlLayerInfo(record);
      if (layer) activeLayers.set(layer.layerId, layer);
      continue;
    }

    if (record.event !== "map_clear") continue;

    const mode = toStringValue(record.mode);
    if (mode === "all") {
      activeLayers.clear();
      continue;
    }

    if (mode === "selected") {
      const layerIds = toUniqueStringList(record.layerIds, record.layerId);
      for (const layerId of layerIds) {
        activeLayers.delete(layerId);
      }
    }
  }

  return Array.from(activeLayers.values());
};

export const getActiveMapLayersForConversation = async (
  userId: string,
  conversationId: string
): Promise<ActiveMapLayer[]> => {
  const messages = await prisma.messages.findMany({
    where: {
      conversation_id: conversationId,
      deleted_at: null,
      conversations: {
        user_id: userId,
        is_deleted: false
      }
    },
    orderBy: [
      { created_at: "asc" },
      { id: "asc" }
    ],
    select: {
      metadata: true
    }
  });

  return getActiveMapLayersFromMetadata(messages.map((message) => message.metadata));
};

const scoreStyleCatalogEntry = (
  entry: StyleCatalogEntry,
  layer: Record<string, unknown>,
  instruction?: string
): number => {
  const layerTerms = [
    layer.type,
    layer.title,
    layer.layerId,
    instruction
  ].map(normalizeStyleMatchText).filter(Boolean);
  const entryTerms = [
    entry.key,
    entry.description,
    entry.layerType,
    entry.styleKey,
    entry.styleName
  ].map(normalizeStyleMatchText).filter(Boolean);

  const textScore = entryTerms.reduce((score, entryTerm) => {
    const matchedLayerTerm = layerTerms.find((layerTerm) => {
      return layerTerm.includes(entryTerm) || entryTerm.includes(layerTerm);
    });
    return score + (matchedLayerTerm ? Math.min(entryTerm.length, matchedLayerTerm.length) : 0);
  }, 0);
  const instructionTerm = normalizeStyleMatchText(instruction);
  const layerTypeTerm = normalizeStyleMatchText(entry.layerType);
  const layerTypeScore = instructionTerm && layerTypeTerm && instructionTerm.includes(layerTypeTerm)
    ? 100
    : 0;
  const instructionTokens = new Set(tokenizeStyleMatchText(instruction));
  const entryTokens = [
    entry.key,
    entry.description,
    entry.layerType,
    entry.styleKey,
    entry.styleName
  ].flatMap(tokenizeStyleMatchText);
  const tokenScore = entryTokens.reduce((score, token) => {
    return score + (instructionTokens.has(token) ? 50 : 0);
  }, 0);
  const defaultScore = entry.isDefaultStyle ? 1 : 0;

  return textScore + layerTypeScore + tokenScore + defaultScore;
};

const getCandidateStyleCatalogEntries = (
  entries: StyleCatalogEntry[],
  layer: Record<string, unknown>
): StyleCatalogEntry[] => {
  const geometryTypes = [
    normalizeGeometryType(layer.geometryType),
    normalizeGeometryType(layer.geometry),
    normalizeGeometryType(layer.geomType)
  ].filter((item): item is string => Boolean(item));
  const uniqueGeometryTypes = new Set(geometryTypes);
  if (uniqueGeometryTypes.size === 0) return [];

  const geometryAwareEntries = entries.filter((entry) => entry.geometryTypes?.length);
  const candidateEntries = uniqueGeometryTypes.size > 0 && geometryAwareEntries.length > 0
    ? geometryAwareEntries.filter((entry) => entry.geometryTypes?.some((geometryType) => uniqueGeometryTypes.has(geometryType)))
    : entries;

  return candidateEntries;
};

export const buildMapStylePayload = async (
  layerPayload: unknown,
  options: { presetKey?: string; instruction?: string } = {}
) => {
  const catalog = await handleStyleCatalogTool();
  if (!catalog.success || !catalog.styles?.length) {
    return {
      success: false,
      event: "map_style",
      message: catalog.message || "No usable style catalog was found."
    };
  }

  const layer = getMapLayerRecord(layerPayload);
  const layerId = toStringValue(layer.layerId) || toStringValue(layer.id);
  const candidateEntries = getCandidateStyleCatalogEntries(catalog.styles, layer);
  const selectedEntry = options.presetKey
    ? candidateEntries.find((entry) => entry.key === options.presetKey || entry.styleKey === options.presetKey)
    : candidateEntries[0];

  if (!layerId || !selectedEntry) {
    return {
      success: false,
      event: "map_style",
      layerId,
      message: "No style matches this layer geometry."
    };
  }

  const renderConfig = pickRecord(selectedEntry.renderConfig);
  const layerType = toStringValue(renderConfig.layerType);
  const mapLayer: Record<string, unknown> = {
    type: layerType,
    ...(Object.keys(pickRecord(renderConfig.layout)).length > 0 ? { layout: pickRecord(renderConfig.layout) } : {}),
    ...(Object.keys(pickRecord(renderConfig.paint)).length > 0 ? { paint: pickRecord(renderConfig.paint) } : {})
  };
  const styleKey = selectedEntry.styleKey || selectedEntry.key;

  return {
    success: true,
    event: "map_style",
    layerId,
    geometryType: toStringValue(layer.geometryType),
    styleKey,
    styleName: selectedEntry.styleName,
    activeStyle: styleKey,
    defaultStyle: candidateEntries[0]?.styleKey || candidateEntries[0]?.key || styleKey,
    layers: [mapLayer]
  };
};

const refreshStyleCatalogTool = async () => {
  styleCatalogCache = undefined;
  return handleStyleCatalogTool();
};

const normalizeColorHex = (value: unknown): string | undefined => {
  const color = toStringValue(value);
  if (!color) return undefined;
  if (/^#[0-9a-f]{3}$/i.test(color)) {
    const [, r = "", g = "", b = ""] = color;
    return `#${r}${r}${g}${g}${b}${b}`.toUpperCase();
  }
  return /^#[0-9a-f]{6}$/i.test(color) ? color.toUpperCase() : undefined;
};

const toStringList = (value: unknown): string[] => {
  if (Array.isArray(value)) {
    return value.map(toStringValue).filter((item): item is string => Boolean(item));
  }
  const stringValue = toStringValue(value);
  return stringValue ? [stringValue] : [];
};

const toUniqueStringList = (...values: unknown[]): string[] => {
  return Array.from(new Set(values.flatMap(toStringList).filter(Boolean)));
};

const resolveCatalogColor = (
  colorKey: string | undefined,
  colors: StyleColorEntry[]
): string | undefined => {
  if (!colorKey) return undefined;
  const palette = new Map(colors.map((color) => [color.key.toLowerCase(), color.value]));
  return normalizeColorHex(palette.get(colorKey.toLowerCase()));
};

const getEditInstruction = (args: EditMapStyleArgs): string => {
  return [
    args.instruction,
    args.message,
    args.query,
    args.request
  ].map(toStringValue).filter(Boolean).join(" ");
};

const buildHeatmapColorRamp = (color: string): unknown[] => {
  return [
    "interpolate",
    ["linear"],
    ["heatmap-density"],
    0,
    "rgba(255,255,255,0)",
    0.35,
    color,
    0.7,
    color,
    1,
    "#ffffff"
  ];
};

const shouldPatchPaintColorKey = (key: string): boolean => {
  return key.endsWith("-color")
    && !key.includes("stroke")
    && !key.includes("outline");
};

const normalizeEditText = (value: unknown): string => {
  return typeof value === "string"
    ? value.toLowerCase().replace(/[\s()[\]{}"'`.,:;|/_-]+/g, "")
    : "";
};

const getExpressionAttributeKey = (value: unknown): string | undefined => {
  if (!Array.isArray(value)) return undefined;

  const [operator, attributeKey] = value;
  if (operator === "get") return toStringValue(attributeKey);

  for (const item of value) {
    const key = getExpressionAttributeKey(item);
    if (key) return key;
  }

  return undefined;
};

const valuesMatchStyleStop = (left: unknown, right: unknown): boolean => {
  const leftNumber = toNumberValue(left);
  const rightNumber = toNumberValue(right);
  if (leftNumber !== undefined && rightNumber !== undefined) {
    return Math.abs(leftNumber - rightNumber) < 0.000001;
  }

  return toStringValue(left) === toStringValue(right);
};

const replaceAttributeExpressionStopOutput = (
  value: unknown,
  attributeKey: string,
  attributeValue: unknown,
  output: unknown
): { value: unknown; changed: boolean } => {
  if (!Array.isArray(value)) return { value, changed: false };

  const next = [...value];
  let changed = false;
  const operator = next[0];

  if (operator === "interpolate" && getExpressionAttributeKey(next[2]) === attributeKey) {
    for (let index = 3; index < next.length - 1; index += 2) {
      if (valuesMatchStyleStop(next[index], attributeValue)) {
        next[index + 1] = output;
        changed = true;
      }
    }

    const numericAttributeValue = toNumberValue(attributeValue);
    if (!changed && numericAttributeValue !== undefined) {
      let insertIndex = next.length;
      for (let index = 3; index < next.length - 1; index += 2) {
        const stopValue = toNumberValue(next[index]);
        if (stopValue !== undefined && stopValue > numericAttributeValue) {
          insertIndex = index;
          break;
        }
      }
      next.splice(insertIndex, 0, numericAttributeValue, output);
      changed = true;
    }
  }

  if (operator === "match" && getExpressionAttributeKey(next[1]) === attributeKey) {
    for (let index = 2; index < next.length - 1; index += 2) {
      if (valuesMatchStyleStop(next[index], attributeValue)) {
        next[index + 1] = output;
        changed = true;
      }
    }

    if (!changed) {
      next.splice(Math.max(2, next.length - 1), 0, attributeValue, output);
      changed = true;
    }
  }

  for (let index = 0; index < next.length; index += 1) {
    const nested = replaceAttributeExpressionStopOutput(next[index], attributeKey, attributeValue, output);
    if (nested.changed) {
      next[index] = nested.value;
      changed = true;
    }
  }

  return { value: changed ? next : value, changed };
};

const replaceAttributeExpressionOutputs = (
  value: unknown,
  attributeKey: string,
  outputs: unknown[]
): { value: unknown; changed: boolean } => {
  if (!Array.isArray(value) || outputs.length === 0) return { value, changed: false };

  const next = [...value];
  let changed = false;
  const operator = next[0];

  if (operator === "interpolate" && getExpressionAttributeKey(next[2]) === attributeKey) {
    let outputIndex = 0;
    for (let index = 4; index < next.length; index += 2) {
      const output = outputs[outputIndex % outputs.length];
      if (JSON.stringify(next[index]) !== JSON.stringify(output)) {
        next[index] = output;
        changed = true;
      }
      outputIndex += 1;
    }
  }

  if (operator === "match" && getExpressionAttributeKey(next[1]) === attributeKey) {
    let outputIndex = 0;
    for (let index = 3; index < next.length - 1; index += 2) {
      const output = outputs[outputIndex % outputs.length];
      if (JSON.stringify(next[index]) !== JSON.stringify(output)) {
        next[index] = output;
        changed = true;
      }
      outputIndex += 1;
    }
  }

  for (let index = 0; index < next.length; index += 1) {
    const nested = replaceAttributeExpressionOutputs(next[index], attributeKey, outputs);
    if (nested.changed) {
      next[index] = nested.value;
      changed = true;
    }
  }

  return { value: changed ? next : value, changed };
};

const collectPaintAttributeStops = (
  value: unknown,
  stops: Array<{ attributeKey: string; value: unknown }> = []
): Array<{ attributeKey: string; value: unknown }> => {
  if (!Array.isArray(value)) {
    if (isRecord(value)) {
      for (const nestedValue of Object.values(value)) {
        collectPaintAttributeStops(nestedValue, stops);
      }
    }
    return stops;
  }

  const operator = value[0];
  if (operator === "interpolate") {
    const attributeKey = getExpressionAttributeKey(value[2]);
    if (attributeKey) {
      for (let index = 3; index < value.length - 1; index += 2) {
        stops.push({ attributeKey, value: value[index] });
      }
    }
  }

  if (operator === "match") {
    const attributeKey = getExpressionAttributeKey(value[1]);
    if (attributeKey) {
      for (let index = 2; index < value.length - 1; index += 2) {
        stops.push({ attributeKey, value: value[index] });
      }
    }
  }

  for (const item of value) {
    collectPaintAttributeStops(item, stops);
  }

  return stops;
};

const resolveAttributeStopEdit = (
  paint: Record<string, unknown>,
  args: EditMapStyleArgs
): { attributeKey: string; attributeValue: unknown; paintKey?: string } | undefined => {
  const instruction = normalizeEditText(getEditInstruction(args));
  const requestedAttributeKey = toStringValue(args.attributeKey);
  const requestedAttributeValue = args.attributeValue;
  const requestedPaintKey = toStringValue(args.paintKey);

  if (requestedAttributeKey && requestedAttributeValue !== undefined) {
    return {
      attributeKey: requestedAttributeKey,
      attributeValue: requestedAttributeValue,
      ...(requestedPaintKey ? { paintKey: requestedPaintKey } : {})
    };
  }

  if (!instruction) return undefined;

  const stops = collectPaintAttributeStops(paint);
  const matchedStop = stops.find((stop) => {
    const normalizedAttribute = normalizeEditText(stop.attributeKey);
    const normalizedValue = normalizeEditText(toStringValue(stop.value));
    return Boolean(
      normalizedAttribute
      && normalizedValue
      && instruction.includes(normalizedAttribute)
      && instruction.includes(normalizedValue)
    );
  });
  return matchedStop
    ? {
      attributeKey: matchedStop.attributeKey,
      attributeValue: matchedStop.value
    }
    : undefined;
};

const buildAttributeStopPaintPatch = (
  paint: Record<string, unknown>,
  args: EditMapStyleArgs,
  output?: unknown
): Record<string, unknown> => {
  const primaryEdit = output !== undefined ? resolveAttributeStopEdit(paint, args) : undefined;
  const explicitEdits: Array<{
    attributeKey: string;
    attributeValue: unknown;
    output: unknown;
    paintKey?: string;
  }> = Array.isArray(args.attributePatches)
    ? args.attributePatches
        .map((item) => pickRecord(item))
        .reduce<Array<{
          attributeKey: string;
          attributeValue: unknown;
          output: unknown;
          paintKey?: string;
        }>>((patches, item) => {
          const attributeKey = toStringValue(item.attributeKey) || toStringValue(args.attributeKey);
          const paintKey = toStringValue(item.paintKey) || toStringValue(args.paintKey);
          const attributeValue = item.attributeValue;
          const patchOutput = normalizeColorHex(item.colorValue)
            || normalizeStylePropertyValue(item.output ?? item.value);
          if (attributeKey && attributeValue !== undefined && patchOutput !== undefined) {
            patches.push({
              attributeKey,
              attributeValue,
              output: patchOutput,
              ...(paintKey ? { paintKey } : {})
            });
          }
          return patches;
        }, [])
    : [];
  const edits = [
    ...(primaryEdit && output !== undefined ? [{ ...primaryEdit, output }] : []),
    ...explicitEdits
  ];
  if (edits.length === 0) return {};

  return Object.fromEntries(
    Object.entries(paint)
      .map(([paintKey, paintValue]) => {
        let nextValue = paintValue;
        let changed = false;
        for (const edit of edits) {
          if (edit.paintKey && edit.paintKey !== paintKey) continue;
          const outputIsColor = typeof edit.output === "string" && Boolean(normalizeColorHex(edit.output));
          if (outputIsColor && !paintKey.endsWith("-color")) continue;
          const patched = replaceAttributeExpressionStopOutput(
            nextValue,
            edit.attributeKey,
            edit.attributeValue,
            edit.output
          );
          nextValue = patched.value;
          changed = changed || patched.changed;
        }
        return changed ? [paintKey, nextValue] : undefined;
      })
      .filter((entry): entry is [string, unknown] => Boolean(entry))
  );
};

const buildAttributeRampOutputPatch = (
  paint: Record<string, unknown>,
  args: EditMapStyleArgs,
  outputs: unknown[]
): Record<string, unknown> => {
  const attributeKey = toStringValue(args.attributeKey);
  if (!attributeKey || outputs.length === 0) return {};
  if (args.attributeValue !== undefined || (Array.isArray(args.attributePatches) && args.attributePatches.length > 0)) return {};

  const requestedPaintKey = toStringValue(args.paintKey);
  return Object.fromEntries(
    Object.entries(paint)
      .map(([paintKey, paintValue]) => {
        if (requestedPaintKey && requestedPaintKey !== paintKey) return undefined;
        const patchOutputs = paintKey.endsWith("-color")
          ? outputs.filter((item) => typeof item !== "string" || Boolean(normalizeColorHex(item)))
          : outputs;
        if (patchOutputs.length === 0) return undefined;

        const patched = replaceAttributeExpressionOutputs(paintValue, attributeKey, patchOutputs);
        return patched.changed ? [paintKey, patched.value] : undefined;
      })
      .filter((entry): entry is [string, unknown] => Boolean(entry))
  );
};

const buildColorPatchFromPaint = (
  paint: Record<string, unknown>,
  color?: string
): Record<string, unknown> => {
  if (!color) return {};

  const patch = Object.fromEntries(
    Object.keys(paint)
      .filter(shouldPatchPaintColorKey)
      .map((key) => [
        key,
        key === "heatmap-color" ? buildHeatmapColorRamp(color) : color
      ])
  );

  if (Object.keys(patch).length > 0) return patch;

  return {};
};

const resolveColorKeysFromInstruction = (
  instruction: string,
  colors: StyleColorEntry[]
): string[] => {
  const normalizedInstruction = normalizeEditText(instruction);
  if (!normalizedInstruction) return [];

  return colors
    .map((color) => color.key)
    .filter((key) => {
      const normalizedKey = normalizeEditText(key);
      return normalizedKey && normalizedInstruction.includes(normalizedKey);
    });
};

const getEditDistance = (left: string, right: string): number => {
  const distances = Array.from({ length: left.length + 1 }, (_, row) => {
    return Array.from({ length: right.length + 1 }, (_, column) => {
      if (row === 0) return column;
      if (column === 0) return row;
      return 0;
    });
  });

  for (let row = 1; row <= left.length; row += 1) {
    for (let column = 1; column <= right.length; column += 1) {
      const cost = left[row - 1] === right[column - 1] ? 0 : 1;
      distances[row][column] = Math.min(
        distances[row - 1][column] + 1,
        distances[row][column - 1] + 1,
        distances[row - 1][column - 1] + cost
      );
    }
  }

  return distances[left.length][right.length];
};

const isClosePaintKeyMatch = (candidate: string, term: string): boolean => {
  if (!candidate || !term) return false;
  if (candidate === term) return true;
  if (Math.abs(candidate.length - term.length) > 2) return false;
  const distance = getEditDistance(candidate, term);
  return distance <= Math.max(1, Math.floor(candidate.length * 0.15));
};

const getInstructionTerms = (instruction: string): string[] => {
  return instruction
    .split(/[^a-z0-9_-]+/i)
    .map(normalizeEditText)
    .filter((term) => term.length >= 4);
};

const resolveFuzzyPaintKeyFromInstruction = (
  paintKeys: string[],
  normalizedInstruction: string,
  suffix?: string
): string | undefined => {
  const terms = getInstructionTerms(normalizedInstruction);
  if (terms.length === 0) return undefined;

  return paintKeys.find((key) => {
    if (suffix && !key.endsWith(suffix)) return false;
    const normalizedKey = normalizeEditText(key);
    return terms.some((term) => isClosePaintKeyMatch(normalizedKey, term));
  });
};

const resolvePaintKeyFromInstruction = (
  paint: Record<string, unknown>,
  args: EditMapStyleArgs
): string | undefined => {
  const explicitPaintKey = toStringValue(args.paintKey);
  if (explicitPaintKey) return explicitPaintKey;

  const instruction = normalizeEditText(getEditInstruction(args));
  if (!instruction) return undefined;

  const existingPaintKey = Object.keys(paint).find((key) => {
    const normalizedKey = normalizeEditText(key);
    return normalizedKey && instruction.includes(normalizedKey);
  });
  if (existingPaintKey) return existingPaintKey;

  return resolveFuzzyPaintKeyFromInstruction(Object.keys(paint), instruction, "-color");
};

const buildExplicitPaintColorPatch = (
  paint: Record<string, unknown>,
  args: EditMapStyleArgs,
  color?: string
): Record<string, unknown> => {
  if (!color) return {};

  const paintKey = resolvePaintKeyFromInstruction(paint, args);
  if (!paintKey || !paintKey.endsWith("-color")) return {};

  return {
    [paintKey]: paintKey === "heatmap-color" ? buildHeatmapColorRamp(color) : color
  };
};

const parseGenericPaintValue = (args: EditMapStyleArgs): unknown => {
  if (args.value !== undefined) return args.value;

  const instruction = getEditInstruction(args);
  const explicitPaintKey = toStringValue(args.paintKey);
  if (explicitPaintKey) {
    const escapedKey = explicitPaintKey.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
    const match = instruction.match(new RegExp(`${escapedKey}\\s*(?:is|to|=|:|เป็น)?\\s*([^\\s,]+)`, "i"));
    if (match?.[1]) return match[1];
  }

  return undefined;
};

const buildExplicitGenericPaintPatch = (
  paint: Record<string, unknown>,
  args: EditMapStyleArgs,
  color?: string
): Record<string, unknown> => {
  const paintKey = resolvePaintKeyByInstruction(paint, args);
  if (!paintKey) return {};

  if (paintKey.endsWith("-color") && color) {
    return { [paintKey]: paintKey === "heatmap-color" ? buildHeatmapColorRamp(color) : color };
  }

  const value = parseGenericPaintValue(args);
  const numberValue = toNumberValue(value);
  if (
    numberValue !== undefined
    && (paint[paintKey] === undefined || typeof paint[paintKey] === "number")
  ) {
    return { [paintKey]: numberValue };
  }

  const stringValue = toStringValue(value);
  if (
    stringValue
    && (paint[paintKey] === undefined || typeof paint[paintKey] === "string")
  ) {
    return { [paintKey]: stringValue };
  }

  return {};
};

const resolvePaintKeyByInstruction = (
  paint: Record<string, unknown>,
  args: EditMapStyleArgs,
  suffix?: string
): string | undefined => {
  const explicitPaintKey = toStringValue(args.paintKey);
  if (explicitPaintKey && (!suffix || explicitPaintKey.endsWith(suffix))) return explicitPaintKey;

  const instruction = normalizeEditText(getEditInstruction(args));
  if (!instruction) return undefined;

  const exactPaintKey = Object.keys(paint).find((key) => {
    if (suffix && !key.endsWith(suffix)) return false;
    const normalizedKey = normalizeEditText(key);
    return normalizedKey && instruction.includes(normalizedKey);
  });
  if (exactPaintKey) return exactPaintKey;

  return resolveFuzzyPaintKeyFromInstruction(Object.keys(paint), instruction, suffix);
};

const getEditablePaintColorKey = (
  paint: Record<string, unknown>
): string | undefined => {
  return Object.keys(paint).find(shouldPatchPaintColorKey);
};

const collectExpressionOutputs = (
  value: unknown,
  outputs: unknown[] = []
): unknown[] => {
  if (!Array.isArray(value)) return outputs;

  const operator = value[0];
  if (operator === "interpolate") {
    for (let index = 4; index < value.length; index += 2) {
      outputs.push(value[index]);
    }
  }
  if (operator === "match") {
    for (let index = 3; index < value.length; index += 2) {
      outputs.push(value[index]);
    }
    if (value.length > 2) outputs.push(value[value.length - 1]);
  }

  for (const item of value) {
    collectExpressionOutputs(item, outputs);
  }

  return outputs;
};

const getCatalogColorValues = async (): Promise<string[]> => {
  const catalog = await handleStyleCatalogTool();
  const colors = catalog.success && Array.isArray(catalog.colors) ? catalog.colors : [];
  return colors
    .map((color) => normalizeColorHex(pickRecord(color).value))
    .filter((color): color is string => Boolean(color));
};

const getAttributeStylePalette = async (
  existingPaint: Record<string, unknown>,
  color?: string,
  paintKey?: string
): Promise<string[]> => {
  const directColorValues = paintKey && existingPaint[paintKey] !== undefined
    ? [existingPaint[paintKey]]
    : Object.values(existingPaint);
  const directColors = directColorValues
    .map(normalizeColorHex)
    .filter((item): item is string => Boolean(item));
  const existingColors = Object.values(existingPaint)
    .flatMap((value) => collectExpressionOutputs(value))
    .map(normalizeColorHex)
    .filter((item): item is string => Boolean(item));
  const catalogColors = await getCatalogColorValues();

  return Array.from(new Set([
    ...(color ? [color] : []),
    ...directColors,
    ...existingColors,
    ...catalogColors
  ]));
};

const getExplicitAttributeOutputs = (value: unknown): unknown[] => {
  if (!Array.isArray(value)) return [];

  return value
    .map((item) => {
      const record = pickRecord(item);
      return record.output !== undefined
        ? record.output
        : record.colorValue !== undefined
          ? record.colorValue
          : record.color !== undefined
            ? record.color
            : record.value !== undefined
              ? record.value
              : item;
    })
    .filter((item) => item !== undefined && item !== null);
};

const buildNumericAttributeOutputs = (
  values: number[],
  count: number
): number[] => {
  if (values.length === 0 || count <= 0) return [];

  const min = Math.min(...values);
  const max = Math.max(...values);
  if (min !== max) {
    return Array.from({ length: count }, (_, index) => {
      const ratio = count === 1 ? 1 : index / (count - 1);
      return Number((min + ((max - min) * ratio)).toFixed(6));
    });
  }

  if (max === 0) return Array.from({ length: count }, () => 0);
  return Array.from({ length: count }, (_, index) => {
    return Number((max * ((index + 1) / count)).toFixed(6));
  });
};

const getAttributeStyleOutputs = async (
  args: EditMapStyleArgs,
  paintKey: string,
  existingPaint: Record<string, unknown>,
  color: string | undefined,
  count: number
): Promise<unknown[]> => {
  const explicitOutputs = getExplicitAttributeOutputs(args.outputs);
  if (explicitOutputs.length > 0) return explicitOutputs;

  if (paintKey.endsWith("-color")) {
    return getAttributeStylePalette(existingPaint, color, paintKey);
  }

  const currentValue = existingPaint[paintKey];
  const expressionOutputs = collectExpressionOutputs(currentValue);
  const numericOutputs = [
    ...expressionOutputs,
    ...(typeof currentValue === "number" ? [currentValue] : [])
  ]
    .map(toNumberValue)
    .filter((value): value is number => value !== undefined);
  if (numericOutputs.length > 0) {
    return buildNumericAttributeOutputs(numericOutputs, count);
  }

  return Array.from(new Set(expressionOutputs));
};

const getAttributeValuesList = (value: unknown): unknown[] => {
  if (!Array.isArray(value)) return [];

  return value
    .map((item) => {
      const record = pickRecord(item);
      return record.value !== undefined ? record.value : item;
    })
    .filter((item) => item !== undefined && item !== null && item !== "");
};

const getRequestedStylePropertyKeys = (...values: unknown[]): string[] => {
  return Array.from(new Set(
    values.flatMap((value) => toStringList(value))
      .map((value) => value.trim())
      .filter(Boolean)
  ));
};

const getExistingStylePropertyKeysByText = (
  layer: Record<string, unknown>,
  args: EditMapStyleArgs,
  kind: StylePropertyKind
): string[] => {
  const existingKeys = Object.keys(pickRecord(layer[kind]));
  if (existingKeys.length === 0) return [];

  const normalizedTarget = normalizeStyleMatchText(args.target);
  const normalizedInstruction = normalizeStyleMatchText(getEditInstruction(args));
  return existingKeys.filter((key) => {
    const normalizedKey = normalizeStyleMatchText(key);
    return Boolean(normalizedKey && (
      normalizedKey === normalizedTarget
      || normalizedInstruction.includes(normalizedKey)
    ));
  });
};

const getExistingStylePropertyTarget = (
  layers: unknown[],
  target: unknown
): { kind: StylePropertyKind; key: string } | undefined => {
  const key = toStringValue(target)?.trim();
  if (!key) return undefined;

  for (const layer of layers) {
    const layerRecord = pickRecord(layer);
    if (pickRecord(layerRecord.paint)[key] !== undefined) return { kind: "paint", key };
    if (pickRecord(layerRecord.layout)[key] !== undefined) return { kind: "layout", key };
  }

  return undefined;
};

const normalizeStylePropertyTargetArgs = (
  args: EditMapStyleArgs,
  layers: unknown[]
): EditMapStyleArgs => {
  const propertyTarget = getExistingStylePropertyTarget(layers, args.target);
  if (!propertyTarget) return args;

  return {
    ...args,
    target: undefined,
    ...(propertyTarget.kind === "paint" && !toStringValue(args.paintKey)
      ? { paintKey: propertyTarget.key }
      : {}),
    ...(propertyTarget.kind === "layout" && !toStringValue(args.layoutKey)
      ? { layoutKey: propertyTarget.key }
      : {})
  };
};

const normalizeStylePropertyOperation = (value: unknown): EditMapStyleOperation | undefined => {
  const normalized = toStringValue(value)?.trim().toLowerCase().replace(/[\s-]+/g, "_");
  if (
    normalized === "add_property"
    || normalized === "remove_property"
    || normalized === "update_layer"
    || normalized === "add_filter"
  ) return normalized;
  return undefined;
};

const inferStylePropertyOperationFromInstruction = (
  instruction: string
): "add_property" | "remove_property" | undefined => {
  if (/\b(remove|delete|drop|clear)\b|ลบ/i.test(instruction)) return "remove_property";
  if (/\b(add|insert|set|change|update)\b|เพิ่ม|เปลี่ยน|แก้/i.test(instruction)) return "add_property";
  return undefined;
};

const getInstructionStylePropertyKey = (instruction: string): string | undefined => {
  const match = instruction.match(/\b([a-z][a-z0-9]*(?:-[a-z0-9]+)+)\b/i);
  return match?.[1];
};

const parseStylePropertyInstruction = (
  args: EditMapStyleArgs
): { kind: StylePropertyKind; key: string; value?: unknown } | undefined => {
  const instruction = getEditInstruction(args);
  const explicitLayoutKey = toStringValue(args.layoutKey);
  if (explicitLayoutKey) {
    return {
      kind: "layout",
      key: explicitLayoutKey,
      ...(args.value !== undefined ? { value: args.value } : {})
    };
  }

  const explicitPaintKey = toStringValue(args.paintKey);
  const key = explicitPaintKey || getInstructionStylePropertyKey(instruction);
  if (!key) return undefined;

  const value = explicitPaintKey ? parseGenericPaintValue(args) : parseGenericPaintValue({ ...args, paintKey: key });
  return {
    kind: "paint",
    key,
    ...(value !== undefined ? { value } : {})
  };
};

const getStylePropertyPrefix = (key: string): string | undefined => {
  const index = key.indexOf("-");
  return index > 0 ? key.slice(0, index) : undefined;
};

const loadMapLibreStyleSpec = async (): Promise<Record<string, unknown> | undefined> => {
  if (mapLibreStyleSpecCache !== undefined) return mapLibreStyleSpecCache || undefined;

  try {
    const packageName = "@maplibre/maplibre-gl-style-spec";
    const module = await import(packageName);
    const moduleRecord = pickRecord(module);
    const defaultRecord = pickRecord(moduleRecord.default);
    const candidates = [
      moduleRecord.latest,
      moduleRecord.v8,
      defaultRecord.latest,
      defaultRecord.v8,
      moduleRecord.default,
      module
    ];

    mapLibreStyleSpecCache = candidates
      .map(pickRecord)
      .find((candidate) => Object.keys(candidate).length > 0) || null;
  } catch {
    mapLibreStyleSpecCache = null;
  }

  return mapLibreStyleSpecCache || undefined;
};

const getOfficialStyleSpecBucket = (
  styleSpec: Record<string, unknown> | undefined,
  layerType: string | undefined,
  kind: StylePropertyKind
): Record<string, unknown> | undefined => {
  if (!styleSpec || !layerType) return undefined;

  const layerSpecKey = layerType.replace(/-/g, "_");
  const bucket = pickRecord(styleSpec[`${kind}_${layerSpecKey}`]);
  return Object.keys(bucket).length > 0 ? bucket : undefined;
};

const isStylePropertyAllowedByOfficialSpec = (
  key: string,
  layer: Record<string, unknown>,
  kind: StylePropertyKind,
  officialSpec?: Record<string, unknown>
): boolean | undefined => {
  const bucket = getOfficialStyleSpecBucket(officialSpec, toStringValue(layer.type), kind);
  if (!bucket) return undefined;

  return bucket[key] !== undefined;
};

const canPatchStyleProperty = (
  key: string,
  layer: Record<string, unknown>,
  kind: StylePropertyKind,
  officialSpec?: Record<string, unknown>
): boolean => {
  const officialResult = isStylePropertyAllowedByOfficialSpec(key, layer, kind, officialSpec);
  return officialResult === true;
};

const normalizeStylePropertyValue = (value: unknown): unknown => {
  const numericValue = toNumberValue(value);
  if (numericValue !== undefined) return numericValue;

  if (typeof value === "string") {
    const normalized = value.trim().toLowerCase();
    if (normalized === "true") return true;
    if (normalized === "false") return false;
  }

  return value;
};

const resolveEditMapStyleOperation = (args: EditMapStyleArgs): EditMapStyleOperation => {
  const explicit = normalizeStylePropertyOperation(args.operation || args.action);
  const instruction = getEditInstruction(args);
  const inferred = inferStylePropertyOperationFromInstruction(instruction);

  if (explicit === "add_filter") {
    return explicit;
  }
  if (explicit === "remove_property" || inferred === "remove_property") {
    return "remove_property";
  }
  if (toStringValue(args.attributeKey)) {
    return "update_layer";
  }
  if (explicit) return explicit;
  if (inferred && getInstructionStylePropertyKey(instruction)) return inferred;

  return "update_layer";
};

const FILTER_COMPARISON_OPERATORS = new Set(["==", "!=", ">", ">=", "<", "<=", "in", "!in"]);
const FILTER_LOGIC_OPERATORS = new Set(["all", "any"]);

const normalizeFilterLogic = (value: unknown): "all" | "any" => {
  const logic = toStringValue(value)?.toLowerCase();
  return logic && FILTER_LOGIC_OPERATORS.has(logic) ? logic as "all" | "any" : "all";
};

const normalizeFilterConditionValue = (value: unknown, fieldType: string | undefined): unknown => {
  const normalizedType = fieldType?.trim().toLowerCase();
  if (normalizedType === "number") return toNumberValue(value);
  if (normalizedType === "boolean") {
    if (typeof value === "boolean") return value;
    const normalized = toStringValue(value)?.toLowerCase();
    if (normalized === "true") return true;
    if (normalized === "false") return false;
    return undefined;
  }
  return value;
};

const getFilterAttributeFields = (args: EditMapStyleArgs): Record<string, string> => {
  return Object.fromEntries(
    Object.entries(pickRecord(args.attributeFields)).flatMap(([key, definition]) => {
      const type = typeof definition === "string"
        ? definition
        : toStringValue(pickRecord(definition).type);
      return type ? [[key, type]] : [];
    })
  );
};

const buildFilterConditionExpression = (
  condition: unknown,
  attributeFields: Record<string, string>
): unknown[] | undefined => {
  const record = pickRecord(condition);
  const attributeKey = toStringValue(record.attributeKey || record.field || record.key);
  const operator = toStringValue(record.operator);
  if (!attributeKey || !operator || !FILTER_COMPARISON_OPERATORS.has(operator)) return undefined;

  const fieldType = attributeFields[attributeKey];
  if (!fieldType) return undefined;

  if (operator === "in" || operator === "!in") {
    const rawValues = Array.isArray(record.values)
      ? record.values
      : Array.isArray(record.value)
        ? record.value
        : [record.value];
    const values = rawValues
      .map((value) => normalizeFilterConditionValue(value, fieldType))
      .filter((value) => value !== undefined);
    return values.length > 0 ? [operator, ["get", attributeKey], ["literal", values]] : undefined;
  }

  const value = normalizeFilterConditionValue(record.value, fieldType);
  return value !== undefined ? [operator, ["get", attributeKey], value] : undefined;
};

const buildStructuredFilterExpression = (args: EditMapStyleArgs): unknown[] | undefined => {
  const conditions = Array.isArray(args.filterConditions) ? args.filterConditions : [];
  const attributeFields = getFilterAttributeFields(args);
  if (conditions.length === 0 || Object.keys(attributeFields).length === 0) return undefined;

  const expressions = conditions
    .map((condition) => buildFilterConditionExpression(condition, attributeFields))
    .filter((condition): condition is unknown[] => Boolean(condition));
  if (expressions.length !== conditions.length || expressions.length === 0) return undefined;
  if (expressions.length === 1) return expressions[0];

  return [normalizeFilterLogic(args.filterLogic), ...expressions];
};

const getFilterExpressionAttributes = (filter: unknown): string[] => {
  if (!Array.isArray(filter)) return [];
  const attributes: string[] = [];
  if (filter[0] === "get") {
    const attribute = toStringValue(filter[1]);
    if (attribute) attributes.push(attribute);
  }
  for (const item of filter) attributes.push(...getFilterExpressionAttributes(item));
  return Array.from(new Set(attributes));
};

const isValidFilterExpression = (filter: unknown, attributeFields: Record<string, string>): filter is unknown[] => {
  if (!Array.isArray(filter) || filter.length === 0) return false;
  const attributes = getFilterExpressionAttributes(filter);
  if (attributes.length === 0 || !attributes.every((attribute) => attributeFields[attribute] !== undefined)) return false;
  return createExpression(filter).result === "success";
};

const flattenFilterConditions = (filter: unknown): unknown[][] => {
  if (!Array.isArray(filter) || filter.length === 0) return [];
  return FILTER_LOGIC_OPERATORS.has(String(filter[0]))
    ? filter.slice(1).filter(Array.isArray) as unknown[][]
    : [filter];
};

const getFilterLogic = (filter: unknown, requestedLogic: unknown): "all" | "any" => {
  const explicitLogic = toStringValue(requestedLogic)?.toLowerCase();
  if (explicitLogic && FILTER_LOGIC_OPERATORS.has(explicitLogic)) {
    return explicitLogic as "all" | "any";
  }
  const currentLogic = Array.isArray(filter) ? toStringValue(filter[0])?.toLowerCase() : undefined;
  return currentLogic && FILTER_LOGIC_OPERATORS.has(currentLogic)
    ? currentLogic as "all" | "any"
    : "all";
};

const applyFilterOperation = (
  layer: Record<string, unknown>,
  args: EditMapStyleArgs,
  operation: Extract<EditMapStyleOperation, "add_filter">
): Record<string, unknown> => {
  const attributeFields = getFilterAttributeFields(args);
  const directFilter = args.filter;
  const candidateFilter = isValidFilterExpression(directFilter, attributeFields)
    ? directFilter
    : buildStructuredFilterExpression(args);
  const requestedFilter = isValidFilterExpression(candidateFilter, attributeFields) ? candidateFilter : undefined;
  if (!requestedFilter) return layer;

  const currentConditions = flattenFilterConditions(layer.filter);
  const requestedConditions = flattenFilterConditions(requestedFilter);
  const filterLogic = getFilterLogic(layer.filter, args.filterLogic);
  const combined = [...currentConditions];
  for (const condition of requestedConditions) {
    if (!combined.some((item) => JSON.stringify(item) === JSON.stringify(condition))) combined.push(condition);
  }
  return {
    ...layer,
    filter: combined.length === 1 ? combined[0] : [filterLogic, ...combined]
  };
};

const buildPropertyAddPatch = (
  layer: Record<string, unknown>,
  args: EditMapStyleArgs,
  color?: string,
  officialSpec?: Record<string, unknown>
): { paint: Record<string, unknown>; layout: Record<string, unknown> } => {
  const paint = { ...pickRecord(args.paint) };
  const layout = { ...pickRecord(args.layout) };
  const parsedProperty = parseStylePropertyInstruction(args);
  const paintKey = parsedProperty?.kind === "paint" ? parsedProperty.key : toStringValue(args.paintKey);
  const layoutKey = parsedProperty?.kind === "layout" ? parsedProperty.key : toStringValue(args.layoutKey);
  const propertyValue = normalizeStylePropertyValue(parsedProperty?.value ?? args.value);

  if (paintKey && paint[paintKey] === undefined && canPatchStyleProperty(paintKey, layer, "paint", officialSpec)) {
    if (paintKey.endsWith("-color") && color) {
      paint[paintKey] = color;
    } else if (propertyValue !== undefined) {
      paint[paintKey] = propertyValue;
    }
  }
  if (layoutKey && layout[layoutKey] === undefined && propertyValue !== undefined && canPatchStyleProperty(layoutKey, layer, "layout", officialSpec)) {
    layout[layoutKey] = propertyValue;
  }

  return { paint, layout };
};

const applyStylePropertyOperation = (
  layer: Record<string, unknown>,
  args: EditMapStyleArgs,
  operation: "add_property" | "remove_property",
  color?: string,
  officialSpec?: Record<string, unknown>
): Record<string, unknown> => {
  const existingPaint = pickRecord(layer.paint);
  const existingLayout = pickRecord(layer.layout);

  if (operation === "remove_property") {
    const parsedProperty = parseStylePropertyInstruction(args);
    const parsedPaintKey = parsedProperty?.kind === "paint" ? parsedProperty.key : undefined;
    const parsedLayoutKey = parsedProperty?.kind === "layout" ? parsedProperty.key : undefined;
    const paintKeys = getRequestedStylePropertyKeys(
      args.paintKey,
      parsedPaintKey,
      args.removePaintKeys,
      Object.keys(pickRecord(args.paint)),
      getExistingStylePropertyKeysByText(layer, args, "paint")
    )
      .filter((key) => canPatchStyleProperty(key, layer, "paint", officialSpec));
    const layoutKeys = getRequestedStylePropertyKeys(
      args.layoutKey,
      parsedLayoutKey,
      args.removeLayoutKeys,
      Object.keys(pickRecord(args.layout)),
      getExistingStylePropertyKeysByText(layer, args, "layout")
    )
      .filter((key) => canPatchStyleProperty(key, layer, "layout", officialSpec));
    if (paintKeys.length === 0 && layoutKeys.length === 0) return layer;

    const paint = { ...existingPaint };
    const layout = { ...existingLayout };
    for (const key of paintKeys) delete paint[key];
    for (const key of layoutKeys) delete layout[key];

    return {
      ...layer,
      paint,
      ...(Object.keys(layout).length > 0 || Object.keys(existingLayout).length > 0 ? { layout } : {})
    };
  }

  const patches = buildPropertyAddPatch(layer, args, color, officialSpec);
  const paintPatch = patches.paint;
  const layoutPatch = patches.layout;
  if (Object.keys(paintPatch).length === 0 && Object.keys(layoutPatch).length === 0) return layer;

  return {
    ...layer,
    ...(Object.keys(paintPatch).length > 0 ? { paint: { ...existingPaint, ...paintPatch } } : {}),
    ...(Object.keys(layoutPatch).length > 0 ? { layout: { ...existingLayout, ...layoutPatch } } : {})
  };
};

const getAttributeNumericStats = (
  args: EditMapStyleArgs
): { min: number; max: number } | undefined => {
  const stats = pickRecord(args.attributeStats);
  const min = toNumberValue(stats.min);
  const max = toNumberValue(stats.max);
  if (min !== undefined && max !== undefined && min !== max) return { min, max };

  const values = getAttributeValuesList(args.attributeValues)
    .map(toNumberValue)
    .filter((value): value is number => value !== undefined)
    .sort((left, right) => left - right);
  if (values.length === 0) return undefined;

  const valueMin = values[0];
  const valueMax = values[values.length - 1];
  return valueMin !== valueMax ? { min: valueMin, max: valueMax } : undefined;
};

const buildAttributePaintPatch = async (
  args: EditMapStyleArgs,
  color: string | undefined,
  existingPaint: Record<string, unknown>
): Promise<Record<string, unknown>> => {
  const attributeKey = toStringValue(args.attributeKey);
  if (!attributeKey || args.attributeValue !== undefined || (Array.isArray(args.attributePatches) && args.attributePatches.length > 0)) return {};

  const paintKey = resolvePaintKeyByInstruction(existingPaint, args)
    || toStringValue(args.paintKey)
    || getEditablePaintColorKey(existingPaint);
  if (!paintKey || (typeof color === "string" && normalizeColorHex(color) && !paintKey.endsWith("-color"))) return {};

  const values = getAttributeValuesList(args.attributeValues).slice(0, 5);
  const outputs = await getAttributeStyleOutputs(args, paintKey, existingPaint, color, Math.max(values.length, 3));
  if (outputs.length === 0) return {};
  const fallbackOutput = args.fallbackOutput !== undefined
    ? args.fallbackOutput
    : existingPaint[paintKey] !== undefined && !Array.isArray(existingPaint[paintKey])
      ? existingPaint[paintKey]
      : outputs[outputs.length - 1];

  const attributeType = toStringValue(args.attributeType)?.toLowerCase();
  const numericStats = getAttributeNumericStats(args);
  if (attributeType === "number" && numericStats) {
    const mid = numericStats.min + ((numericStats.max - numericStats.min) / 2);
    return {
      [paintKey]: [
        "interpolate",
        ["linear"],
        ["get", attributeKey],
        numericStats.min,
        outputs[0],
        mid,
        outputs[Math.min(1, outputs.length - 1)],
        numericStats.max,
        outputs[Math.min(2, outputs.length - 1)]
      ]
    };
  }

  if (values.length === 0) return {};

  const matchExpression: unknown[] = ["match", ["get", attributeKey]];
  values.forEach((value, index) => {
    matchExpression.push(value, outputs[index % outputs.length]);
  });
  matchExpression.push(fallbackOutput);

  return {
    [paintKey]: matchExpression
  };
};

const combineLayerFilter = (existingFilter: unknown, nextFilter: unknown): unknown => {
  if (existingFilter === undefined || existingFilter === null) return nextFilter;
  return ["all", existingFilter, nextFilter];
};

const buildExcludeLayerAttributeValuesFilter = (
  attributeKey: string,
  values: unknown[]
): unknown | undefined => {
  const conditions = values.map((value) => ["!=", ["get", attributeKey], value]);
  if (conditions.length === 0) return undefined;
  return conditions.length === 1 ? conditions[0] : ["all", ...conditions];
};

const buildFallbackHeatmapPaint = (
  paint: Record<string, unknown>,
  paintKey: string,
  color: string
): Record<string, unknown> => {
  return Object.fromEntries(
    Object.entries({
      ...paint,
      [paintKey]: buildHeatmapColorRamp(color)
    }).map(([key, value]) => {
      if (!key.endsWith("-opacity") || typeof value !== "number") return [key, value];
      return [key, Math.max(0, Math.min(1, value / 4))];
    })
  );
};

const buildHeatmapAttributeStyleLayers = async (
  layer: Record<string, unknown>,
  args: EditMapStyleArgs,
  color: string | undefined
): Promise<Array<Record<string, unknown>> | undefined> => {
  if (toStringValue(layer.type) !== "heatmap") return undefined;
  const attributeKey = toStringValue(args.attributeKey);
  if (
    !attributeKey
    || args.attributeValue !== undefined
    || (Array.isArray(args.attributePatches) && args.attributePatches.length > 0)
  ) return undefined;

  const existingPaint = pickRecord(layer.paint);
  const paintKey = resolvePaintKeyByInstruction(existingPaint, args)
    || toStringValue(args.paintKey)
    || getEditablePaintColorKey(existingPaint);
  if (paintKey !== "heatmap-color") return undefined;

  const values = getAttributeValuesList(args.attributeValues).slice(0, 5);
  if (values.length === 0) return undefined;

  const palette = (await getAttributeStylePalette(existingPaint, color, paintKey))
    .filter((item, index, items) => items.indexOf(item) === index);
  if (palette.length === 0) return undefined;

  const baseId = toStringValue(layer.id)
    || [
      toStringValue(args.layerId),
      toStringValue(args.styleLayerId),
      toStringValue(layer.type)
    ].filter(Boolean).join("-")
    || "map-style-layer";
  const filteredLayers = values.map((value, index) => {
    const valueText = toStringValue(value) || String(index);
    const suffix = valueText.toLowerCase().replace(/[^a-z0-9]+/gi, "-").replace(/^-+|-+$/g, "") || String(index);
    return {
      ...layer,
      id: `${baseId}-${attributeKey}-${suffix}`,
      filter: combineLayerFilter(layer.filter, ["==", ["get", attributeKey], value]),
      paint: {
        ...existingPaint,
        [paintKey]: buildHeatmapColorRamp(palette[index % palette.length])
      }
    };
  });
  const fallbackFilter = buildExcludeLayerAttributeValuesFilter(attributeKey, values);
  if (!fallbackFilter) return filteredLayers;

  return [
    {
      ...layer,
      id: `${baseId}-${attributeKey}-fallback`,
      filter: combineLayerFilter(layer.filter, fallbackFilter),
      paint: buildFallbackHeatmapPaint(existingPaint, paintKey, palette[palette.length - 1])
    },
    ...filteredLayers
  ];
};

const getPaintPatchForLayerType = async (
  args: EditMapStyleArgs,
  color?: string,
  existingPaint: Record<string, unknown> = {}
): Promise<Record<string, unknown>> => {
  const rawExplicitPaint = pickRecord(args.paint);
  const explicitPaint = Object.fromEntries(
    Object.entries(rawExplicitPaint).filter(([key]) => existingPaint[key] !== undefined)
  );
  const attributeStopOutput = color ?? normalizeStylePropertyValue(args.value);
  const attributeStopPatch = buildAttributeStopPaintPatch(existingPaint, args, attributeStopOutput);
  const attributeRampOutputs = [
    ...getExplicitAttributeOutputs(args.outputs),
    ...(color ? [color] : [])
  ];
  const attributeRampPatch = buildAttributeRampOutputPatch(existingPaint, args, attributeRampOutputs);
  const attributePaintPatch = Object.keys(attributeRampPatch).length > 0
    ? {}
    : await buildAttributePaintPatch(args, color, existingPaint);
  const isAttributeValueEdit = Boolean(
    toStringValue(args.attributeKey)
    && (
      args.attributeValue !== undefined
      || (Array.isArray(args.attributePatches) && args.attributePatches.length > 0)
    )
  );
  if (isAttributeValueEdit) {
    return attributeStopPatch;
  }
  const isAttributeStyleRequest = Boolean(
    toStringValue(args.attributeKey)
    && args.attributeValue === undefined
    && (!Array.isArray(args.attributePatches) || args.attributePatches.length === 0)
  );
  if (
    isAttributeStyleRequest
    && Object.keys(attributeRampPatch).length === 0
    && Object.keys(attributePaintPatch).length === 0
  ) {
    return {};
  }
  const explicitPaintColorPatch = buildExplicitPaintColorPatch(existingPaint, args, color);
  const explicitGenericPaintPatch = buildExplicitGenericPaintPatch(existingPaint, args, color);
  const colorPatch = Object.keys(attributeStopPatch).length > 0
    || Object.keys(attributeRampPatch).length > 0
    || Object.keys(attributePaintPatch).length > 0
    || Object.keys(explicitPaintColorPatch).length > 0
    || Object.keys(explicitGenericPaintPatch).length > 0
    ? {}
    : buildColorPatchFromPaint(existingPaint, color);

  return {
    ...explicitPaint,
    ...attributeStopPatch,
    ...attributeRampPatch,
      ...attributePaintPatch,
      ...explicitPaintColorPatch,
      ...explicitGenericPaintPatch,
      ...colorPatch
  };
};

export const handleEditMapStyleTool = async (
  aiArgs: EditMapStyleArgs,
  currentMapStyle?: unknown
) => {
  const currentStyle = pickRecord(currentMapStyle);
  const currentLayers = Array.isArray(currentStyle.layers) ? currentStyle.layers : [];
  if (Object.keys(currentStyle).length === 0 || currentLayers.length === 0) {
    return {
      success: false,
      event: "map_style",
      message: "No recent map_style is available to edit. Please select or display a layer first."
    };
  }

  aiArgs = normalizeStylePropertyTargetArgs(aiArgs, currentLayers);

  const instruction = getEditInstruction(aiArgs);
  const operation = resolveEditMapStyleOperation(aiArgs);
  const isFilterOperation = operation === "add_filter";
  const requestedColorValue = normalizeColorHex(aiArgs.colorValue) || normalizeColorHex(aiArgs.value);
  const hasAttributePatches = Array.isArray(aiArgs.attributePatches) && aiArgs.attributePatches.length > 0;
  let matchedColorKeys: string[] = [];
  let resolvedColor = requestedColorValue;
  if (!isFilterOperation) {
    let catalog = await handleStyleCatalogTool();
    let colors = catalog.success && Array.isArray(catalog.colors) ? catalog.colors : [];
    matchedColorKeys = [
      ...toStringList(aiArgs.colorKey),
      ...(toStringValue(aiArgs.paintKey)?.endsWith("-color") ? toStringList(aiArgs.value) : []),
      ...resolveColorKeysFromInstruction(instruction, colors)
    ].filter((key, index, keys) => keys.indexOf(key) === index);
    if (!requestedColorValue && matchedColorKeys.length > 1 && !hasAttributePatches) {
      return {
        success: false,
        event: "map_style",
        message: "Multiple colors require attribute value patches or separate style edit requests."
      };
    }
    resolvedColor = requestedColorValue || resolveCatalogColor(
      matchedColorKeys.length === 1 ? matchedColorKeys[0] : undefined,
      colors
    );

    if (!resolvedColor && matchedColorKeys.length === 1) {
      catalog = await refreshStyleCatalogTool();
      colors = catalog.success && Array.isArray(catalog.colors) ? catalog.colors : [];
      resolvedColor = resolveCatalogColor(matchedColorKeys[0], colors);
    }

    if (!resolvedColor && matchedColorKeys.length === 1 && !hasAttributePatches) {
      return {
        success: false,
        event: "map_style",
        colorKey: matchedColorKeys[0],
        message: `Color ${matchedColorKeys[0]} was not found in the style catalog colors.`
      };
    }
  }

  const requestedLayerId = toStringValue(aiArgs.layerId) || toStringValue(pickRecord(aiArgs.params).layerId);
  const currentMapLayerId = toStringValue(currentStyle.layerId) || requestedLayerId;
  const requestedTarget = toStringValue(aiArgs.target)?.toLowerCase();
  const availableStyleLayerIds = new Set(
    currentLayers
      .map((layer) => toStringValue(pickRecord(layer).id))
      .filter((value): value is string => Boolean(value))
  );
  const availableLayerTargets = new Set(
    currentLayers
      .flatMap((layer) => {
        const layerRecord = pickRecord(layer);
        return [
          toStringValue(layerRecord.id)?.toLowerCase(),
          toStringValue(layerRecord.type)?.toLowerCase()
        ];
      })
      .filter((value): value is string => Boolean(value))
  );
  const target = requestedTarget && availableLayerTargets.has(requestedTarget)
    ? requestedTarget
    : undefined;
  const officialStyleSpec = isFilterOperation ? undefined : await loadMapLibreStyleSpec();
  let matchedFilterLayer = false;

  const patchLayer = async (layer: unknown): Promise<Record<string, unknown> | Array<Record<string, unknown>>> => {
    const layerRecord = pickRecord(layer);
    const layerType = toStringValue(layerRecord.type) || "";
    const requestedStyleLayerId = toStringValue(aiArgs.styleLayerId);
    const styleLayerId = requestedStyleLayerId && availableStyleLayerIds.has(requestedStyleLayerId)
      ? requestedStyleLayerId
      : undefined;
    const targetMatches = styleLayerId
      ? styleLayerId === toStringValue(layerRecord.id)
      : !target
        || target === layerType.toLowerCase()
        || target === toStringValue(layerRecord.id)?.toLowerCase();

    if (!targetMatches) return layerRecord;

    if (operation === "add_filter") {
      matchedFilterLayer = true;
      return applyFilterOperation(layerRecord, aiArgs, operation);
    }

    if (operation === "add_property" || operation === "remove_property") {
      return applyStylePropertyOperation(layerRecord, aiArgs, operation, resolvedColor, officialStyleSpec);
    }

    const heatmapAttributeLayers = await buildHeatmapAttributeStyleLayers(layerRecord, aiArgs, resolvedColor);
    if (heatmapAttributeLayers) return heatmapAttributeLayers;

    const paintPatch = await getPaintPatchForLayerType(aiArgs, resolvedColor, pickRecord(layerRecord.paint));
    const layoutPatch = pickRecord(aiArgs.layout);

    return {
      ...layerRecord,
      ...(Object.keys(layoutPatch).length > 0 ? { layout: { ...pickRecord(layerRecord.layout), ...layoutPatch } } : {}),
      ...(Object.keys(paintPatch).length > 0 ? { paint: { ...pickRecord(layerRecord.paint), ...paintPatch } } : {})
    };
  };
  const patchedLayers = await Promise.all(currentLayers.map((layer) => {
    const layerRecord = pickRecord(layer);
    const layerIdMatches = !requestedLayerId
      || requestedLayerId === currentMapLayerId
      || requestedLayerId === toStringValue(layerRecord.id);

    return layerIdMatches ? patchLayer(layerRecord) : layerRecord;
  }));
  const layers = patchedLayers.flatMap((layer) => Array.isArray(layer) ? layer : [layer]);
  const styleChanged = JSON.stringify(layers) !== JSON.stringify(currentLayers);

  if (!styleChanged) {
    if (isFilterOperation && matchedFilterLayer && (aiArgs.filter !== undefined || Array.isArray(aiArgs.filterConditions))) {
      return {
        ...currentStyle,
        success: true,
        event: "map_style",
        layerId: currentMapLayerId,
        layers: currentLayers,
        styleInstruction: instruction
      };
    }
    const attributeKey = toStringValue(aiArgs.attributeKey);
    const attributeValues = getAttributeValuesList(aiArgs.attributeValues);
    if (attributeKey && aiArgs.attributeValue === undefined && attributeValues.length > 0) {
      return {
        ...currentStyle,
        success: true,
        event: "map_style",
        layerId: currentMapLayerId,
        layers: currentLayers,
        styleInstruction: instruction,
        attributeStyleKey: attributeKey,
        ...(toStringValue(aiArgs.attributeType) ? { attributeStyleType: toStringValue(aiArgs.attributeType) } : {})
      };
    }
    // Keep deterministic no-op errors separate from missing data. The chatbot
    // layer can retry only lookup/network failures, not invalid paint requests.
    return {
      ...currentStyle,
      success: false,
      event: "map_style",
      layerId: currentMapLayerId,
      message: attributeKey && aiArgs.attributeValue === undefined && attributeValues.length === 0
        ? `No values were available to build a style expression for attribute ${attributeKey}.`
        : operation.endsWith("_filter")
          ? "No matching map filter was changed for this request."
          : "No matching paint/layout property was changed for this style edit request."
    };
  }

  return {
    ...currentStyle,
    success: true,
    event: "map_style",
    layerId: currentMapLayerId,
    layers,
    styleInstruction: instruction,
    ...(toStringValue(aiArgs.attributeKey) ? { attributeStyleKey: toStringValue(aiArgs.attributeKey) } : {}),
    ...(toStringValue(aiArgs.attributeType) ? { attributeStyleType: toStringValue(aiArgs.attributeType) } : {}),
    ...(resolvedColor ? { appliedColor: resolvedColor } : {}),
    ...(matchedColorKeys.length === 1 ? { colorKey: matchedColorKeys[0] } : {})
  };
};

export const handleClearMapLayersTool = async (
  userId: string,
  conversationId: string,
  aiArgs: ClearMapLayersArgs
) => {
  try {
    const mode = toStringValue(aiArgs.mode);
    if (!mode) {
      return {
        success: false,
        event: "map_clear",
        message: "Map clear mode is required."
      };
    }

    const validModes = new Set(["selected", "all"]);
    if (!validModes.has(mode)) {
      return {
        success: false,
        event: "map_clear",
        message: `Unsupported map clear mode: ${mode}`
      };
    }

    if (mode === "all") {
      return {
        success: true,
        event: "map_clear",
        mode: "all"
      };
    }

    const requestedLayerIds = toUniqueStringList(aiArgs.layerIds, aiArgs.layerId);
    if (requestedLayerIds.length > 0) {
      return {
        success: true,
        event: "map_clear",
        mode: "selected",
        layerIds: requestedLayerIds,
        ...(requestedLayerIds.length === 1 ? { layerId: requestedLayerIds[0] } : {})
      };
    }

    const activeLayers = await getActiveMapLayersForConversation(userId, conversationId);
    const layers = requestedLayerIds.length > 0
      ? activeLayers.filter((item) => requestedLayerIds.includes(item.layerId))
      : activeLayers.slice(-1);

    if (layers.length === 0) {
      return {
        success: false,
        event: "map_clear",
        mode: "selected",
        message: "No active map layer matched the clear request."
      };
    }

    return {
      success: true,
      event: "map_clear",
      mode: "selected",
      layerIds: layers.map((layer) => layer.layerId),
      ...(layers.length === 1 ? { layerId: layers[0].layerId } : {})
    };
  } catch (error) {
    console.error("Clear Map Layers Tool Error:", error);
    return {
      success: false,
      event: "map_clear",
      message: "An error occurred while managing map layers."
    };
  }
};

const replaceTemplateVariables = (
  template: string,
  variables: Record<string, unknown>
): string => {
  return Object.entries(variables).reduce((result, [key, value]) => {
    if (value === undefined || value === null) return result;
    return result.replace(new RegExp(`{${key}}`, "g"), String(value));
  }, template);
};

const replaceTemplateValue = (value: unknown, variables: Record<string, unknown>): unknown => {
  if (typeof value === "string") return replaceTemplateVariables(value, variables);
  if (Array.isArray(value)) return value.map((item) => replaceTemplateValue(item, variables));
  if (isRecord(value)) {
    return Object.fromEntries(
      Object.entries(value).map(([key, item]) => [key, replaceTemplateValue(item, variables)])
    );
  }
  return value;
};

const hasUnresolvedTemplateValue = (value: unknown): boolean => {
  if (typeof value === "string") return /{[^}]+}/.test(value);
  if (Array.isArray(value)) return value.some(hasUnresolvedTemplateValue);
  if (isRecord(value)) return Object.values(value).some(hasUnresolvedTemplateValue);
  return false;
};

const renderQueryConfig = (
  value: unknown,
  variables: Record<string, unknown>
): Record<string, unknown> => {
  const rendered = pickRecord(replaceTemplateValue(value, variables));

  return Object.fromEntries(
    Object.entries(rendered).filter(([, item]) => {
      if (item === undefined || item === null || item === "") return false;
      return !hasUnresolvedTemplateValue(item);
    })
  );
};

const extractTemplateKeys = (...templates: string[]): string[] => {
  const keys = new Set<string>();
  const regex = /{([^}]+)}/g;

  for (const template of templates) {
    let match: RegExpExecArray | null;
    while ((match = regex.exec(template)) !== null) {
      if (match[1]?.trim()) keys.add(match[1].trim());
    }
  }

  return Array.from(keys);
};

const extractTemplateKeysFromValue = (value: unknown): string[] => {
  if (typeof value === "string") {
    return extractTemplateKeys(value);
  }

  if (Array.isArray(value)) {
    return Array.from(new Set(value.flatMap(extractTemplateKeysFromValue)));
  }

  if (isRecord(value)) {
    return Array.from(new Set(Object.values(value).flatMap(extractTemplateKeysFromValue)));
  }

  return [];
};

const extractTemplateKeysFromRenderedConfig = (configString: string): string[] => {
  try {
    return extractTemplateKeysFromValue(JSON.parse(configString));
  } catch {
    return extractTemplateKeys(configString);
  }
};

const decryptUserApiKey = (apiKey: ResolvedUserApiKey): string => {
  return decrypt(apiKey.encryptedKey, apiKey.iv);
};

const resolveUserMapApiKeys = async (
  userId: string,
  headerApiKey?: string
): Promise<ResolvedUserApiKey[]> => {
  const cleanHeaderApiKey = headerApiKey?.trim();

  if (cleanHeaderApiKey) {
    return prisma.user_apikey.findMany({
      where: {
        userId,
        keyHash: hashApiKey(cleanHeaderApiKey),
        isActive: true,
        deletedAt: null
      },
      select: {
        id: true,
        provider: true,
        keyName: true,
        encryptedKey: true,
        iv: true,
        hostId: true,
        mapconfig_hosts: {
          select: {
            id: true,
            provider: true,
            hostname: true,
            baseUrl: true,
            serviceConfig: true
          }
        }
      }
    }).then((keys) => keys.map(({ mapconfig_hosts, ...key }) => ({
      ...key,
      host: mapconfig_hosts
    })));
  }

  return prisma.user_apikey.findMany({
    where: {
      userId,
      isActive: true,
      deletedAt: null
    },
    select: {
      id: true,
      provider: true,
      keyName: true,
      encryptedKey: true,
      iv: true,
      hostId: true,
      mapconfig_hosts: {
        select: {
          id: true,
          provider: true,
          hostname: true,
          baseUrl: true,
          serviceConfig: true
        }
      }
    },
    orderBy: { createdAt: "desc" }
  }).then((keys) => keys.map(({ mapconfig_hosts, ...key }) => ({
    ...key,
    host: mapconfig_hosts
  })));
};

const buildTemplateVariables = (
  aiArgs: MapToolArgs,
  layerConfigTemplate: unknown,
  intentName: string,
  provider: string,
  apiKey: string
): Record<string, unknown> => {
  const configTemplate = pickRecord(layerConfigTemplate);
  const reservedKeys = new Set([
    "intentName",
    "provider",
    "params",
    "options",
    "selectedOptions",
    "variables"
  ]);

  const directArgs = Object.fromEntries(
    Object.entries(aiArgs).filter(([key]) => !reservedKeys.has(key))
  );

  return {
    ...pickRecord(configTemplate.defaults),
    ...pickRecord(configTemplate.params),
    ...directArgs,
    ...pickRecord(aiArgs.params),
    ...pickRecord(aiArgs.options),
    ...pickRecord(aiArgs.selectedOptions),
    ...pickRecord(aiArgs.variables),
    intentName,
    provider,
    apiKey
  };
};

const createOptionInfo = (key: string): MapOptionInfo => {
  return {
    key,
    required: true,
    source: "template"
  };
};

const FRONTEND_TILE_KEYS = new Set(["z", "x", "y"]);
const INTERNAL_TEMPLATE_KEYS = new Set(["apiKey", "intentName", "provider"]);
const OPTION_CONTAINER_KEYS = [
  "options",
  "choices",
  "mapOptions",
  "map_options",
  "paramsOptions",
  "parameters",
  "fields"
];
const CHOICE_ARRAY_KEYS = ["choices", "options", "values", "enum", "enums", "items"];
const CHOICE_VALUE_KEYS = ["value", "id", "key", "code", "name", "path"];
const CHOICE_LABEL_KEYS = ["label", "title", "name", "text", "description"];

const hasBlockingUnresolvedMapTemplate = (
  finalUrl: string,
  configString: string
): boolean => {
  const keys = [
    ...extractTemplateKeys(finalUrl),
    ...extractTemplateKeysFromRenderedConfig(configString)
  ];

  return keys.some((key) => !FRONTEND_TILE_KEYS.has(key));
};

const buildMissingOptionInfo = (
  config: { urlTemplate: string; layerConfigTemplate: unknown },
  variables: Record<string, unknown>
): MapOptionInfo[] => {
  const keys = [
    ...extractTemplateKeys(config.urlTemplate),
    ...extractTemplateKeysFromValue(config.layerConfigTemplate)
  ]
    .filter((key) => !FRONTEND_TILE_KEYS.has(key))
    .filter((key) => !INTERNAL_TEMPLATE_KEYS.has(key))
    .filter((key) => variables[key] === undefined || variables[key] === null || variables[key] === "");

  return keys.map(createOptionInfo);
};

const toCleanString = (value: unknown): string | undefined => {
  if (typeof value === "string" && value.trim()) return value.trim();
  if (typeof value === "number" || typeof value === "boolean") return String(value);
  return undefined;
};

const pickFirstString = (
  record: Record<string, unknown>,
  keys: string[]
): string | undefined => {
  for (const key of keys) {
    const value = toCleanString(record[key]);
    if (value) return value;
  }

  return undefined;
};

const normalizeChoice = (value: unknown): MapOptionChoice | undefined => {
  const primitiveValue = toCleanString(value);
  if (primitiveValue) {
    return {
      label: primitiveValue,
      value: primitiveValue
    };
  }

  if (!isRecord(value)) return undefined;

  const choiceValue = pickFirstString(value, CHOICE_VALUE_KEYS);
  if (!choiceValue) return undefined;

  const choiceLabel = pickFirstString(value, CHOICE_LABEL_KEYS) || choiceValue;
  const description = toCleanString(value.description);

  return {
    label: choiceLabel,
    value: choiceValue,
    ...(description && description !== choiceLabel ? { description } : {})
  };
};

const dedupeChoices = (choices: MapOptionChoice[]): MapOptionChoice[] => {
  const seen = new Set<string>();
  return choices.filter((choice) => {
    if (seen.has(choice.value)) return false;
    seen.add(choice.value);
    return true;
  });
};

const extractChoiceArray = (value: unknown): MapOptionChoice[] => {
  if (Array.isArray(value)) {
    return dedupeChoices(
      value
        .map(normalizeChoice)
        .filter((choice): choice is MapOptionChoice => Boolean(choice))
    );
  }

  if (!isRecord(value)) return [];

  for (const key of CHOICE_ARRAY_KEYS) {
    const choices = extractChoiceArray(value[key]);
    if (choices.length > 0) return choices;
  }

  return [];
};

const findOptionDefinition = (
  template: unknown,
  optionKey: string
): unknown => {
  if (!isRecord(template)) return undefined;

  if (template[optionKey] !== undefined) return template[optionKey];

  for (const containerKey of OPTION_CONTAINER_KEYS) {
    const container = template[containerKey];
    if (isRecord(container) && container[optionKey] !== undefined) {
      return container[optionKey];
    }
  }

  for (const value of Object.values(template)) {
    if (!isRecord(value)) continue;
    const match = findOptionDefinition(value, optionKey);
    if (match !== undefined) return match;
  }

  return undefined;
};

const buildOptionGroup = (
  key: string,
  layerConfigTemplate: unknown
): MapOptionInfo => {
  const definition = findOptionDefinition(layerConfigTemplate, key);
  const definitionRecord = pickRecord(definition);
  const label = pickFirstString(definitionRecord, ["label", "title", "name"]) || key;
  const description = pickFirstString(definitionRecord, ["description", "question", "help"]);
  const descriptions = pickRecord(definitionRecord.descriptions);
  const choices = extractChoiceArray(definition)
    .map((choice) => {
      const choiceDescription = toCleanString(descriptions[choice.value]);
      return {
        ...choice,
        ...(choiceDescription ? { description: choiceDescription } : {})
      };
    });

  return {
    key,
    required: definitionRecord.required !== false,
    source: "template",
    label,
    ...(description ? { description } : {}),
    ...(choices.length > 0 ? { choices } : {})
  };
};

const getTemplateOptionKeys = (config: {
  urlTemplate: string;
  layerConfigTemplate: unknown;
}): string[] => {
  if (isCollectionDetailConfig(config.layerConfigTemplate)) {
    return [getVectorTileOptionKey(config.layerConfigTemplate)];
  }

  return [
    ...extractTemplateKeys(config.urlTemplate),
    ...extractTemplateKeysFromValue(config.layerConfigTemplate)
  ]
    .filter((key) => !FRONTEND_TILE_KEYS.has(key))
    .filter((key) => !INTERNAL_TEMPLATE_KEYS.has(key))
    .filter((key, index, keys) => keys.indexOf(key) === index);
};

const buildTemplateOptionGroups = (config: {
  urlTemplate: string;
  layerConfigTemplate: unknown;
}): MapOptionInfo[] => {
  return getTemplateOptionKeys(config)
    .map((key) => buildOptionGroup(key, config.layerConfigTemplate));
};

const getSelectedTemplateValues = (
  aiArgs: MapToolArgs,
  config: { urlTemplate: string; layerConfigTemplate: unknown },
  intentName: string,
  provider: string,
  apiKey = ""
) => {
  const variables = buildTemplateVariables(
    aiArgs,
    config.layerConfigTemplate,
    intentName,
    provider,
    apiKey
  );

  return Object.fromEntries(
    getTemplateOptionKeys(config)
      .map((key) => [key, variables[key]])
      .filter(([, value]) => value !== undefined && value !== null && value !== "")
  );
};

const validateTemplateSelections = (
  optionGroups: MapOptionInfo[],
  variables: Record<string, unknown>
) => {
  const invalidKeys: string[] = [];

  for (const option of optionGroups) {
    const value = variables[option.key];
    if (value === undefined || value === null || value === "" || !option.choices?.length) {
      continue;
    }

    const selectedValue = String(value);
    const allowedValues = new Set(option.choices.map((choice) => choice.value));
    if (!allowedValues.has(selectedValue)) {
      invalidKeys.push(option.key);
    }
  }

  return invalidKeys;
};

const LAYER_CONFIG_METADATA_KEYS = new Set([
  "parameters_metadata",
  "parameter_metadata",
  "options",
  "choices",
  "mapOptions",
  "map_options",
  "paramsOptions",
  "parameters",
  "fields",
  "defaults",
  "params"
]);

const stripLayerConfigMetadata = (
  config: Record<string, unknown>
): Record<string, unknown> => {
  return Object.fromEntries(
    Object.entries(config).filter(([key]) => !LAYER_CONFIG_METADATA_KEYS.has(key))
  );
};

const parseDayRange = (value: unknown): 1 | 3 | 7 | 30 | undefined => {
  const rawValue = toCleanString(value);
  if (!rawValue) return undefined;

  const match = rawValue.match(/\d+/);
  const days = match ? Number(match[0]) : Number(rawValue);

  return days === 1 || days === 3 || days === 7 || days === 30
    ? days
    : undefined;
};

const createFallbackLayerName = (
  hazard: string,
  dayPath: string,
  type: string
) => `${hazard}_${dayPath}_${type}`;

const resolveLayerCatalogSummary = (
  variables: Record<string, unknown>
) => {
  const hazard = toCleanString(variables.hazard);
  const dayPath = toCleanString(variables.dayPath || variables.days);
  const type = toCleanString(variables.type);
  const days = parseDayRange(dayPath);

  if (!hazard || !dayPath || !type || !days) {
    return {};
  }

  return {
    hazard,
    dayPath,
    days,
    type,
    layerName: createFallbackLayerName(hazard, dayPath, type)
  };
};

const createAccessOption = (
  key: "intentName" | "provider",
  choices: MapOptionChoice[]
): MapOptionInfo => ({
  key,
  required: true,
  source: "map_access",
  label: key,
  choices
});

const filterConfigsByProviders = <T extends { provider: string }>(
  configs: T[],
  allowedProviders: string[]
): T[] => {
  return configs.filter((config) => providerAllowed(allowedProviders, config.provider));
};

const findConfigByIntentProvider = <T extends { intentName: string; provider: string }>(
  configs: T[],
  intentName: string,
  provider: string
): T | undefined => {
  return configs.find((config) => (
    config.intentName === intentName && sameProvider(config.provider, provider)
  ));
};

const normalizeConfigMatchTerm = (value: string): string => {
  return value.toLowerCase().replace(/[\s()[\]{}"'`.,:;|/_-]+/g, "");
};

const splitConfigMatchTokens = (value: string): string[] => {
  return value
    .toLowerCase()
    .split(/[\s()[\]{}"'`.,:;|/_-]+/g)
    .map((token) => token.trim())
    .filter((token) => token.length >= 3);
};

const collectConfigStringValues = (value: unknown): string[] => {
  const cleanValue = toCleanString(value);
  if (cleanValue) return [cleanValue];

  if (Array.isArray(value)) {
    return value.flatMap(collectConfigStringValues);
  }

  return [];
};

const collectConfigMatchTerms = (config: {
  intentName: string;
  provider?: string;
  urlTemplate?: string;
  layerConfigTemplate?: unknown;
}): string[] => {
  const template = pickRecord(config.layerConfigTemplate);
  const collectionQuery = pickRecord(template.collectionQuery);
  const itemTypeTerms = collectConfigStringValues(collectionQuery.itemType)
    .filter((value) => normalizeConfigMatchTerm(value).length > 4);

  const rawTerms = [
    config.intentName,
    config.provider,
    config.urlTemplate,
    ...collectConfigStringValues(template.type),
    ...collectConfigStringValues(template.handler),
    ...itemTypeTerms,
    ...collectConfigStringValues(template.keywords),
    ...collectConfigStringValues(template.aliases),
    ...collectConfigStringValues(template.matchTerms),
    ...collectConfigStringValues(template.searchTerms),
    ...collectConfigStringValues(template.intentKeywords)
  ];

  return Array.from(new Set(rawTerms.flatMap((value) => {
    const cleanValue = toCleanString(value);
    if (!cleanValue) return [];
    const tokens = splitConfigMatchTokens(cleanValue);
    const singularTokens = tokens
      .filter((token) => token.endsWith("s") && token.length > 4)
      .map((token) => token.slice(0, -1));
    return [cleanValue, ...tokens, ...singularTokens];
  })))
    .map(normalizeConfigMatchTerm)
    .filter((term) => term.length >= 3);
};

const filterConfigsByQuery = <T extends {
  intentName: string;
  provider?: string;
  layerConfigTemplate?: unknown;
}>(
  configs: T[],
  query?: string
): T[] => {
  const normalizedQuery = query ? normalizeConfigMatchTerm(query) : "";
  if (!normalizedQuery) return configs;

  const scoredConfigs = configs.map((config) => {
    const terms = collectConfigMatchTerms(config);
    const score = terms.reduce((total, term) => {
      if (!normalizedQuery.includes(term)) return total;
      return total + term.length;
    }, 0);

    return { config, score };
  }).filter((item) => item.score > 0);

  if (scoredConfigs.length === 0) return configs;

  const maxScore = Math.max(...scoredConfigs.map((item) => item.score));
  return scoredConfigs
    .filter((item) => item.score === maxScore)
    .map((item) => item.config);
};

const getExplicitConfigMatchesByQuery = <T extends {
  intentName: string;
  provider?: string;
  layerConfigTemplate?: unknown;
}>(
  configs: T[],
  query?: string
): T[] => {
  const normalizedQuery = query ? normalizeConfigMatchTerm(query) : "";
  if (!normalizedQuery) return [];

  const scoredConfigs = configs.map((config) => {
    const terms = collectConfigMatchTerms(config);
    const score = terms.reduce((total, term) => {
      if (!normalizedQuery.includes(term)) return total;
      return total + term.length;
    }, 0);

    return { config, score };
  }).filter((item) => item.score > 0);

  if (scoredConfigs.length === 0) return [];

  const maxScore = Math.max(...scoredConfigs.map((item) => item.score));
  return scoredConfigs
    .filter((item) => item.score === maxScore)
    .map((item) => item.config);
};

const buildMapOptionQuestion = (options: MapOptionInfo[]): string | undefined => {
  const keys = options
    .filter((option) => option.required)
    .map((option) => option.key);

  if (keys.length === 0) return undefined;

  const hasHazard = keys.includes("hazard");
  const hasDayPath = keys.includes("dayPath") || keys.includes("days");
  const hasType = keys.includes("type");

  if (hasHazard && hasDayPath && hasType) {
    return "Which data type, date range, and map format do you want?";
  }

  const questionParts = keys.map((key) => {
    if (key === "hazard") return "which data type";
    if (key === "dayPath" || key === "days") return "how many days back";
    if (key === "type") return "which map format";
    if (key === "layerId") return "which layer";
    if (key === "intentName") return "which map type";
    if (key === "provider") return "which provider";
    return `the value for ${key}`;
  });

  return `Please choose ${questionParts.join(" and ")}.`;
};

type MapQuestionCode =
  | "select_vallaris_style"
  | "select_vallaris_type";

const buildMapQuestion = (
  code: MapQuestionCode,
  context: Record<string, string | undefined> = {}
): string => {
  if (code === "select_vallaris_style") {
    return context.query
      ? `I found multiple styles that may match "${context.query}". Please choose one first.`
      : "Which VALLARIS style do you want to use?";
  }

  return "Which map format do you want?";
};

const buildMapOptionDescription = (
  code: MapQuestionCode,
  context: Record<string, string | undefined> = {}
): string => {
  if (code === "select_vallaris_style") {
    return context.query
      ? `Choose the style that matches "${context.query}"`
      : "Choose the style to use";
  }

  return context.styleTitle
    ? `Choose the type for ${context.styleTitle}`
    : "Choose the type for the selected style";
};

type VallarisLink = {
  href: string;
  rel?: string;
  type?: string;
  title?: string;
  templated?: boolean;
};

type VallarisStyle = {
  id: string;
  title?: string;
  description?: string;
  links: VallarisLink[];
  raw: Record<string, unknown>;
};

type EnrichedVallarisStyle = VallarisStyle & {
  searchableText: string;
  metadata?: unknown;
  stylesheet?: unknown;
};

type VallarisStyleMatch = {
  style: EnrichedVallarisStyle;
  score: number;
  confidence: number;
};

const VALLARIS_ENRICH_LIMIT = 40;
const VALLARIS_MATCH_THRESHOLD = 0.28;
const VALLARIS_STYLE_CHOICE_LIMIT = 8;
const toStringValue = (value: unknown): string | undefined => {
  if (typeof value === "string" && value.trim()) return value.trim();
  if (typeof value === "number" || typeof value === "boolean") return String(value);
  return undefined;
};

const getMapQuery = (aiArgs: MapToolArgs): string | undefined => {
  const containers = [
    aiArgs,
    pickRecord(aiArgs.params),
    pickRecord(aiArgs.options),
    pickRecord(aiArgs.selectedOptions),
    pickRecord(aiArgs.variables)
  ];
  const keys = [
    "query",
    "message",
    "search",
    "keyword",
    "keywords",
    "topic",
    "request",
    "intent",
    "hazard",
    "layer",
    "layerName",
    "style",
    "styleTitle"
  ];

  for (const container of containers) {
    for (const key of keys) {
      const value = toStringValue(container[key]);
      if (value) return value;
    }
  }

  return undefined;
};

const getSelectedVallarisStyleId = (aiArgs: MapToolArgs): string | undefined => {
  const containers = [
    aiArgs,
    pickRecord(aiArgs.params),
    pickRecord(aiArgs.options),
    pickRecord(aiArgs.selectedOptions),
    pickRecord(aiArgs.variables)
  ];

  for (const container of containers) {
    const value = toStringValue(container.styleId)
      || toStringValue(container.style_id)
      || toStringValue(container.id);
    if (value) return value;
  }

  return undefined;
};

const getSelectedVallarisType = (aiArgs: MapToolArgs): string | undefined => {
  const containers = [
    aiArgs,
    pickRecord(aiArgs.params),
    pickRecord(aiArgs.options),
    pickRecord(aiArgs.selectedOptions),
    pickRecord(aiArgs.variables)
  ];

  for (const container of containers) {
    const value = toStringValue(container.type)
      || toStringValue(container.mapType)
      || toStringValue(container.map_option)
      || toStringValue(container.mapOption);
    if (value) return value.toLowerCase();
  }

  return undefined;
};

const inferVallarisTypeFromQuery = (
  query: string | undefined,
  choices: MapOptionChoice[]
): string | undefined => {
  if (!query?.trim()) return undefined;

  const queryTokens = new Set(tokenizeMatchText(query));
  const matches = choices.filter((choice) => {
    const candidates = [choice.value, choice.type, choice.label]
      .map((value) => value?.trim().toLowerCase())
      .filter((value): value is string => Boolean(value));

    return candidates.some((candidate) => queryTokens.has(candidate));
  });

  const uniqueMatches = new Set(matches.map((choice) => choice.value));
  return uniqueMatches.size === 1 ? matches[0]?.value : undefined;
};

const joinProviderUrl = (baseUrl: string, urlTemplate: string): string => {
  const cleanTemplate = urlTemplate.trim();
  if (/^https?:\/\//i.test(cleanTemplate)) return cleanTemplate;

  return [
    baseUrl.replace(/\/+$/g, ""),
    cleanTemplate.replace(/^\/+/g, "")
  ].filter(Boolean).join("/");
};

const appendApiKeyQuery = (url: string, apiKey: string): string => {
  if (!apiKey || /[?&](api_key|apikey|apiKey)=/i.test(url)) return url;
  const separator = url.includes("?") ? "&" : "?";
  return `${url}${separator}api_key=${encodeURIComponent(apiKey)}`;
};

const createPublicVallarisMapUrl = (url: string): string => {
  try {
    const parsedUrl = new URL(url);
    parsedUrl.search = "";
    return parsedUrl.toString();
  } catch {
    return url.split("?")[0] || url;
  }
};

const buildVallarisCatalogUrl = (
  config: { baseUrl: string; urlTemplate: string },
  apiKey: string
): string => {
  return appendApiKeyQuery(
    joinProviderUrl(config.baseUrl, config.urlTemplate),
    apiKey
  );
};

const VECTOR_TILE_CHOICE_LIMIT = 15;
const VECTOR_TILE_GEOMETRY_INFER_TIMEOUT_MS = 2000;
const VECTOR_TILE_GEOMETRY_CACHE_TTL_MS = 60 * 60 * 1000;

const vectorTileGeometryCache = new Map<string, {
  expiresAt: number;
  geometryType: string;
}>();

const isCollectionDetailConfig = (layerConfigTemplate: unknown): boolean => {
  const template = pickRecord(layerConfigTemplate);
  const handler = toStringValue(template.handler)?.toLowerCase();
  if (handler === "collection_detail") return true;

  return Boolean(template.collectionQuery && toStringValue(template.detailUrlTemplate));
};

const getVectorTileOptionKey = (layerConfigTemplate: unknown): string => {
  return toStringValue(pickRecord(layerConfigTemplate).optionKey) || "layerId";
};

const getCollectionDetailType = (layerConfigTemplate: unknown): string => {
  return toStringValue(pickRecord(layerConfigTemplate).type) || "collection_detail";
};

const getPmtilesUrlTemplate = (layerConfigTemplate: unknown): string | undefined => {
  return toStringValue(pickRecord(layerConfigTemplate).pmtilesUrlTemplate);
};

const getVectorTileLayerId = (aiArgs: MapToolArgs, optionKey: string): string | undefined => {
  const containers = [
    aiArgs,
    pickRecord(aiArgs.params),
    pickRecord(aiArgs.options),
    pickRecord(aiArgs.selectedOptions),
    pickRecord(aiArgs.variables)
  ];

  for (const container of containers) {
    const value = toStringValue(container[optionKey]) || toStringValue(container.layerId);
    if (value) return value;
  }

  return undefined;
};

const getVectorTileDatasetIdFromArgs = (aiArgs: MapToolArgs): string | undefined => {
  const containers = getMapArgsContainers(aiArgs);

  for (const container of containers) {
    const value = toStringValue(container.datasetId) || toStringValue(container.dataset_id);
    if (value) return value;
  }

  return undefined;
};

const getVectorTileCollectionRequestParams = (
  aiArgs: MapToolArgs,
  optionKey: string
): Record<string, unknown> => {
  const ignoredKeys = new Set([
    optionKey,
    "layerId",
    "id",
    "pagination",
    "limit",
    "offset",
    "nextOffset",
    "currentOffset",
    "action",
    "optionsOnly"
  ]);
  const params = {
    ...pickRecord(aiArgs.params),
    ...pickRecord(aiArgs.options),
    ...pickRecord(aiArgs.variables)
  };

  return Object.fromEntries(
    Object.entries(params).filter(([key, value]) => {
      return !ignoredKeys.has(key)
        && toStringValue(value) !== undefined
        && !isRecord(value)
        && !Array.isArray(value);
    })
  );
};
// 29/05/2026
const buildMapOptionUrl = (
  baseUrl: string,
  template: string,
  apiKey: string,
  params: Record<string, unknown> = {}
): string => {
  const cleanBaseUrl = baseUrl.trim();
  const cleanTemplate = template.trim();
  if (!cleanBaseUrl || !/^https?:\/\//i.test(cleanBaseUrl)) {
    throw new Error(`Invalid map host baseUrl: ${cleanBaseUrl || "(empty)"}`);
  }
  if (!cleanTemplate) {
    throw new Error("Invalid map urlTemplate: (empty)");
  }

  const usedKeys = new Set<string>();
  const renderedTemplate = Object.entries(params).reduce((url, [key, value]) => {
    const cleanValue = toStringValue(value);
    if (!cleanValue || !url.includes(`{${key}}`)) return url;
    usedKeys.add(key);
    return url.replace(new RegExp(`{${key}}`, "g"), encodeURIComponent(cleanValue));
  }, cleanTemplate);
  const base = joinProviderUrl(cleanBaseUrl, renderedTemplate);
  const query = Object.entries(params)
    .filter(([key, value]) => !usedKeys.has(key) && toStringValue(value))
    .map(([key, value]) => `${encodeURIComponent(key)}=${encodeURIComponent(toStringValue(value) || "")}`)
    .join("&");
  return appendApiKeyQuery(query ? `${base}${base.includes("?") ? "&" : "?"}${query}` : base, apiKey);
};

const buildProviderUrl = (
  baseUrl: string,
  template: string,
  params: Record<string, unknown> = {}
): string => {
  const cleanBaseUrl = baseUrl.trim();
  const cleanTemplate = template.trim();
  if (!cleanBaseUrl || !/^https?:\/\//i.test(cleanBaseUrl)) {
    throw new Error(`Invalid map host baseUrl: ${cleanBaseUrl || "(empty)"}`);
  }
  if (!cleanTemplate) {
    throw new Error("Invalid map urlTemplate: (empty)");
  }

  const usedKeys = new Set<string>();
  const renderedTemplate = Object.entries(params).reduce((url, [key, value]) => {
    const cleanValue = toStringValue(value);
    if (!cleanValue || !url.includes(`{${key}}`)) return url;
    usedKeys.add(key);
    return url.replace(new RegExp(`{${key}}`, "g"), encodeURIComponent(cleanValue));
  }, cleanTemplate);
  const base = joinProviderUrl(cleanBaseUrl, renderedTemplate);
  const query = Object.entries(params)
    .filter(([key, value]) => !usedKeys.has(key) && toStringValue(value))
    .map(([key, value]) => `${encodeURIComponent(key)}=${encodeURIComponent(toStringValue(value) || "")}`)
    .join("&");

  return query ? `${base}${base.includes("?") ? "&" : "?"}${query}` : base;
};

const createVectorTilePublicUrl = (url: string): string => {
  return createPublicVallarisMapUrl(url);
};

type VectorTileCollection = {
  id: string;
  title?: string;
  description?: string;
  geometryType?: string;
  sourceLayer?: string;
  datasetId?: string;
};

type MapAttributeField = {
  type: string;
};

type MapOptionPaginationRequest = {
  enabled: boolean;
  limit?: number;
  offset?: number;
};

type MapOptionPaginationResult = {
  public?: {
    numberMatched?: number;
    numberReturned: number;
    hasNext: boolean;
    hasBack: boolean;
  };
  state?: {
    enabled: true;
    limit: number;
    offset: number;
    hasNext: boolean;
    nextOffset?: number;
  };
};

const normalizeGeometryType = (value: unknown): string | undefined => {
  const geometry = toStringValue(value)?.toLowerCase();
  if (!geometry) return undefined;
  if (geometry.includes("point")) return "point";
  if (geometry.includes("line")) return "line";
  if (geometry.includes("polygon")) return "polygon";
  if (geometry.includes("raster") || geometry.includes("image") || geometry.includes("coverage")) return "raster";
  return geometry;
};

const normalizeAttributeFieldType = (value: unknown): string | undefined => {
  return toStringValue(value);
};

const normalizeAttributeFields = (fields: unknown): Record<string, MapAttributeField> | undefined => {
  if (!fields) return undefined;

  const entries = new Map<string, MapAttributeField>();
  const addField = (name: unknown, type: unknown) => {
    const fieldName = toStringValue(name);
    const fieldType = normalizeAttributeFieldType(type);
    if (!fieldName || !fieldType) return;
    entries.set(fieldName, { type: fieldType });
  };

  if (isRecord(fields)) {
    for (const [fieldName, fieldDefinition] of Object.entries(fields)) {
      const definition = pickRecord(fieldDefinition);
      addField(fieldName, toStringValue(fieldDefinition) || definition.type || definition.dataType || definition.fieldType);
    }
  }

  if (Array.isArray(fields)) {
    for (const field of fields) {
      if (!isRecord(field)) continue;
      addField(
        field.name || field.key || field.id || field.field,
        field.type || field.dataType || field.fieldType
      );
    }
  }

  return entries.size > 0 ? Object.fromEntries(entries) : undefined;
};

const findAttributeFieldsInPayload = (payload: unknown, depth = 0): Record<string, MapAttributeField> | undefined => {
  if (depth > 5 || !payload) return undefined;

  if (Array.isArray(payload)) {
    for (const item of payload) {
      const fields = findAttributeFieldsInPayload(item, depth + 1);
      if (fields) return fields;
    }
    return undefined;
  }

  if (!isRecord(payload)) return undefined;

  const attributes = pickRecord(payload.attributes);
  const directFields = normalizeAttributeFields(attributes.fields)
    || normalizeAttributeFields(payload.fields)
    || normalizeAttributeFields(pickRecord(payload.schema).fields)
    || normalizeAttributeFields(pickRecord(payload.metadata).fields);
  if (directFields) return directFields;

  for (const value of Object.values(payload)) {
    const fields = findAttributeFieldsInPayload(value, depth + 1);
    if (fields) return fields;
  }

  return undefined;
};

const getAttributeFieldsFromTileRecord = (
  tileRecord: Record<string, unknown>,
  tilePayload?: unknown
): Record<string, MapAttributeField> | undefined => {
  const attributes = pickRecord(tileRecord.attributes);
  return normalizeAttributeFields(attributes.fields)
    || normalizeAttributeFields(tileRecord.fields)
    || normalizeAttributeFields(pickRecord(tileRecord.schema).fields)
    || normalizeAttributeFields(pickRecord(tileRecord.metadata).fields)
    || findAttributeFieldsInPayload(tilePayload)
    || findAttributeFieldsInPayload(tileRecord);
};

const inferGeometryTypeFromAttributeFields = (
  fields: Record<string, MapAttributeField> | undefined
): string | undefined => {
  if (!fields) return undefined;

  const fieldNames = new Set(Object.keys(fields).map((name) => name.trim().toLowerCase()));
  const hasLatitude = fieldNames.has("latitude") || fieldNames.has("lat");
  const hasLongitude = fieldNames.has("longitude") || fieldNames.has("lon") || fieldNames.has("lng");

  return hasLatitude && hasLongitude ? "point" : undefined;
};

const getVectorTileDatasetId = (
  tileRecord: Record<string, unknown>,
  tilePayload?: unknown
): string | undefined => {
  const candidates = [
    pickRecord(pickRecord(tileRecord.tileConfig).data_filter).dataset_id,
    pickRecord(pickRecord(tileRecord.tileConfig).dataFilter).dataset_id,
    pickRecord(pickRecord(tileRecord.tile_config).data_filter).dataset_id,
    pickRecord(tileRecord.data_filter).dataset_id,
    pickRecord(tileRecord.dataFilter).dataset_id,
    tileRecord.dataset_id,
    tileRecord.datasetId,
    pickRecord(pickRecord(pickRecord(tilePayload).tileConfig).data_filter).dataset_id,
    pickRecord(pickRecord(pickRecord(tilePayload).tile_config).data_filter).dataset_id
  ];

  return candidates.map(toStringValue).find(Boolean);
};

const getDirectGeometryType = (value: unknown): string | undefined => {
  const record = pickRecord(value);
  return normalizeGeometryType(record.geometryType)
    || normalizeGeometryType(record.geometry)
    || normalizeGeometryType(record.geomType)
    || normalizeGeometryType(record.geom_type);
};

const findSourceLayer = (value: unknown, fallback?: string): string | undefined => {
  const record = pickRecord(value);
  const direct = toStringValue(record.sourceLayer)
    || toStringValue(record.source_layer)
    || toStringValue(record.layerName)
    || toStringValue(record.name);
  if (direct) return direct;

  const vectorLayers = Array.isArray(record.vector_layers)
    ? record.vector_layers
    : Array.isArray(record.vectorLayers)
      ? record.vectorLayers
      : undefined;
  const firstVectorLayer = vectorLayers?.find(isRecord);
  return toStringValue(firstVectorLayer?.id) || fallback;
};

const extractVectorTileCollections = (payload: unknown): VectorTileCollection[] => {
  const source = Array.isArray(payload)
    ? payload
    : isRecord(payload)
      ? (["collections", "features", "items", "data", "results"]
        .map((key) => payload[key])
        .find(Array.isArray) || [payload])
      : [];

  return source
    .map((item): VectorTileCollection | undefined => {
      if (!isRecord(item)) return undefined;
      const properties = pickRecord(item.properties);
      const record: Record<string, unknown> = { ...item, ...properties, id: item.id ?? properties.id };
      const id = toStringValue(record.id) || toStringValue(record.layerId);
      if (!id) return undefined;

      return {
        id,
        title: toStringValue(record.title) || toStringValue(record.name) || id,
        description: toStringValue(record.description),
        geometryType: getDirectGeometryType(record),
        sourceLayer: findSourceLayer(record, id),
        datasetId: getVectorTileDatasetId(record, item)
      };
    })
    .filter((item): item is VectorTileCollection => Boolean(item));
};

const getMapArgsContainers = (aiArgs: MapToolArgs): Record<string, unknown>[] => {
  return [
    aiArgs,
    pickRecord(aiArgs.params),
    pickRecord(aiArgs.options),
    pickRecord(aiArgs.selectedOptions),
    pickRecord(aiArgs.variables)
  ];
};

const getNestedContainers = (
  containers: Record<string, unknown>[],
  key: string
): Record<string, unknown>[] => {
  return containers.flatMap((container) => [
    container,
    pickRecord(container[key])
  ]);
};

const firstNumberFromContainers = (
  containers: Record<string, unknown>[],
  keys: string[],
  validate: (value: number) => boolean
): number | undefined => {
  for (const container of containers) {
    for (const key of keys) {
      const value = toNumberValue(container[key]);
      if (value !== undefined && validate(value)) return Math.floor(value);
    }
  }

  return undefined;
};

const getMapOptionPaginationRequest = (
  aiArgs: MapToolArgs,
  template: Record<string, unknown>,
  fallbackLimit: number
): MapOptionPaginationRequest => {
  const config = pickRecord(template.pagination);
  const enabled = config.enabled === true || toNumberValue(config.limit) !== undefined;
  if (!enabled) return { enabled: false };

  const containers = getNestedContainers(getMapArgsContainers(aiArgs), "pagination");
  const configuredLimit = toNumberValue(config.limit);
  const requestedLimit = firstNumberFromContainers(containers, ["limit"], (value) => value > 0);
  const requestedOffset = firstNumberFromContainers(containers, ["offset"], (value) => value >= 0);
  const nextOffset = firstNumberFromContainers(containers, ["nextOffset"], (value) => value >= 0);
  const currentOffset = firstNumberFromContainers(containers, ["currentOffset"], (value) => value >= 0);
  const action = containers
    .map((container) => toStringValue(container.action)?.toLowerCase())
    .find(Boolean);
  const limit = Math.max(1, Math.min(50, requestedLimit ?? configuredLimit ?? fallbackLimit));
  let offset = requestedOffset ?? 0;

  if ((action === "next" || action === "next_page" || action === "load_more") && requestedOffset === undefined) {
    offset = nextOffset ?? ((currentOffset ?? 0) + limit);
  }

  return {
    enabled: true,
    limit,
    offset: Math.max(0, offset)
  };
};

const buildMapOptionPaginationResult = (
  payload: unknown,
  request: MapOptionPaginationRequest,
  returnedCount: number
): MapOptionPaginationResult => {
  if (!request.enabled || request.limit === undefined || request.offset === undefined) return {};

  const record = pickRecord(payload);
  const numberMatched = toNumberValue(record.numberMatched ?? record.totalMatched ?? record.total ?? record.count);
  const numberReturned = toNumberValue(record.numberReturned ?? record.returned) ?? returnedCount;
  const hasNext = numberMatched !== undefined
    ? request.offset + numberReturned < numberMatched
    : numberReturned >= request.limit;
  const hasBack = request.offset > 0;

  return {
    public: {
      numberMatched,
      numberReturned,
      hasNext,
      hasBack
    },
    state: {
      enabled: true,
      limit: request.limit,
      offset: request.offset,
      hasNext,
      ...(hasNext ? { nextOffset: request.offset + numberReturned } : {})
    }
  };
};

const buildVectorTileChoices = (collections: VectorTileCollection[], layerType: string): MapOptionChoice[] => {
  return collections.slice(0, VECTOR_TILE_CHOICE_LIMIT).map((collection) => ({
    label: collection.title || collection.id,
    value: collection.id,
    layerId: collection.id,
    layerTitle: collection.title,
    type: layerType,
    ...(collection.geometryType ? { geometryType: collection.geometryType } : {}),
    ...(collection.sourceLayer ? { sourceLayer: collection.sourceLayer } : {}),
    ...(collection.description ? { description: collection.description } : {})
  }));
};

const fetchVectorTileJson = async (url: string): Promise<unknown> => {
  return fetchVallarisJson(url);
};

const fetchVallarisJson = async (url: string): Promise<unknown> => {
  const response = await fetch(url, {
    headers: {
      Accept: "application/json, application/vnd.mapbox.style+json"
    }
  });

  if (!response.ok) {
    throw new Error(`VALLARIS request failed: ${response.status} ${response.statusText} (${createPublicVallarisMapUrl(url)})`);
  }

  return response.json();
};

const fetchOptionalVallarisJson = async (url: string): Promise<unknown | undefined> => {
  try {
    return await fetchVallarisJson(url);
  } catch (error) {
    console.warn("VALLARIS enrich lookup skipped:", error instanceof Error ? error.message : error);
    return undefined;
  }
};

const normalizeVallarisLink = (value: unknown): VallarisLink | undefined => {
  if (!isRecord(value)) return undefined;
  const href = toStringValue(value.href);
  if (!href) return undefined;

  return {
    href,
    ...(toStringValue(value.rel) ? { rel: toStringValue(value.rel) } : {}),
    ...(toStringValue(value.type) ? { type: toStringValue(value.type) } : {}),
    ...(toStringValue(value.title) ? { title: toStringValue(value.title) } : {}),
    ...(typeof value.templated === "boolean" ? { templated: value.templated } : {})
  };
};

const normalizeVallarisStyle = (value: unknown): VallarisStyle | undefined => {
  if (!isRecord(value)) return undefined;

  const id = toStringValue(value.id)
    || toStringValue(value._id)
    || toStringValue(value.styleId)
    || toStringValue(value.style_id);
  if (!id) return undefined;

  const links = Array.isArray(value.links)
    ? value.links
        .map(normalizeVallarisLink)
        .filter((link): link is VallarisLink => Boolean(link))
    : [];

  return {
    id,
    title: toStringValue(value.title) || toStringValue(value.name),
    description: toStringValue(value.description),
    links,
    raw: value
  };
};

const extractVallarisStyles = (payload: unknown): VallarisStyle[] => {
  const directStyle = normalizeVallarisStyle(payload);
  if (directStyle) return [directStyle];

  if (Array.isArray(payload)) {
    return payload
      .map(normalizeVallarisStyle)
      .filter((style): style is VallarisStyle => Boolean(style));
  }

  if (!isRecord(payload)) return [];

  const arrayKeys = ["styles", "items", "data", "results", "features", "collections"];
  for (const key of arrayKeys) {
    const value = payload[key];
    if (!Array.isArray(value)) continue;

    const styles = value
      .map((item) => normalizeVallarisStyle(isRecord(item) && isRecord(item.properties)
        ? { ...item.properties, id: item.id ?? item.properties.id, links: item.links ?? item.properties.links }
        : item
      ))
      .filter((style): style is VallarisStyle => Boolean(style));
    if (styles.length > 0) return styles;
  }

  return [];
};

const collectSearchableStrings = (
  value: unknown,
  output: string[] = [],
  depth = 0
): string[] => {
  if (output.length >= 160 || depth > 5) return output;

  const clean = toStringValue(value);
  if (clean) {
    output.push(clean);
    return output;
  }

  if (Array.isArray(value)) {
    for (const item of value) {
      collectSearchableStrings(item, output, depth + 1);
      if (output.length >= 160) break;
    }
    return output;
  }

  if (!isRecord(value)) return output;

  for (const [key, nestedValue] of Object.entries(value)) {
    if (["href", "api_key", "apikey"].includes(key)) continue;
    if (["id", "name", "title", "description", "keywords", "tags", "source", "source-layer"].includes(key)) {
      output.push(key);
    }
    collectSearchableStrings(nestedValue, output, depth + 1);
    if (output.length >= 160) break;
  }

  return output;
};

const findVallarisMetadataLink = (style: VallarisStyle): VallarisLink | undefined => {
  return style.links.find((link) => {
    const rel = link.rel?.toLowerCase();
    const type = link.type?.toLowerCase();
    const title = link.title?.toLowerCase();
    return rel === "describedby"
      || title?.includes("metadata")
      || type === "application/json";
  });
};

const findVallarisStylesheetLink = (style: VallarisStyle): VallarisLink | undefined => {
  return style.links.find((link) => {
    const rel = link.rel?.toLowerCase();
    const type = link.type?.toLowerCase();
    const title = link.title?.toLowerCase();
    return rel === "stylesheet"
      || type === "application/vnd.mapbox.style+json"
      || title?.includes("mapbox");
  });
};

const enrichVallarisStyle = async (
  style: VallarisStyle,
  apiKey: string,
  shouldFetchLinkedDetails: boolean
): Promise<EnrichedVallarisStyle> => {
  const metadataLink = shouldFetchLinkedDetails ? findVallarisMetadataLink(style) : undefined;
  const stylesheetLink = shouldFetchLinkedDetails ? findVallarisStylesheetLink(style) : undefined;
  const [metadata, stylesheet] = await Promise.all([
    metadataLink ? fetchOptionalVallarisJson(appendApiKeyQuery(metadataLink.href, apiKey)) : undefined,
    stylesheetLink ? fetchOptionalVallarisJson(appendApiKeyQuery(stylesheetLink.href, apiKey)) : undefined
  ]);
  const searchableParts = [
    style.id,
    style.title,
    style.description,
    ...style.links.flatMap((link) => [link.title, link.rel, link.type]),
    ...collectSearchableStrings(metadata),
    ...collectSearchableStrings(stylesheet)
  ].filter((part): part is string => Boolean(part?.trim()));

  return {
    ...style,
    searchableText: searchableParts.join(" "),
    ...(metadata !== undefined ? { metadata } : {}),
    ...(stylesheet !== undefined ? { stylesheet } : {})
  };
};

const getVallarisStyles = async (
  config: { baseUrl: string; urlTemplate: string },
  apiKey: string
): Promise<EnrichedVallarisStyle[]> => {
  const catalogPayload = await fetchVallarisJson(buildVallarisCatalogUrl(config, apiKey));
  const styles = extractVallarisStyles(catalogPayload);

  return Promise.all(
    styles.map((style, index) => enrichVallarisStyle(style, apiKey, index < VALLARIS_ENRICH_LIMIT))
  );
};

const getVallarisStylesPage = async (
  config: { baseUrl: string; urlTemplate: string; layerConfigTemplate?: unknown },
  aiArgs: MapToolArgs,
  apiKey: string
): Promise<{
  styles: EnrichedVallarisStyle[];
  pagination: MapOptionPaginationResult;
}> => {
  const template = pickRecord(config.layerConfigTemplate);
  const paginationRequest = getMapOptionPaginationRequest(aiArgs, template, VALLARIS_STYLE_CHOICE_LIMIT);
  const catalogUrl = paginationRequest.enabled
    ? buildMapOptionUrl(config.baseUrl, config.urlTemplate, apiKey, {
      limit: paginationRequest.limit,
      offset: paginationRequest.offset
    })
    : buildVallarisCatalogUrl(config, apiKey);
  const catalogPayload = await fetchVallarisJson(catalogUrl);
  const styles = extractVallarisStyles(catalogPayload);
  const enrichedStyles = await Promise.all(
    styles.map((style, index) => enrichVallarisStyle(style, apiKey, index < VALLARIS_ENRICH_LIMIT))
  );

  return {
    styles: enrichedStyles,
    pagination: buildMapOptionPaginationResult(catalogPayload, paginationRequest, enrichedStyles.length)
  };
};

const normalizeMatchText = (value: string): string => {
  return value.toLowerCase().replace(/[\s()[\]{}"'`.,:;|/_-]+/g, "");
};

const tokenizeMatchText = (value: string): string[] => {
  return value
    .toLowerCase()
    .split(/[\s()[\]{}"'`.,:;|/_-]+/)
    .map((term) => term.trim())
    .filter(Boolean);
};

const getUniqueMatchTerms = (value: string): string[] => {
  const terms = tokenizeMatchText(value).filter((term) => term.length >= 2);
  return terms.filter((term, index) => terms.indexOf(term) === index);
};

const buildVallarisTermWeights = (
  styles: EnrichedVallarisStyle[],
  query: string
): Map<string, number> => {
  const queryTerms = getUniqueMatchTerms(query);
  const weights = new Map<string, number>();

  for (const term of queryTerms) {
    const documentFrequency = styles.filter((style) => {
      return new Set(tokenizeMatchText(style.searchableText)).has(term);
    }).length;

    weights.set(
      term,
      1 + Math.log((styles.length + 1) / (documentFrequency + 1))
    );
  }

  return weights;
};

const scoreVallarisStyle = (
  query: string,
  style: EnrichedVallarisStyle,
  termWeights: Map<string, number>
): VallarisStyleMatch => {
  const normalizedQuery = normalizeMatchText(query);
  const searchableText = style.searchableText.toLowerCase();
  const normalizedSearchable = normalizeMatchText(style.searchableText);
  const scoringTerms = getUniqueMatchTerms(query);
  const searchableTokens = new Set(tokenizeMatchText(style.searchableText));
  const titleTokens = new Set(tokenizeMatchText(style.title || ""));
  const descriptionTokens = new Set(tokenizeMatchText(style.description || ""));
  let score = 0;

  if (normalizedQuery && normalizedSearchable.includes(normalizedQuery)) score += 10;
  if (style.title && normalizeMatchText(style.title).includes(normalizedQuery)) score += 6;
  if (style.description && normalizeMatchText(style.description).includes(normalizedQuery)) score += 4;

  for (const term of scoringTerms) {
    const weight = termWeights.get(term) || 1;
    const isExactSearchableToken = searchableTokens.has(term);
    const isExactTitleToken = titleTokens.has(term);
    const isExactDescriptionToken = descriptionTokens.has(term);

    if (searchableText.includes(term)) score += 1 * weight;
    if (style.title?.toLowerCase().includes(term)) score += 2 * weight;
    if (style.description?.toLowerCase().includes(term)) score += 2 * weight;
    if (isExactSearchableToken) score += 2 * weight;
    if (isExactTitleToken) score += 3 * weight;
    if (isExactDescriptionToken) score += 4 * weight;
  }

  const maxTermWeight = scoringTerms.reduce((sum, term) => sum + (termWeights.get(term) || 1), 0);
  const confidence = Math.min(0.99, score / Math.max(12, maxTermWeight * 6 + 8));
  return { style, score, confidence };
};

const rankVallarisStyles = (
  styles: EnrichedVallarisStyle[],
  query?: string
): VallarisStyleMatch[] => {
  if (!query?.trim()) {
    return styles
      .map((style) => ({ style, score: 0, confidence: 0 }))
      .slice(0, VALLARIS_STYLE_CHOICE_LIMIT);
  }

  const termWeights = buildVallarisTermWeights(styles, query);

  return styles
    .map((style) => scoreVallarisStyle(query, style, termWeights))
    .sort((left, right) => right.score - left.score || left.style.title?.localeCompare(right.style.title || "") || 0);
};

const getVallarisPreviewUrl = (style: VallarisStyle): string | undefined => {
  return style.links.find((link) => {
    const rel = link.rel?.toLowerCase();
    const title = link.title?.toLowerCase();
    return rel === "preview" || title?.includes("preview");
  })?.href;
};

const isVallarisMapOptionLink = (link: VallarisLink): boolean => {
  if (!link.title?.trim()) return false;

  try {
    const parsedUrl = new URL(link.href);
    return parsedUrl.pathname.includes("/maps/");
  } catch {
    return link.href.includes("/maps/");
  }
};

const createVallarisMapOptionValue = (link: VallarisLink): string | undefined => {
  const titleValue = link.title
    ?.trim()
    .toLowerCase()
    .replace(/[\s()[\]{}"'`.,:;|/_]+/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-|-$/g, "");

  if (titleValue) return titleValue;

  try {
    const parsedUrl = new URL(link.href);
    const pathParts = parsedUrl.pathname.split("/").filter(Boolean);
    return pathParts[pathParts.length - 1]?.replace(/\.[^.]+$/, "");
  } catch {
    const pathParts = link.href.split(/[/?#]/).filter(Boolean);
    return pathParts[pathParts.length - 1]?.replace(/\.[^.]+$/, "");
  }
};

const buildVallarisMapChoices = (
  style: EnrichedVallarisStyle,
  apiKey?: string,
  includePublicUrl = false
): MapOptionChoice[] => {
  const choicesByValue = new Map<string, MapOptionChoice>();

  for (const link of style.links) {
    if (!isVallarisMapOptionLink(link)) continue;
    const value = createVallarisMapOptionValue(link);
    if (!value || choicesByValue.has(value)) continue;

    choicesByValue.set(value, {
      label: link.title?.trim() || value,
      value,
      type: value,
      templated: link.templated === true || (link.href.includes("{z}") && link.href.includes("{x}") && link.href.includes("{y}")),
      ...(link.type ? { mediaType: link.type } : {}),
      ...(link.rel ? { rel: link.rel } : {}),
      ...(apiKey ? { url: appendApiKeyQuery(link.href, apiKey) } : includePublicUrl ? { url: createPublicVallarisMapUrl(link.href) } : {}),
      //description: `${style.title || style.id} (${link.title?.trim() || value})`
    });
  }

  return Array.from(choicesByValue.values());
};

const buildVallarisStyleChoices = (
  matches: VallarisStyleMatch[]
): MapOptionChoice[] => {
  return matches.slice(0, VALLARIS_STYLE_CHOICE_LIMIT).map((match) => ({
    label: match.style.title || match.style.id,
    value: match.style.id,
    description: [
      match.style.description,
      match.confidence > 0 ? `confidence=${match.confidence.toFixed(2)}` : undefined
    ].filter(Boolean).join(" | ")
  }));
};

const inferVectorTileLayerIdFromQuery = (
  query: string | undefined,
  choices: MapOptionChoice[]
): string | undefined => {
  if (!query?.trim() || choices.length === 0) return undefined;

  const normalizedQuery = normalizeMatchText(query);
  const queryTokens = new Set(tokenizeMatchText(query));
  const scoredChoices = choices.map((choice) => {
    const candidateTexts = [
      choice.label,
      choice.description,
      choice.layerTitle
    ].filter((value): value is string => typeof value === "string" && Boolean(value.trim()));

    let score = 0;
    for (const text of candidateTexts) {
      const normalizedText = normalizeMatchText(text);
      const textTokens = tokenizeMatchText(text);
      const matchedTokens = textTokens.filter((token) => queryTokens.has(token));
      const hasExactTextMatch = normalizedText && normalizedQuery.includes(normalizedText);
      const hasFullTokenMatch = textTokens.length > 0 && matchedTokens.length === textTokens.length;

      if (!hasExactTextMatch && !hasFullTokenMatch) continue;

      if (hasExactTextMatch) score += normalizedText.length * 2;

      for (const token of matchedTokens) {
        if (!queryTokens.has(token)) continue;
        score += /^\d+$/.test(token) ? 8 : Math.max(2, token.length);
      }
    }

    return { choice, score };
  }).filter((item) => item.score > 0)
    .sort((left, right) => right.score - left.score);

  const best = scoredChoices[0];
  if (!best) return undefined;

  const second = scoredChoices[1];
  if (second && second.score >= best.score) return undefined;

  return best.choice.value;
};
 //29/05/2026
const buildVectorTileOptionsPayload = async (
  config: { intentName: string; provider: string; baseUrl: string; urlTemplate: string; layerConfigTemplate: unknown },
  aiArgs: MapToolArgs,
  apiKey: string
) => {
  const template = pickRecord(config.layerConfigTemplate);
  const optionKey = getVectorTileOptionKey(config.layerConfigTemplate);
  const layerType = getCollectionDetailType(config.layerConfigTemplate);
  const queryVariables = {
    ...buildTemplateVariables(aiArgs, template, config.intentName, config.provider, apiKey)
  };
  const collectionQuery = renderQueryConfig(template.collectionQuery, queryVariables);
  const collectionRequestParams = getVectorTileCollectionRequestParams(aiArgs, optionKey);
  const requestQuery = {
    ...collectionQuery,
    ...collectionRequestParams
  };
  const paginationRequest = getMapOptionPaginationRequest(aiArgs, template,VECTOR_TILE_CHOICE_LIMIT);
  const collectionsUrl = buildMapOptionUrl(
    config.baseUrl,
    config.urlTemplate,
    apiKey,
    paginationRequest.enabled
      ? {
        ...requestQuery,
        limit: paginationRequest.limit,
        offset: paginationRequest.offset
      }
      : requestQuery
  );
  const collectionsPayload = await fetchVectorTileJson(collectionsUrl);
  const collections = extractVectorTileCollections(collectionsPayload);
  const pagination = buildMapOptionPaginationResult(collectionsPayload, paginationRequest, collections.length);
  const collectionChoices = buildVectorTileChoices(collections, layerType);
  const optionsOnly = aiArgs.optionsOnly === true
    || pickRecord(aiArgs.params).optionsOnly === true
    || pickRecord(aiArgs.options).optionsOnly === true
    || pickRecord(aiArgs.variables).optionsOnly === true;
  const explicitLayerId = optionsOnly ? undefined : getVectorTileLayerId(aiArgs, optionKey);
  const shouldInferLayerId = !optionsOnly && !explicitLayerId && Object.keys(collectionRequestParams).length === 0;
  const selectedLayerId = explicitLayerId
    || (shouldInferLayerId ? inferVectorTileLayerIdFromQuery(getMapQuery(aiArgs), collectionChoices) : undefined);
  const selectedCollection = selectedLayerId
    ? collections.find((collection) => collection.id === selectedLayerId)
    : undefined;

  if (selectedLayerId && !selectedCollection) {
    return {
      success: true,
      needInfo: false,
      missingKeys: [],
      options: [],
      choices: [],
      selectedValues: {
        [optionKey]: selectedLayerId,
        layerId: selectedLayerId
      },
      complete: true,
      intentName: config.intentName,
      provider: config.provider,
      question: undefined,
      questionHint: "A vector tile layerId is already selected. Call get_map_layer with this layerId."
    };
  }

  if (collections.length === 0) {
    return {
      success: false,
      needInfo: false,
      missingKeys: [],
      options: [],
      complete: false,
      intentName: config.intentName,
      provider: config.provider,
      selectedValues: collectionRequestParams,
      ...(pagination.public ? { pagination: pagination.public } : {}),
      ...(pagination.state ? { paginationState: pagination.state } : {}),
      message: "No VALLARIS vector tile layers were found."
    };
  }

  if (!selectedCollection) {
    const layerOption: MapOptionInfo = {
      key: optionKey,
      required: true,
      source: "template",
      label: "Layer",
      description: `Choose the ${layerType} layer to use`,
      choices: collectionChoices
    };
    return {
      success: true,
      needInfo: true,
      missingKeys: [optionKey],
      options: [layerOption],
      choices: [layerOption],
      selectedValues: collectionRequestParams,
      complete: false,
      intentName: config.intentName,
      provider: config.provider,
      ...(pagination.public ? { pagination: pagination.public } : {}),
      ...(pagination.state ? { paginationState: pagination.state } : {}),
      question: buildMapOptionQuestion([layerOption]),
      questionHint: "Ask the user to choose one vector tile layerId from these DB/API-backed choices."
    };
  }

  return {
    success: true,
    needInfo: false,
    missingKeys: [],
    options: [],
    choices: [],
    selectedValues: {
      [optionKey]: selectedCollection.id,
      layerId: selectedCollection.id,
      layerTitle: selectedCollection.title,
      ...(selectedCollection.datasetId ? { datasetId: selectedCollection.datasetId } : {}),
      ...(selectedCollection.description ? { description: selectedCollection.description } : {}),
      ...(selectedCollection.geometryType ? { geometryType: selectedCollection.geometryType } : {}),
      ...(selectedCollection.sourceLayer ? { sourceLayer: selectedCollection.sourceLayer } : {}),
      type: layerType
    },
    complete: true,
    intentName: config.intentName,
    provider: config.provider,
    question: undefined,
    questionHint: "LayerId is selected. Call get_map_layer to retrieve layer metadata."
  };
};

const resolveVallarisStyleSelection = async (
  config: { baseUrl: string; urlTemplate: string; layerConfigTemplate?: unknown },
  aiArgs: MapToolArgs,
  apiKey: string
): Promise<{
  styles: EnrichedVallarisStyle[];
  matches: VallarisStyleMatch[];
  selectedStyle?: EnrichedVallarisStyle;
  selectedMatch?: VallarisStyleMatch;
  query?: string;
  pagination: MapOptionPaginationResult;
}> => {
  const { styles, pagination } = await getVallarisStylesPage(config, aiArgs, apiKey);
  const selectedStyleId = getSelectedVallarisStyleId(aiArgs);
  const query = getMapQuery(aiArgs);
  const matches = rankVallarisStyles(styles, query);
  const shouldRequireExplicitStyleSelection = pickRecord(pickRecord(config.layerConfigTemplate).pagination).enabled === true;
  const selectedStyle = selectedStyleId
    ? styles.find((style) => style.id === selectedStyleId)
    : undefined;

  if (selectedStyle) {
    return {
      styles,
      matches,
      selectedStyle,
      selectedMatch: matches.find((match) => match.style.id === selectedStyle.id),
      query,
      pagination
    };
  }

  if (shouldRequireExplicitStyleSelection) {
    return {
      styles,
      matches,
      query,
      pagination
    };
  }

  const bestMatch = matches[0];
  const secondMatch = matches[1];
  const isAmbiguous = Boolean(
    bestMatch
    && secondMatch
    && bestMatch.score > 0
    && secondMatch.score / bestMatch.score >= 0.85
    && bestMatch.confidence < 0.8
  );

  if (bestMatch && bestMatch.confidence >= VALLARIS_MATCH_THRESHOLD && !isAmbiguous) {
    return {
      styles,
      matches,
      selectedStyle: bestMatch.style,
      selectedMatch: bestMatch,
      query,
      pagination
    };
  }

  return { styles, matches, query, pagination };
};

const buildVallarisOptionsPayload = async (
  config: { intentName: string; provider: string; baseUrl: string; urlTemplate: string; layerConfigTemplate?: unknown },
  aiArgs: MapToolArgs,
  apiKey: string,
  includeSecureUrls = false
) => {
  const selection = await resolveVallarisStyleSelection(config, aiArgs, apiKey);
  const explicitSelectedType = getSelectedVallarisType(aiArgs);

  if (selection.styles.length === 0) {
    return {
      success: false,
      needInfo: false,
      missingKeys: [],
      options: [],
      complete: false,
      intentName: config.intentName,
      provider: config.provider,
      ...(selection.pagination.public ?{ pagination: selection.pagination.public } : {}),
      ...(selection.pagination.state ? { paginationState: selection.pagination.state } : {}),
      message: "Can't find style from vallaris."
    };
  }

  if (!selection.selectedStyle) {
    const styleOption: MapOptionInfo = {
      key: "styleId",
      required: true,
      source: "template",
      label: "Style",
      description: buildMapOptionDescription("select_vallaris_style", {
        query: selection.query
      }),
      choices: buildVallarisStyleChoices(selection.matches)
    };

    return {
      success: true,
      needInfo: true,
      missingKeys: ["styleId"],
      options: [styleOption],
      choices: [styleOption],
      selectedValues: {},
      complete: false,
      intentName: config.intentName,
      provider: config.provider,
      ...(selection.pagination.public ? { pagination: selection.pagination.public } : {}),
      ...(selection.pagination.state ? { paginationState: selection.pagination.state } : {}),
      question: buildMapQuestion("select_vallaris_style", {
        query: selection.query
      }),
      questionHint: "Ask the user to choose one VALLARIS styleId from these DB/API-backed candidates."
    };
  }

  const publicMapChoices = buildVallarisMapChoices(
    selection.selectedStyle,
    undefined,
    includeSecureUrls

  );
  const selectedType = explicitSelectedType
    || inferVallarisTypeFromQuery(selection.query, publicMapChoices);
  const selectedPublicChoice = selectedType
    ? publicMapChoices.find((choice) => choice.value === selectedType || choice.type === selectedType)
    : undefined;
  const baseSelectedValues = {
    styleId: selection.selectedStyle.id,
    styleTitle: selection.selectedStyle.title,
    ...(selection.selectedStyle.description ? { description: selection.selectedStyle.description } : {}),
    ...(selection.selectedMatch ? { confidence: selection.selectedMatch.confidence } : {}),
    ...(getVallarisPreviewUrl(selection.selectedStyle) ? { previewUrl: getVallarisPreviewUrl(selection.selectedStyle) } : {})
  };

  if (publicMapChoices.length === 0) {
    return {
      success: false,
      needInfo: false,
      missingKeys: [],
      options: [],
      choices: [],
      selectedValues: baseSelectedValues,
      complete: false,
      intentName: config.intentName,
      provider: config.provider,
      message: "No map links were found for the selected VALLARIS style."
    };
  }

  if (!selectedPublicChoice) {
    const typeOption: MapOptionInfo = {
      key: "type",
      required: true,
      source: "template",
      label: "Map type",
      description: buildMapOptionDescription("select_vallaris_type", {
        styleTitle: selection.selectedStyle.title || selection.selectedStyle.id
      }),
      choices: publicMapChoices
    };

    return {
      success: true,
      needInfo: true,
      missingKeys: ["type"],
      options: [typeOption],
      choices: [typeOption],
      selectedValues: baseSelectedValues,
      complete: false,
      intentName: config.intentName,
      provider: config.provider,
      question: buildMapQuestion("select_vallaris_type"),
      questionHint: "Ask the user to choose one VALLARIS map type from these DB/API-backed choices. Do not expose provider API keys in map_options."
    };
  }

  const secureMapChoices = includeSecureUrls
    ? buildVallarisMapChoices(selection.selectedStyle, apiKey)
    : [];
  const selectedSecureChoice = includeSecureUrls
    ? secureMapChoices.find((choice) => choice.value === selectedPublicChoice.value || choice.type === selectedPublicChoice.type)
    : undefined;

  if (includeSecureUrls && !selectedSecureChoice?.url) {
    return {
      success: false,
      needInfo: false,
      missingKeys: [],
      options: [],
      choices: [],
      selectedValues: baseSelectedValues,
      complete: false,
      intentName: config.intentName,
      provider: config.provider,
      message: "Unable to prepare the internal URL for the selected VALLARIS map."
    };
  }

  const selectedValues = {
    ...baseSelectedValues,
    type: selectedPublicChoice.value,
    ...(includeSecureUrls && selectedPublicChoice.url ? { url: selectedPublicChoice.url } : {}),
    templated: selectedPublicChoice.templated
  };

  return {
    success: true,
    needInfo: false,
    missingKeys: [],
    options: [],
    choices: [],
    selectedValues,
    complete: true,
    intentName: config.intentName,
    provider: config.provider,
    question: undefined,
    questionHint: "VALLARIS styleId and map type are selected. Call get_map_layer to recompute the secure URL."
  };
};

const toNumberValue = (value: unknown): number | undefined => {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string" && value.trim()) {
    const numberValue = Number(value);
    return Number.isFinite(numberValue) ? numberValue : undefined;
  }
  return undefined;
};

const toNumberArray = (value: unknown): number[] | undefined => {
  if (!Array.isArray(value)) return undefined;
  const values = value
    .map(toNumberValue)
    .filter((item): item is number => item !== undefined);
  return values.length === value.length ? values : undefined;
};

const toPublicStringArray = (value: unknown): string[] | undefined => {
  if (!Array.isArray(value)) return undefined;
  const values = value
    .map(toStringValue)
    .filter((item): item is string => Boolean(item))
    .map(createVectorTilePublicUrl);
  return values.length > 0 ? values : undefined;
};

const toStringArray = (value: unknown): string[] | undefined => {
  if (!Array.isArray(value)) return undefined;
  const values = value
    .map(toStringValue)
    .filter((item): item is string => Boolean(item));
  return values.length > 0 ? values : undefined;
};

const clampTileLatitude = (lat: number): number => {
  return Math.max(Math.min(lat, 85.05112878), -85.05112878);
};

const lonLatToTile = (lon: number, lat: number, zoom: number): { x: number; y: number } => {
  const latRad = clampTileLatitude(lat) * Math.PI / 180;
  const scale = 2 ** zoom;
  const x = Math.floor(((lon + 180) / 360) * scale);
  const y = Math.floor((1 - Math.log(Math.tan(latRad) + 1 / Math.cos(latRad)) / Math.PI) / 2 * scale);
  const maxIndex = scale - 1;
  return {
    x: Math.max(0, Math.min(maxIndex, x)),
    y: Math.max(0, Math.min(maxIndex, y))
  };
};

const buildVectorTileSampleUrl = (
  tileTemplates: string[] | undefined,
  center: number[] | undefined,
  minzoom: number | undefined,
  maxzoom: number | undefined
): string | undefined => {
  const template = tileTemplates?.[0];
  const lon = center?.[0];
  const lat = center?.[1];
  const centerZoom = center?.[2];
  if (!template || lon === undefined || lat === undefined) return undefined;

  const rawZoom = Number.isFinite(centerZoom) ? centerZoom as number : maxzoom ?? minzoom ?? 0;
  const upperZoom = maxzoom ?? rawZoom;
  const zoom = Math.max(minzoom ?? 0, Math.min(upperZoom, Math.floor(rawZoom)));
  const { x, y } = lonLatToTile(lon, lat, zoom);

  return template
    .replace(/%7Bz%7D/gi, "{z}")
    .replace(/%7Bx%7D/gi, "{x}")
    .replace(/%7By%7D/gi, "{y}")
    .replace(/{z}/g, String(zoom))
    .replace(/{x}/g, String(x))
    .replace(/{y}/g, String(y));
};

const buildVectorTileSampleUrls = (
  tileTemplates: string[] | undefined,
  center: number[] | undefined,
  bounds: number[] | undefined,
  minzoom: number | undefined,
  maxzoom: number | undefined
): string[] => {
  const template = tileTemplates?.[0];
  if (!template) return [];

  const centerZoom = center?.[2];
  const minZoomValue = minzoom ?? 0;
  const maxZoomValue = maxzoom ?? Math.max(minZoomValue, Number.isFinite(centerZoom) ? centerZoom as number : minZoomValue);
  const rawZooms = [
    Number.isFinite(centerZoom) ? centerZoom as number : undefined,
    maxZoomValue,
    Math.floor((minZoomValue + maxZoomValue) / 2),
    minZoomValue
  ];
  const zooms = Array.from(new Set(
    rawZooms
      .map((zoom) => zoom === undefined ? undefined : Math.max(minZoomValue, Math.min(maxZoomValue, Math.floor(zoom))))
      .filter((zoom): zoom is number => zoom !== undefined)
  ));
  const normalizedTemplate = template
    .replace(/%7Bz%7D/gi, "{z}")
    .replace(/%7Bx%7D/gi, "{x}")
    .replace(/%7By%7D/gi, "{y}");
  const urls: string[] = [];
  const samplePoints: Array<[number, number]> = [];

  if (center?.[0] !== undefined && center?.[1] !== undefined) {
    samplePoints.push([center[0], center[1]]);
  }

  if (bounds?.length === 4) {
    const [minLon, minLat, maxLon, maxLat] = bounds;
    const lonSteps = [0.25, 0.5, 0.75];
    const latSteps = [0.25, 0.5, 0.75];

    for (const lonStep of lonSteps) {
      for (const latStep of latSteps) {
        samplePoints.push([
          minLon + (maxLon - minLon) * lonStep,
          minLat + (maxLat - minLat) * latStep
        ]);
      }
    }
  }

  const uniqueSamplePoints = Array.from(
    new Map(samplePoints.map(([lon, lat]) => [`${lon.toFixed(6)},${lat.toFixed(6)}`, [lon, lat] as [number, number]])).values()
  );
  if (uniqueSamplePoints.length === 0) return [];

  for (const zoom of zooms) {
    for (const [lon, lat] of uniqueSamplePoints) {
      const { x, y } = lonLatToTile(lon, lat, zoom);
      const maxIndex = 2 ** zoom - 1;
      const offsets = zoom <= 6
        ? [[0, 0]]
        : [
          [0, 0],
          [-1, 0],
          [1, 0],
          [0, -1],
          [0, 1]
        ];

      for (const [dx, dy] of offsets) {
        const sampleX = x + dx;
        const sampleY = y + dy;
        if (sampleX < 0 || sampleY < 0 || sampleX > maxIndex || sampleY > maxIndex) continue;

        urls.push(
          normalizedTemplate
            .replace(/{z}/g, String(zoom))
            .replace(/{x}/g, String(sampleX))
            .replace(/{y}/g, String(sampleY))
        );
      }
    }
  }

  return Array.from(new Set(urls)).slice(0, 36);
};

const fetchVectorTileBytes = async (url: string): Promise<Uint8Array> => {
  const abortController = new AbortController();
  const timeout = setTimeout(() => abortController.abort(), VECTOR_TILE_GEOMETRY_INFER_TIMEOUT_MS);

  try {
    const response = await fetch(url, {
      headers: {
        Accept: "application/vnd.mapbox-vector-tile, application/x-protobuf, application/octet-stream"
      },
      signal: abortController.signal
    });

    if (!response.ok) {
      throw new Error(`tile sample request failed: ${response.status} ${response.statusText}`);
    }

    return new Uint8Array(await response.arrayBuffer());
  } finally {
    clearTimeout(timeout);
  }
};

const readPbfVarint = (bytes: Uint8Array, offset: number): { value: number; offset: number } => {
  let value = 0;
  let shift = 0;
  let currentOffset = offset;

  while (currentOffset < bytes.length) {
    const byte = bytes[currentOffset++] || 0;
    value += (byte & 0x7f) * (2 ** shift);
    if ((byte & 0x80) === 0) return { value, offset: currentOffset };
    shift += 7;
  }

  return { value, offset: currentOffset };
};

const skipPbfField = (bytes: Uint8Array, offset: number, wireType: number): number => {
  if (wireType === 0) return readPbfVarint(bytes, offset).offset;
  if (wireType === 1) return Math.min(bytes.length, offset + 8);
  if (wireType === 2) {
    const length = readPbfVarint(bytes, offset);
    return Math.min(bytes.length, length.offset + length.value);
  }
  if (wireType === 5) return Math.min(bytes.length, offset + 4);
  return bytes.length;
};

const readFirstVectorTileFeatureType = (bytes: Uint8Array): number | undefined => {
  let offset = 0;
  while (offset < bytes.length) {
    const tag = readPbfVarint(bytes, offset);
    offset = tag.offset;
    const fieldNumber = tag.value >> 3;
    const wireType = tag.value & 7;

    if (fieldNumber === 3 && wireType === 2) {
      const layerLength = readPbfVarint(bytes, offset);
      const layerEnd = Math.min(bytes.length, layerLength.offset + layerLength.value);
      let layerOffset = layerLength.offset;

      while (layerOffset < layerEnd) {
        const layerTag = readPbfVarint(bytes, layerOffset);
        layerOffset = layerTag.offset;
        const layerFieldNumber = layerTag.value >> 3;
        const layerWireType = layerTag.value & 7;

        if (layerFieldNumber === 2 && layerWireType === 2) {
          const featureLength = readPbfVarint(bytes, layerOffset);
          const featureEnd = Math.min(layerEnd, featureLength.offset + featureLength.value);
          let featureOffset = featureLength.offset;
          layerOffset = featureEnd;

          while (featureOffset < featureEnd) {
            const featureTag = readPbfVarint(bytes, featureOffset);
            featureOffset = featureTag.offset;
            const featureFieldNumber = featureTag.value >> 3;
            const featureWireType = featureTag.value & 7;

            if (featureFieldNumber === 3 && featureWireType === 0) {
              return readPbfVarint(bytes, featureOffset).value;
            }

            featureOffset = skipPbfField(bytes, featureOffset, featureWireType);
          }
        } else {
          layerOffset = skipPbfField(bytes, layerOffset, layerWireType);
        }
      }
    } else {
      offset = skipPbfField(bytes, offset, wireType);
    }
  }

  return undefined;
};

const vectorTileFeatureTypeToGeometryType = (featureType: number | undefined): string | undefined => {
  if (featureType === 1) return "point";
  if (featureType === 2) return "line";
  if (featureType === 3) return "polygon";
  return undefined;
};

const inferVectorTileGeometryType = async (
  layerId: string,
  tileTemplates: string[] | undefined,
  center: number[] | undefined,
  bounds: number[] | undefined,
  minzoom: number | undefined,
  maxzoom: number | undefined
): Promise<string | undefined> => {
  const cached = vectorTileGeometryCache.get(layerId);
  if (cached && cached.expiresAt > Date.now()) return cached.geometryType;

  const sampleUrl = buildVectorTileSampleUrl(tileTemplates, center, minzoom, maxzoom);
  const sampleUrls = buildVectorTileSampleUrls(tileTemplates, center, bounds, minzoom, maxzoom);
  const candidateUrls = Array.from(new Set([
    ...(sampleUrl ? [sampleUrl] : []),
    ...sampleUrls
  ]));
  if (candidateUrls.length === 0) return undefined;

  try {
    const results = await Promise.allSettled(
      candidateUrls.map(async (url) => {
        const bytes = await fetchVectorTileBytes(url);
        return vectorTileFeatureTypeToGeometryType(readFirstVectorTileFeatureType(bytes));
      })
    );
    const geometryType = results.find((result): result is PromiseFulfilledResult<string> => (
      result.status === "fulfilled" && Boolean(result.value)
    ))?.value;
    if (geometryType) {
      vectorTileGeometryCache.set(layerId, {
        geometryType,
        expiresAt: Date.now() + VECTOR_TILE_GEOMETRY_CACHE_TTL_MS
      });
    }
    return geometryType;
  } catch (error) {
    console.warn("Vector tile geometry inference skipped:", error instanceof Error ? error.message : error);
    return undefined;
  }
};

const buildVectorTileLayerPayload = async (
  config: { intentName: string; provider: string; baseUrl: string; urlTemplate: string; layerConfigTemplate: unknown },
  aiArgs: MapToolArgs,
  apiKey: string
) => {
  const layerType = getCollectionDetailType(config.layerConfigTemplate);
  const optionKey = getVectorTileOptionKey(config.layerConfigTemplate);
  const layerId = getVectorTileLayerId(aiArgs, optionKey);
  if (!layerId) {
    const optionsPayload = await buildVectorTileOptionsPayload(config, aiArgs, apiKey);
    return { success: false, needsOptions: true, payload: { event: "map_options", ...optionsPayload } };
  }

  const detailUrlTemplate = toStringValue(pickRecord(config.layerConfigTemplate).detailUrlTemplate);
  if (!detailUrlTemplate) {
    return { success: false, error: "layerConfigTemplate.detailUrlTemplate is not configured for collection detail." };
  }

  const tileDetailUrl = buildMapOptionUrl(config.baseUrl, detailUrlTemplate, apiKey, { id: layerId, layerId });
  const tilePayload = pickRecord(await fetchVectorTileJson(tileDetailUrl));
  const tileRecord = isRecord(tilePayload.data) ? tilePayload.data : tilePayload;
  const minzoom = toNumberValue(tileRecord.minzoom ?? tileRecord.minZoom);
  const maxzoom = toNumberValue(tileRecord.maxzoom ?? tileRecord.maxZoom);
  const center = toNumberArray(tileRecord.center);
  const bounds = toNumberArray(tileRecord.bounds);
  const secureTiles = (toStringArray(tileRecord.tiles)
    || toStringArray(tileRecord.tileUrls)
    || toStringArray(tileRecord.tile_urls))
    ?.map((url) => appendApiKeyQuery(url, apiKey));
  const tiles = secureTiles?.map(createVectorTilePublicUrl);
  const template = pickRecord(config.layerConfigTemplate);
  const attributeFields = getAttributeFieldsFromTileRecord(tileRecord, tilePayload);
  const geometryType = getDirectGeometryType(tileRecord)
    || getDirectGeometryType(template)
    || inferGeometryTypeFromAttributeFields(attributeFields)
    || await inferVectorTileGeometryType(layerId, secureTiles, center, bounds, minzoom, maxzoom);
  const sourceLayer = findSourceLayer(tileRecord, layerId);
  const datasetId = getVectorTileDatasetId(tileRecord, tilePayload) || getVectorTileDatasetIdFromArgs(aiArgs);

  return {
    success: true,
    payload: {
      event: "layer_catalog",
      intentName: config.intentName,
      provider: config.provider,
      layer: {
        type: layerType,
        layerId,
        title: toStringValue(tileRecord.title) || toStringValue(tileRecord.name),
        url: createVectorTilePublicUrl(tileDetailUrl),
        ...(tiles ? { tiles } : {}),
        ...(minzoom !== undefined ? { minzoom } : {}),
        ...(maxzoom !== undefined ? { maxzoom } : {}),
        ...(center ? { center } : {}),
        ...(bounds ? { bounds } : {}),
        ...(geometryType ? { geometryType } : {}),
        ...(sourceLayer ? { sourceLayer } : {}),
        ...(attributeFields || datasetId ? {
          attributes: {
            ...(attributeFields ? { fields: attributeFields } : {}),
            ...(datasetId ? { datasetId } : {})
          }
        } : {})
      }
    }
  };
};

const buildVallarisLayerPayload = async (
  config: { intentName: string; provider: string; baseUrl: string; urlTemplate: string },
  aiArgs: MapToolArgs,
  apiKey: string
) => {
  const optionsPayload = await buildVallarisOptionsPayload(config, aiArgs, apiKey, true);
  if (!optionsPayload.success || !optionsPayload.complete) {
    return {
      success: false,
      needsOptions: true,
      payload: {
        event: "map_options",
        ...optionsPayload
      }
    };
  }

  const selectedValues = pickRecord(optionsPayload.selectedValues);
  const styleId = toStringValue(selectedValues.styleId);
  const styleTitle = toStringValue(selectedValues.styleTitle);
  const selectedType = toStringValue(selectedValues.type);
  const selectedUrl = toStringValue(selectedValues.url);
  const type = selectedType;

  if (!styleId || !type || !selectedUrl) {
    return {
      success: false,
      needsOptions: true,
      payload: {
        event: "map_options",
        ...optionsPayload,
        complete: false,
        needInfo: true
      }
    };
  }

  return {
    success: true,
    payload: {
      event: "layer_catalog",
      intentName: config.intentName,
      provider: config.provider,
      layer: {
        styleId,
        ...(styleTitle ? { title: styleTitle, styleTitle } : {}),
        type,
        url: selectedUrl,
      }
    }
  };
};

const getAttributeValuesConfig = (
  layerConfigTemplate: unknown
): Record<string, unknown> => {
  const template = pickRecord(layerConfigTemplate);
  return pickRecord(template.attributeValues)
    || pickRecord(template.attributeValueQuery)
    || pickRecord(template.valueQuery)
    || {};
};

const extractAttributeValueItems = (payload: unknown): Record<string, unknown>[] => {
  if (Array.isArray(payload)) return payload.filter(isRecord);

  const record = pickRecord(payload);
  const candidate = record.items || record.data || record.results || record.features;
  if (!Array.isArray(candidate)) return [];

  return candidate
    .map((item) => {
      const itemRecord = pickRecord(item);
      return isRecord(itemRecord.properties) ? pickRecord(itemRecord.properties) : itemRecord;
    })
    .filter((item) => Object.keys(item).length > 0);
};

const getAttributeValueFromItem = (
  item: Record<string, unknown>,
  attributeKey: string,
  valueKey?: string
): unknown => {
  if (valueKey && item[valueKey] !== undefined) return item[valueKey];
  if (item.value !== undefined) return item.value;
  return item[attributeKey];
};

const getAttributeCountFromItem = (
  item: Record<string, unknown>,
  attributeKey: string,
  countKey?: string
): number | undefined => {
  const rawValue = countKey && item[countKey] !== undefined
    ? item[countKey]
    : item.count ?? item[attributeKey];
  return toNumberValue(rawValue);
};

const summarizeAttributeValues = (
  items: Record<string, unknown>[],
  attributeKey: string,
  attributeType?: string,
  valueKey?: string,
  countKey?: string,
  limit?: number
): {
  values: Array<Record<string, unknown>>;
  stats?: { min: number; max: number };
} => {
  const normalizedType = attributeType?.toLowerCase();
  const valuesByKey = new Map<string, { value: unknown; count?: number }>();

  for (const item of items) {
    const value = getAttributeValueFromItem(item, attributeKey, valueKey);
    if (value === undefined || value === null || value === "") continue;

    const key = String(value);
    const count = getAttributeCountFromItem(item, attributeKey, countKey);
    const existing = valuesByKey.get(key);
    valuesByKey.set(key, {
      value,
      count: (existing?.count || 0) + (count || 0)
    });
  }

  const values = Array.from(valuesByKey.values());
  const realValueLimit = limit !== undefined ? Math.max(Math.floor(limit), 1) : values.length;
  if (normalizedType !== "number") {
    return {
      values: values
        .sort((left, right) => (right.count || 0) - (left.count || 0) || String(left.value).localeCompare(String(right.value)))
        .slice(0, realValueLimit)
        .map((item) => ({
          label: String(item.value),
          value: item.value
        }))
    };
  }

  const numericValues = values
    .map((item) => ({ ...item, numericValue: toNumberValue(item.value) }))
    .filter((item): item is { value: unknown; count?: number; numericValue: number } => item.numericValue !== undefined)
    .sort((left, right) => left.numericValue - right.numericValue);

  const selectedValues = numericValues.length <= realValueLimit
    ? numericValues
    : Array.from({ length: realValueLimit }, (_, index) => {
      const ratio = realValueLimit === 1 ? 0 : index / (realValueLimit - 1);
      return numericValues[Math.round(ratio * (numericValues.length - 1))];
    }).filter((item, index, allItems) => {
      return item && allItems.findIndex((candidate) => candidate?.numericValue === item.numericValue) === index;
    });

  return {
    values: selectedValues.map((item) => ({
      label: String(item.numericValue),
      value: item.numericValue
    })),
    ...(numericValues.length > 0
      ? {
        stats: {
          min: numericValues[0].numericValue,
          max: numericValues[numericValues.length - 1].numericValue
        }
      }
      : {})
  };
};

const getConnectionIdFromPayload = (payload: unknown): string | undefined => {
  const record = pickRecord(payload);
  const connections = Array.isArray(record.connections)
    ? record.connections
    : Array.isArray(record.items)
      ? record.items
      : Array.isArray(record.data)
        ? record.data
        : [];
  return connections.map((item) => toStringValue(pickRecord(item).id)).find(Boolean);
};

const buildAttributeExploreBody = (
  valueConfig: Record<string, unknown>,
  variables: Record<string, unknown>,
  connectionId: string,
  attributeKey: string
): Record<string, unknown> => {
  const configuredBody = pickRecord(replaceTemplateValue(valueConfig.body || valueConfig.requestBody, variables));
  if (Object.keys(configuredBody).length > 0) {
    return {
      ...configuredBody,
      connectionId: toStringValue(configuredBody.connectionId) || connectionId
    };
  }

  const datasourceTemplate = toStringValue(valueConfig.datasourceTemplate);
  if (!datasourceTemplate) return {};
  const aggregate = toStringValue(valueConfig.aggregate);
  const offset = toNumberValue(valueConfig.offset);
  const limit = toNumberValue(valueConfig.limit);

  return {
    connectionId,
    datasource: {
      id: replaceTemplateVariables(datasourceTemplate, variables)
    },
    columns: [
      {
        name: attributeKey,
        alias: "value"
      }
    ],
    aggregate: [
      ...(aggregate ? [{
        column: attributeKey,
        aggregate,
        alias: attributeKey
      }] : [])
    ],
    ...(offset !== undefined ? { offset } : {}),
    ...(limit !== undefined ? { limit } : {})
  };
};

export const handleMapAttributeValuesTool = async (
  userId: string,
  aiArgs: MapToolArgs,
  headerApiKey?: string
) => {
  try {
    const intentName = toStringValue(aiArgs.intentName);
    const provider = normalizeProvider(toStringValue(aiArgs.provider));
    const attributeKey = toStringValue(aiArgs.attributeKey);
    const attributeType = toStringValue(aiArgs.attributeType);
    const layerId = toStringValue(aiArgs.layerId) || toStringValue(aiArgs.id);
    const datasetId = toStringValue(aiArgs.datasetId);

    if (!intentName || !provider || !attributeKey || !datasetId) {
      return {
        success: false,
        event: "map_attribute_values",
        message: "intentName, provider, datasetId, and attributeKey are required to fetch attribute values."
      };
    }

    const configMatches = await prisma.mapconfig.findMany({
      where: {
        intentName,
        isActive: true
      }
    });
    const config = findConfigByIntentProvider(configMatches, intentName, provider);
    if (!config) {
      return {
        success: false,
        event: "map_attribute_values",
        message: `No active mapconfig was found for ${provider}:${intentName}.`
      };
    }

    const valueConfig = getAttributeValuesConfig(config.layerConfigTemplate);
    const connectionUrlTemplate = toStringValue(valueConfig.connectionUrlTemplate);
    const exploreUrlTemplate = toStringValue(valueConfig.exploreUrlTemplate || valueConfig.urlTemplate);
    // Attribute values are provider-configured because the same layer render API
    // may use a different analytics/explore API for value lookup.
    if (!connectionUrlTemplate || !exploreUrlTemplate) {
      return {
        success: false,
        event: "map_attribute_values",
        message: "Attribute value endpoints are not configured."
      };
    }

    const userApiKeys = await resolveUserMapApiKeys(userId, headerApiKey);
    const userApiKey = selectApiKeyForProvider(
      userApiKeys,
      config.provider
    );
    if (!userApiKey) {
      return {
        success: false,
        event: "map_attribute_values",
        message: `The user has no usable API key for provider ${config.provider}.`
      };
    }

    const apiKey = decryptUserApiKey(userApiKey);
    const runtimeConfig = withApiKeyHostBaseUrl(config, userApiKey);
    const variables = buildTemplateVariables(
      {
        ...aiArgs,
        layerId,
        id: layerId,
        datasetId,
        dataset_id: datasetId,
        attributeKey,
        column: attributeKey
      },
      config.layerConfigTemplate,
      intentName,
      config.provider,
      apiKey
    );
    const connectionQuery = pickRecord(replaceTemplateValue(valueConfig.connectionQuery, variables));
    const connectionUrl = buildMapOptionUrl(runtimeConfig.baseUrl, connectionUrlTemplate, apiKey, connectionQuery);
    const connectionPayload = await fetchVectorTileJson(connectionUrl);
    const connectionId = toStringValue(valueConfig.connectionId) || getConnectionIdFromPayload(connectionPayload);
    if (!connectionId) {
      return {
        success: false,
        event: "map_attribute_values",
        message: "No analytics connectionId was found."
      };
    }

    const exploreBody = buildAttributeExploreBody(valueConfig, variables, connectionId, attributeKey);
    if (Object.keys(exploreBody).length === 0) {
      return {
        success: false,
        event: "map_attribute_values",
        message: "layerConfigTemplate.attributeValues.body or datasourceTemplate is not configured."
      };
    }
    const exploreKeySetting = valueConfig.exploreApiKey ?? valueConfig.apiKey ?? valueConfig.analyticsApiKey;
    const configuredExploreApiKey = typeof exploreKeySetting === "string"
      ? toStringValue(replaceTemplateValue(exploreKeySetting, variables))
      : undefined;
    const exploreKeyConfig = pickRecord(exploreKeySetting);
    const hostServiceKeyName = ["host", "mapconfig_host", "mapconfigHost"].includes(toStringValue(exploreKeyConfig.source)?.toLowerCase() || "")
      ? toStringValue(exploreKeyConfig.key || exploreKeyConfig.keyName || exploreKeyConfig.name)
      : undefined;
    const shouldUseCurrentExploreKey = exploreKeySetting === true || toStringValue(exploreKeySetting)?.toLowerCase() === "true";
    const exploreApiKey = shouldUseCurrentExploreKey
      ? userApiKey
      : Object.keys(exploreKeyConfig).length > 0 && !hostServiceKeyName
        ? selectApiKeyForConfig(userApiKeys, config.provider, exploreKeyConfig)
        : undefined;
    const exploreRuntimeConfig = exploreApiKey
      ? withApiKeyHostBaseUrl(config, exploreApiKey)
      : runtimeConfig;
    const exploreApiKeyValue = exploreApiKey ? decryptUserApiKey(exploreApiKey) : undefined;
    const hostExploreApiKeyValue = hostServiceKeyName
      ? getHostServiceApiKey(exploreApiKey?.host || userApiKey.host, hostServiceKeyName)
      : undefined;
    const finalExploreApiKeyValue = configuredExploreApiKey || hostExploreApiKeyValue || exploreApiKeyValue;
    const exploreVariables = finalExploreApiKeyValue
      ? {
        ...variables,
        apiKey: finalExploreApiKeyValue,
        exploreApiKey: finalExploreApiKeyValue
      }
      : variables;
    const exploreQuery = pickRecord(replaceTemplateValue(valueConfig.exploreQuery || valueConfig.query, exploreVariables));
    const baseExploreUrl = buildProviderUrl(exploreRuntimeConfig.baseUrl, exploreUrlTemplate, exploreQuery);
    const exploreUrl = finalExploreApiKeyValue ? appendApiKeyQuery(baseExploreUrl, finalExploreApiKeyValue) : baseExploreUrl;
    const exploreResponse = await fetch(exploreUrl, {
      method: (toStringValue(valueConfig.method) || "POST").toUpperCase(),
      headers: {
        Accept: "application/json",
        "Content-Type": "application/json"
      },
      body: JSON.stringify(exploreBody)
    });
    if (!exploreResponse.ok) {
      return {
        success: false,
        event: "map_attribute_values",
        message: `${config.provider} attribute values request failed: ${exploreResponse.status} ${exploreResponse.statusText}`
      };
    }

    const explorePayload = await exploreResponse.json();
    const items = extractAttributeValueItems(explorePayload);
    const valueKey = toStringValue(replaceTemplateValue(valueConfig.valueKey, variables));
    const countKey = toStringValue(replaceTemplateValue(valueConfig.countKey, variables)) || attributeKey;
    const configuredValueLimit = attributeType?.toLowerCase() === "number"
      ? toNumberValue(valueConfig.numericValueLimit) || toNumberValue(valueConfig.valueLimit) || toNumberValue(valueConfig.suggestionLimit)
      : toNumberValue(valueConfig.valueLimit);
    const summary = summarizeAttributeValues(items, attributeKey, attributeType, valueKey, countKey, configuredValueLimit);
    const fallbackValue = valueConfig.fallbackValue !== undefined
      ? replaceTemplateValue(valueConfig.fallbackValue, variables)
      : 0;

    return {
      success: true,
      event: "map_attribute_values",
      attributeKey,
      ...(attributeType ? { attributeType } : {}),
      valueSource: "data",
      values: summary.values,
      fallbackValue,
      ...(summary.stats ? { stats: summary.stats } : {}),
      numberReturned: summary.values.length,
      numberMatched: toNumberValue(pickRecord(explorePayload).numberMatched) || items.length
    };
  } catch (error) {
    console.error("Map Attribute Values Tool Error:", error);
    return {
      success: false,
      event: "map_attribute_values",
      message: "An error occurred while fetching attribute values."
    };
  }
};

const createStringEnumProperty = (values: string[], description: string) => {
  const uniqueValues = Array.from(new Set(values.filter(Boolean)));
  return {
    type: "string",
    ...(uniqueValues.length > 0 ? { enum: uniqueValues } : {}),
    description
  };
};

const mapChoicesForKey = (options: MapOptionInfo[], key: string): MapOptionChoice[] => {
  const choicesByValue = new Map<string, MapOptionChoice>();
  for (const option of options) {
    if (option.key !== key || !option.choices) continue;
    for (const choice of option.choices) {
      if (!choicesByValue.has(choice.value)) choicesByValue.set(choice.value, choice);
    }
  }

  return Array.from(choicesByValue.values());
};

const describeMapChoices = (choices: MapOptionChoice[]): string => {
  return choices
    .map((choice) => {
      const details = [
        choice.label && choice.label !== choice.value ? choice.label : undefined,
        choice.description
      ].filter(Boolean);

      return details.length > 0
        ? `${choice.value} (${details.join(", ")})`
        : choice.value;
    })
    .join(", ");
};

const createChoiceEnumProperty = (
  choices: MapOptionChoice[],
  description: string
) => {
  const values = choices.map((choice) => choice.value);
  const choiceDescription = describeMapChoices(choices);

  return createStringEnumProperty(
    values,
    choiceDescription
      ? `${description}. DB-backed choices: ${choiceDescription}`
      : description
  );
};

export const buildDynamicMapOptionToolSchema = (
  configs: MapConfigForTools[]
) => {
  const optionGroups = configs.flatMap(buildTemplateOptionGroups);
  const paramsProperties = Object.fromEntries(
    Array.from(new Set(optionGroups.map((option) => option.key)))
      .map((key) => [
        key,
        createChoiceEnumProperty(
          mapChoicesForKey(optionGroups, key),
          `The ${key} value used to fill the map URL/template`
        )
      ])
  );

  return {
    type: "function",
    function: {
      name: "map_options",
      description: "Use immediately when the user asks for a map. Fetch and validate map choices from DB-backed mapconfig. Do not guess choices. For VALLARIS, pass the user's request in query/message so the backend can fetch the style list, enrich metadata/stylesheet, match styleId, and return map type links.",
      parameters: {
        type: "object",
        properties: {
          query: {
            type: "string",
            description: "The latest user message describing the requested map data, such as earthquakes, floods, or a layer name. Especially useful for VALLARIS style matching."
          },
          message: {
            type: "string",
            description: "Alias of query"
          },
          intentName: createStringEnumProperty(
            configs.map((config) => config.intentName),
            "The mapconfig intent name matching the map request"
          ),
          provider: createStringEnumProperty(
            getUniqueProviders(configs.map((config) => config.provider)),
            "The provider the user is allowed to use"
          ),
          params: {
            type: "object",
            properties: paramsProperties,
            description: "Values inferred from the user's message or selected from the UI. Put known values here before asking the user for more. Use only DB-backed enums/descriptions. Collection-backed options will forward selected params to the collection endpoint."
          },
          options: {
            type: "object",
            properties: paramsProperties,
            description: "Alias of params"
          },
          variables: {
            type: "object",
            properties: paramsProperties,
            description: "Additional variables used to fill templates"
          }
        },
        required: []
      }
    }
  };
};

export const buildMapOptionChoiceContext = (configs: MapConfigForTools[]) => {
  return configs.map((config) => {
    const template = pickRecord(config.layerConfigTemplate);
    const collectionQuery = pickRecord(template.collectionQuery);
    const options = buildTemplateOptionGroups(config)
      .filter((option) => option.choices?.length)
      .map((option) => ({
        key: option.key,
        label: option.label || option.key,
        choices: option.choices?.map((choice) => ({
          value: choice.value,
          label: choice.label,
          ...(choice.description ? { description: choice.description } : {})
        })) || []
      }));

    return {
      intentName: config.intentName,
      provider: config.provider,
      type: toStringValue(template.type),
      handler: toStringValue(template.handler),
      itemType: toStringValue(collectionQuery.itemType),
      optionKey: toStringValue(template.optionKey),
      options
    };
  });
};

export const mapToolSchema = {
  type: "function",
  function: {
    name: "get_map_layer",
    description: "Build the map URL and layer payload from the central mapconfig and the selected option values.",
    parameters: {
      type: "object",
        properties: {
        query: {
          type: "string",
          description: "The latest user message used to search/select a style, especially for provider VALLARIS."
        },
        message: {
          type: "string",
          description: "Alias of query"
        },
        intentName: {
          type: "string",
          description: "The intent name from mapconfig returned by map_access"
        },
        provider: {
          type: "string",
          description: "The provider from map_access that the user is allowed to use"
        },
        params: {
          type: "object",
          description: "Values inferred by the chatbot to replace placeholders in urlTemplate/layerConfigTemplate"
        },
        options: {
          type: "object",
          description: "Alias of params for chatbot-inferred values"
        },
        variables: {
          type: "object",
          description: "Additional variables used to replace placeholders in templates"
        }
      },
      required: ["intentName", "provider"]
    }
  }
};

export const styleCatalogToolSchema = {
  type: "function",
  function: {
    name: "style_catalog",
    description: "Fetch map style presets from STYLE_CATALOG_URL, such as polygon, line, point, and raster styles. Return keys/descriptions/layer types for the chatbot to choose from. Do not guess styles when a catalog is available; call this tool first.",
    parameters: {
      type: "object",
      properties: {},
      required: []
    }
  }
};

export const editMapStyleToolSchema = {
  type: "function",
  function: {
    name: "edit_map_style",
    description: "Edit the latest map style using the existing map_style as the base. Use this for paint/layout edits and feature filter operations without calling get_map_layer again. Attributes can drive compatible paint properties or structured filter conditions. Use paintKey/value, layoutKey/value, paint, or layout for direct property edits. Use add_filter with filterConditions to add feature visibility filters.",
    parameters: {
      type: "object",
      properties: {
        layerId: {
          type: "string",
          description: "The layerId to edit. If omitted, the latest layer is used."
        },
        layerTitle: {
          type: "string",
          description: "The exact title/name of a displayed map layer to edit when the user names a layer but does not provide layerId. Leave this empty for generic map/style wording."
        },
        operation: {
          type: "string",
          enum: ["update_layer", "add_property", "remove_property", "add_filter"],
          description: "Use update_layer for normal edits, add_property/remove_property for paint/layout keys, and add_filter for feature visibility filters."
        },
        action: {
          type: "string",
          enum: ["update_layer", "add_property", "remove_property", "add_filter"],
          description: "Alias for operation."
        },
        styleLayerId: {
          type: "string",
          description: "The id of a MapLibre style layer inside map_style.layers. Use this to remove or update a specific style layer."
        },
        sourceLayer: {
          type: "string",
          description: "The sourceLayer/name of the map layer to edit when it is known."
        },
        target: {
          type: "string",
          description: "The current style layer type or style layer id to edit. If omitted, layers from the latest map_style are used."
        },
        layerType: {
          type: "string",
          description: "Optional current MapLibre layer type hint."
        },
        layer: {
          type: "object",
          description: "Optional object containing paint/layout properties to merge into the current style layer."
        },
        instruction: {
          type: "string",
          description: "The original user instruction"
        },
        colorKey: {
          type: "string",
          description: "Primary color from the catalog, such as black, red, or gray"
        },
        colorValue: {
          type: "string",
          description: "A validated hex color, such as #1F2937."
        },
        attributeKey: {
          type: "string",
          description: "Attribute field used by an existing paint expression when editing one attribute stop, such as bright_ti5."
        },
        attributeType: {
          type: "string",
          description: "Attribute type from layer metadata, such as Number or String."
        },
        attributeValue: {
          oneOf: [{ type: "string" }, { type: "number" }],
          description: "The exact stop/category value in an existing attribute paint expression to edit, such as 315."
        },
        attributePatches: {
          type: "array",
          items: {
            type: "object",
            properties: {
              attributeKey: { type: "string" },
              attributeValue: { oneOf: [{ type: "string" }, { type: "number" }] },
              paintKey: { type: "string" },
              output: {},
              value: {},
              colorValue: { type: "string" }
            },
            required: ["attributeValue"]
          },
          description: "Multiple attribute stop/category edits applied in one call. Each item supplies attributeValue and output/value/colorValue; attributeKey and paintKey inherit from the top-level arguments when omitted."
        },
        attributeValues: {
          type: "array",
          items: {},
          description: "Known attribute values from the backend value lookup. Usually omitted by the model because the backend can fill it."
        },
        attributeStats: {
          type: "object",
          description: "Known numeric stats such as { min, max }. Usually omitted by the model because the backend can fill it."
        },
        outputs: {
          type: "array",
          items: {},
          description: "Optional style outputs assigned across attribute values, such as opacity numbers, radius numbers, widths, or colors. If omitted, the backend derives outputs from the selected current paint property."
        },
        fallbackOutput: {
          description: "Optional fallback style output for attribute values without an explicit match."
        },
        paintKey: {
          type: "string",
          description: "Exact MapLibre paint key from the current map_style to edit, remove, or drive from attribute values. When the user uses a natural-language visual term, choose the closest semantically matching existing paint key."
        },
        layoutKey: {
          type: "string",
          description: "MapLibre layout key to edit, add, or remove."
        },
        removePaintKeys: {
          type: "array",
          items: { type: "string" },
          description: "Paint keys to remove when operation is remove_property."
        },
        removeLayoutKeys: {
          type: "array",
          items: { type: "string" },
          description: "Layout keys to remove when operation is remove_property."
        },
        value: {
          oneOf: [{ type: "string" }, { type: "number" }, { type: "boolean" }],
          description: "Generic value for paintKey edits, such as 4 for circle-stroke-width or a catalog color key/hex for color paint keys."
        },
        paint: {
          type: "object",
          description: "MapLibre paint properties to merge directly"
        },
        layout: {
          type: "object",
          description: "MapLibre layout properties to merge directly"
        },
        filter: {
          type: "array",
          items: {},
          description: "Optional complete MapLibre filter expression. Prefer filterConditions when possible."
        },
        filterLogic: {
          type: "string",
          enum: ["all", "any"],
          description: "Logic used to combine multiple filter conditions."
        },
        filterConditions: {
          type: "array",
          items: {
            type: "object",
            properties: {
              attributeKey: { type: "string" },
              operator: {
                type: "string",
                enum: ["==", "!=", ">", ">=", "<", "<=", "in", "!in"]
              },
              value: {},
              values: { type: "array", items: {} }
            },
            required: ["attributeKey", "operator"]
          },
          description: "Structured feature filter conditions. Attribute names and types are validated against the selected layer catalog."
        }
      },
      required: []
    }
  }
};

export const clearMapLayersToolSchema = {
  type: "function",
  function: {
    name: "clear_map_layers",
    description: "Clear displayed map layers without fetching new map data. Use this when the user wants to clear the current map layer, a selected map layer/style by id or title, multiple selected map layers, or all displayed map layers. The backend resolves titles and styleIds from conversation map state.",
    parameters: {
      type: "object",
      properties: {
        mode: {
          type: "string",
          enum: ["selected", "all"],
          description: "selected clears one or more displayed map entries and their style/color state. all clears every displayed map entry."
        },
        layerId: {
          type: "string",
          description: "Specific map identifier to clear when mode is selected. This may be a layerId, styleId, id, title, styleTitle, or sourceLayer from the conversation map state."
        },
        layerIds: {
          type: "array",
          items: { type: "string" },
          description: "Specific map identifiers to clear when mode is selected. Values may be layerIds, styleIds, ids, titles, styleTitles, or sourceLayers."
        },
        layerTitle: {
          type: "string",
          description: "Layer or style title named by the user when clearing a selected map entry."
        },
        styleId: {
          type: "string",
          description: "Style id named by the user when clearing a style-based map entry."
        }
      },
      required: ["mode"]
    }
  }
};

export const mapOptionToolSchema = {
  type: "function",
  function: {
    name: "map_options",
    description: "Validate and return map choices from DB-backed layerConfigTemplate so the user can choose before get_map_layer is called.",
    parameters: {
      type: "object",
        properties: {
        query: {
          type: "string",
          description: "The latest user message used to search for a style/layer, especially for provider VALLARIS."
        },
        message: {
          type: "string",
          description: "Alias of query"
        },
        intentName: {
          type: "string",
          description: "The mapconfig intent name, if known"
        },
        provider: {
          type: "string",
          description: "The provider from map_access, if known"
        },
        params: {
          type: "object",
          description: "Values the chatbot inferred from the user's answer"
        },
        options: {
          type: "object",
          description: "Alias of params for values inferred from the user's answer"
        },
        variables: {
          type: "object",
          description: "Additional variables that are already available"
        }
      },
      required: []
    }
  }
};

export const handleMapOptionsTool = async (
  userId: string,
  aiArgs: MapToolArgs,
  headerApiKey?: string
) => {
  try {
    const allowedKeys = await resolveUserMapApiKeys(userId, headerApiKey);
    if (allowedKeys.length === 0) {
      return {
        success: false,
        options: [],
        message: headerApiKey?.trim()
          ? "The x-api-key header does not match any active API key for this user."
          : "The user has not linked any map API keys."
      };
    }

    const allowedProviders = getUniqueProviders(allowedKeys.map((key) => key.provider));
    const allConfigs = await prisma.mapconfig.findMany({
      where: {
        isActive: true
      },
      select: {
        intentName: true,
        provider: true,
        baseUrl: true,
        urlTemplate: true,
        layerConfigTemplate: true
      },
      orderBy: [
        { provider: "asc" },
        { intentName: "asc" }
      ]
    });
    const providerAllowedConfigs = filterConfigsByProviders(allConfigs, allowedProviders);
    const configs = filterConfigsByQuery(providerAllowedConfigs, getMapQuery(aiArgs));
    let intentName = aiArgs.intentName?.trim();
    let provider = normalizeProvider(aiArgs.provider) || undefined;

    if (!provider && allowedProviders.length === 1) {
      provider = allowedProviders[0];
    }

    const providerScopedConfigs = provider
      ? configs.filter((config) => sameProvider(config.provider, provider))
      : configs;
    const providerScopedAllConfigs = provider
      ? providerAllowedConfigs.filter((config) => sameProvider(config.provider, provider))
      : providerAllowedConfigs;
    const explicitQueryMatches = getExplicitConfigMatchesByQuery(
      providerScopedConfigs,
      getMapQuery(aiArgs)
    );
    const selectedIntentName = toStringValue(pickRecord(aiArgs.selectedOptions).intentName);
    const hasSelectedIntentName = Boolean(selectedIntentName && selectedIntentName === intentName);

    if (
      intentName &&
      !hasSelectedIntentName &&
      providerScopedConfigs.length > 1 &&
      (explicitQueryMatches.length !== 1 || explicitQueryMatches[0]?.intentName !== intentName)
    ) {
      intentName = undefined;
    }

    if (!intentName) {
      const intentMatches = explicitQueryMatches.length === 1
        ? explicitQueryMatches
        : providerScopedConfigs;
      if (intentMatches.length === 1) {
        intentName = intentMatches[0]?.intentName;
      }
    }

    if (!provider && intentName) {
      const providerMatches = configs.filter((config) => config.intentName === intentName);
      if (providerMatches.length === 1) {
        provider = normalizeProvider(providerMatches[0]?.provider) || undefined;
      }
    }

    if (!intentName || !provider) {
      const missingKeys = [
        ...(intentName ? [] : ["intentName"]),
        ...(provider ? [] : ["provider"])
      ];
      const providerChoices = allowedProviders.map((value) => ({
        label: value,
        value
      }));
      const intentChoices = providerScopedAllConfigs.map((config) => ({
        label: `${config.provider}:${config.intentName}`,
        value: config.intentName,
        description: `provider=${config.provider}`
      }));

      return {
        success: true,
        needInfo: true,
        missingKeys,
        options: [
          ...(intentName ? [] : [createAccessOption("intentName", intentChoices)]),
          ...(provider ? [] : [createAccessOption("provider", providerChoices)])
        ],
        configs,
        complete: false,
        question: buildMapOptionQuestion([
          ...(intentName ? [] : [createAccessOption("intentName", intentChoices)]),
          ...(provider ? [] : [createAccessOption("provider", providerChoices)])
        ]),
        questionHint: "Call map_options for map requests. If intent/provider is unclear, ask the user using these DB-backed choices instead of inventing values."
      };
    }

    if (!providerAllowed(allowedProviders, provider)) {
      return {
        success: false,
        options: [],
        message: `The user is not allowed to use provider ${provider}.`
      };
    }

    const configMatches = await prisma.mapconfig.findMany({
      where: {
        intentName,
        isActive: true
      }
    });
    const config = findConfigByIntentProvider(configMatches, intentName, provider);

    if (!config) {
      return {
        success: false,
        options: [],
        message: `No active mapconfig was found for ${provider}:${intentName}.`
      };
    }

    if (isVallarisProvider(config.provider) && isCollectionDetailConfig(config.layerConfigTemplate)) {
      const userApiKey = selectApiKeyForProvider(allowedKeys, config.provider);
      if (!userApiKey) {
        return {
          success: false,
          options: [],
          message: `The user has no usable API key for provider ${config.provider}.`
        };
      }

      let decryptedApiKey = "";
      try {
        decryptedApiKey = decryptUserApiKey(userApiKey);
      } catch (error) {
        console.error("Decrypt VALLARIS API key error:", error);
        return {
          success: false,
          options: [],
          message: "An error occurred while reading the VALLARIS API key."
        };
      }

      return buildVectorTileOptionsPayload(withApiKeyHostBaseUrl(config, userApiKey), aiArgs, decryptedApiKey);
    }

    if (isVallarisProvider(config.provider)) {
      const userApiKey = selectApiKeyForProvider(allowedKeys, config.provider);
      if (!userApiKey) {
        return {
          success: false,
          options: [],
          message: `The user has no usable API key for provider ${config.provider}.`
        };
      }

      let decryptedApiKey = "";
      try {
        decryptedApiKey = decryptUserApiKey(userApiKey);
      } catch (error) {
        console.error("Decrypt VALLARIS API key error:", error);
        return {
          success: false,
          options: [],
          message: "An error occurred while reading the VALLARIS API key."
        };
      }

      return buildVallarisOptionsPayload(withApiKeyHostBaseUrl(config, userApiKey), aiArgs, decryptedApiKey);
    }

    const templateVariables = buildTemplateVariables(aiArgs, config.layerConfigTemplate, intentName, config.provider, "");
    const optionGroups = buildTemplateOptionGroups(config);
    const missingKeys = optionGroups
      .filter((option) => {
        const value = templateVariables[option.key];
        return option.required && (value === undefined || value === null || value === "");
      })
      .map((option) => option.key);
    const invalidKeys = validateTemplateSelections(optionGroups, templateVariables);
    const selectedValues = getSelectedTemplateValues(aiArgs, config, intentName, config.provider);

    return {
      success: true,
      needInfo: missingKeys.length > 0 || invalidKeys.length > 0,
      missingKeys,
      invalidKeys,
      options: optionGroups,
      choices: optionGroups,
      selectedValues,
      complete: missingKeys.length === 0 && invalidKeys.length === 0,
      intentName,
      provider: config.provider,
      question: buildMapOptionQuestion(
        optionGroups.filter((option) => missingKeys.includes(option.key) || invalidKeys.includes(option.key))
      ),
      questionHint: missingKeys.length > 0 || invalidKeys.length > 0
        ? "Ask the user to choose the missing or invalid values using only these DB-backed choices. When all required values are selected, call get_map_layer with params."
        : undefined
    };
  } catch (error) {
    console.error("Map Options Tool Error:", error);
    return {
      success: false,
      needInfo: false,
      missingKeys: [],
      options: [],
      complete: false,
      message: "An error occurred while building map options."
    };
  }
};

export const handleMapTool = async (
  userId: string,
  aiArgs: MapToolArgs,
  headerApiKey?: string
) => {
  try {
    const intentName = aiArgs.intentName?.trim();
    const provider = normalizeProvider(aiArgs.provider);

    if (!intentName || !provider) {
      return { error: "The AI sent incomplete map tool arguments. intentName and provider are required." };
    }

    const configMatches = await prisma.mapconfig.findMany({
      where: {
        intentName
      }
    });
    const activeConfig = findConfigByIntentProvider(
      configMatches.filter((config) => config.isActive),
      intentName,
      provider
    );
    const config = activeConfig || findConfigByIntentProvider(configMatches, intentName, provider);

    if (!config) {
      return {
        error: `No mapconfig was found for intent ${intentName} and provider ${provider}.`
      };
    }

    if (!config.isActive) {
      return {
        error: `mapconfig ${config.intentName} for ${config.provider} is disabled.`
      };
    }

    const userApiKey = selectApiKeyForProvider(
      await resolveUserMapApiKeys(userId, headerApiKey),
      config.provider
    );

    if (!userApiKey) {
      return {
        error: headerApiKey?.trim()
          ? `The API key sent in the header does not match provider ${config.provider}, or it is not authorized.`
          : `The user has not linked an API key for ${config.provider}. Please configure an API key first.`
      };
    }

    let decryptedApiKey = "";
    try {
      decryptedApiKey = decryptUserApiKey(userApiKey);
    } catch (error) {
      console.error("Decrypt map API key error:", error);
      return { error: "An error occurred while reading your API key. The API key configuration may be invalid." };
    }

    if (isVallarisProvider(config.provider) && isCollectionDetailConfig(config.layerConfigTemplate)) {
      return buildVectorTileLayerPayload(withApiKeyHostBaseUrl(config, userApiKey), aiArgs, decryptedApiKey);
    }

    if (isVallarisProvider(config.provider)) {
      return buildVallarisLayerPayload(withApiKeyHostBaseUrl(config, userApiKey), aiArgs, decryptedApiKey);
    }

    const templateVariables = buildTemplateVariables(
      aiArgs,
      config.layerConfigTemplate,
      intentName,
      config.provider,
      decryptedApiKey
    );
    const optionGroups = buildTemplateOptionGroups(config);
    const invalidKeys = validateTemplateSelections(optionGroups, templateVariables);

    if (invalidKeys.length > 0) {
      return {
        success: false,
        needsOptions: true,
        payload: {
          event: "map_options",
          needInfo: true,
          missingKeys: [],
          invalidKeys,
          options: optionGroups,
          choices: optionGroups,
          selectedValues: getSelectedTemplateValues(aiArgs, config, intentName, config.provider),
          intentName,
          provider: config.provider,
          complete: false,
          question: buildMapOptionQuestion(
            optionGroups.filter((option) => invalidKeys.includes(option.key))
          ),
          questionHint: "Selected map values are not in layerConfigTemplate choices. Ask the user to choose again from these DB-backed choices."
        }
      };
    }

    const runtimeConfig = withApiKeyHostBaseUrl(config, userApiKey);
    const finalUrl = replaceTemplateVariables(
      `${runtimeConfig.baseUrl}${runtimeConfig.urlTemplate}`,
      templateVariables
    );

    const configString = replaceTemplateVariables(
      config.layerConfigTemplate ? JSON.stringify(config.layerConfigTemplate) : "{}",
      templateVariables
    );

    if (hasBlockingUnresolvedMapTemplate(finalUrl, configString)) {
      const options = buildMissingOptionInfo(config, templateVariables)
        .map((option) => {
          const configuredOption = optionGroups.find((group) => group.key === option.key);
          return configuredOption || option;
        });
      if (options.length > 0) {
        const missingKeys = options.map((option) => option.key);
        return {
          success: false,
          needsOptions: true,
          payload: {
            event: "map_options",
            needInfo: true,
            missingKeys,
            options,
            choices: optionGroups,
            selectedValues: getSelectedTemplateValues(aiArgs, config, intentName, config.provider),
            intentName,
            provider: config.provider,
            complete: false,
            question: buildMapOptionQuestion(options),
            questionHint: "Ask the user to choose the missing values using only these DB-backed choices. When complete, call get_map_layer with params."
          }
        };
      }

      return {
        error: `The map URL cannot be built yet. The URL/template still contains unresolved variables: ${finalUrl}`
      };
    }

    let finalLayerConfig: Record<string, unknown>;
    try {
      finalLayerConfig = JSON.parse(configString);
    } catch (error) {
      console.error("Map layerConfigTemplate parse error:", error);
      return { error: "The mapconfig layerConfigTemplate is not valid JSON after variable replacement." };
    }
    const publicLayerConfig = stripLayerConfigMetadata(finalLayerConfig);
    const catalogSummary = resolveLayerCatalogSummary(templateVariables);

    return {
      success: true,
      payload: {
        event: "layer_catalog",
        layer: {
          ...publicLayerConfig,
          ...catalogSummary,
          url: finalUrl
        }
      }
    };
  } catch (error) {
    console.error("Map Tool Handler Error:", error);
    return { error: "The map database is temporarily unavailable." };
  }
};

export const handleRenderPmtilesLayerTool = async (
  userId: string,
  mapPayload: unknown,
  headerApiKey?: string
) => {
  try {
    const payloadRecord = pickRecord(mapPayload);
    const layerRecord = pickRecord(payloadRecord.layer);
    const intentName = toStringValue(payloadRecord.intentName);
    const provider = normalizeProvider(toStringValue(payloadRecord.provider));
    const layerId = toStringValue(layerRecord.layerId || layerRecord.id);

    if (!intentName || !provider || !layerId) {
      return {
        success: false,
        error: "The current map layer does not include intentName, provider, and layerId."
      };
    }

    const configMatches = await prisma.mapconfig.findMany({
      where: { intentName }
    });
    const activeConfig = findConfigByIntentProvider(
      configMatches.filter((config) => config.isActive),
      intentName,
      provider
    );
    const config = activeConfig || findConfigByIntentProvider(configMatches, intentName, provider);

    if (!config || !config.isActive) {
      return {
        success: false,
        error: `No active mapconfig was found for ${provider}:${intentName}.`
      };
    }

    const pmtilesUrlTemplate = getPmtilesUrlTemplate(config.layerConfigTemplate);
    if (!pmtilesUrlTemplate) {
      return {
        success: false,
        error: "layerConfigTemplate.pmtilesUrlTemplate is not configured for this map layer."
      };
    }

    const userApiKey = selectApiKeyForProvider(
      await resolveUserMapApiKeys(userId, headerApiKey),
      config.provider
    );

    if (!userApiKey) {
      return {
        success: false,
        error: headerApiKey?.trim()
          ? `The API key sent in the header does not match provider ${config.provider}, or it is not authorized.`
          : `The user has not linked an API key for ${config.provider}. Please configure an API key first.`
      };
    }

    const runtimeConfig = withApiKeyHostBaseUrl(config, userApiKey);
    const apiKey = decryptUserApiKey(userApiKey);
    const privatePmtilesUrl = buildMapOptionUrl(runtimeConfig.baseUrl, pmtilesUrlTemplate, apiKey, { id: layerId });
    const publicPmtilesUrl = createVectorTilePublicUrl(privatePmtilesUrl);
    const { tiles, pmtiles, renderType, detailUrl, type, ...baseLayer } = layerRecord;

    return {
      success: true,
      payload: {
        ...payloadRecord,
        event: "layer_catalog",
        layer: {
          ...baseLayer,
          type: "pmtiles",
          renderType: "pmtiles",
          layerId,
          url: publicPmtilesUrl
        }
      }
    };
  } catch (error) {
    console.error("Render PMTiles Layer Tool Error:", error);
    return {
      success: false,
      error: "An error occurred while building the PMTiles layer payload."
    };
  }
};

export const checkMapAccessSchema = {
  type: "function",
  function: {
    name: "check_user_map",
    description: "Check the user's provider API keys and fetch the central mapconfig entries the user is allowed to use.",
    parameters: {
      type: "object",
      properties: {},
      required: []
    }
  }
};

export const resolveUserMapToolConfigs = async (
  userId: string,
  headerApiKey?: string,
  query?: string
): Promise<MapConfigForTools[]> => {
  const userKeys = await resolveUserMapApiKeys(userId, headerApiKey);
  if (userKeys.length === 0) return [];

  const allowedProviders = getUniqueProviders(userKeys.map((key) => key.provider));

  const configs = await prisma.mapconfig.findMany({
    where: {
      isActive: true
    },
    select: {
      intentName: true,
      provider: true,
      urlTemplate: true,
      layerConfigTemplate: true
    },
    orderBy: [
      { provider: "asc" },
      { intentName: "asc" }
    ]
  });

  return filterConfigsByQuery(
    filterConfigsByProviders(configs, allowedProviders),
    query
  ).map((config) => ({
    ...config,
    type: getConfigType(config.layerConfigTemplate)
  }));
};

export const handleCheckMapAccess = async (userId: string, headerApiKey?: string, query?: string) => {
  try {
    const userKeys = await resolveUserMapApiKeys(userId, headerApiKey);

    if (userKeys.length === 0) {
      return {
        success: false,
        allowedProviders: [],
        allowedHosts: [],
        configs: [],
        message: headerApiKey?.trim()
          ? "The x-api-key header does not match any active API key for this user."
          : "The user has not linked any map API keys. Please advise the user to configure their API keys in the settings."
      };
    }

    const allowedProviders = getUniqueProviders(userKeys.map((key) => key.provider));
    const allowedHosts = getAllowedHosts(userKeys);

    const allConfigs = await prisma.mapconfig.findMany({
      where: {
        isActive: true
      },
      select: {
        intentName: true,
        provider: true,
        baseUrl: true,
        urlTemplate: true,
        layerConfigTemplate: true
      },
      orderBy: [
        { provider: "asc" },
        { intentName: "asc" }
      ]
    });
    const providerAllowedConfigs = filterConfigsByProviders(allConfigs, allowedProviders);
    const explicitQueryMatches = getExplicitConfigMatchesByQuery(providerAllowedConfigs, query);
    const configs = explicitQueryMatches.length > 0
      ? explicitQueryMatches
      : providerAllowedConfigs;

    if (configs.length === 0) {
      return {
        success: false,
        allowedProviders,
        allowedHosts,
        configs: [],
        message: "The user has map API keys, but no active mapconfig exists for those providers."
      };
    }

    return {
      success: true,
      allowedProviders,
      allowedHosts,
      configs: configs.map((config) => ({
        intentName: config.intentName,
        type: getConfigType(config.layerConfigTemplate),
        urlTemplate: config.urlTemplate
      }))
    };
  } catch (error) {
    console.error("Check Map Access Error:", error);
    return {
      success: false,
      allowedProviders: [],
      allowedHosts: [],
      configs: [],
      message: "An error occurred while retrieving map access permissions from the database."
    };
  }
};
