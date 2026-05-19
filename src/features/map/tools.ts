import { prisma } from "../setup/prisma";
import { decrypt, hashApiKey } from "../setup/encryption";
import { env } from "../../lib/env";
import type {
  MapToolArgs,
  MapOptionInfo,
  MapOptionChoice,
  MapConfigForTools
} from "./type";


type ResolvedUserApiKey = {
  id: string;
  provider: string;
  keyName: string;
  encryptedKey: string;
  iv: string;
};

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

type EditMapStyleArgs = MapToolArgs & {
  layerId?: string;
  instruction?: string;
  target?: string;
  colorKey?: string;
  colorKeys?: unknown;
  colorValue?: string;
  mix?: unknown;
  opacity?: number | string;
  radius?: number | string;
  size?: number | string;
  width?: number | string;
  paint?: unknown;
  layout?: unknown;
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

const sameProvider = (left?: string, right?: string): boolean => {
  return normalizeProvider(left) === normalizeProvider(right);
};

const STYLE_CATALOG_CACHE_TTL_MS = 5 * 60 * 1000;

const getUniqueProviders = (providers: string[]): string[] => {
  return Array.from(new Set(providers.map(normalizeProvider).filter(Boolean)));
};

const providerAllowed = (allowedProviders: string[], provider?: string): boolean => {
  const normalizedProvider = normalizeProvider(provider);
  return Boolean(normalizedProvider) && allowedProviders.includes(normalizedProvider);
};

const isVallarisProvider = (provider?: string): boolean => {
  return normalizeProvider(provider) === "VALLARIS";
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
  const geometryAwareEntries = entries.filter((entry) => entry.geometryTypes?.length);
  const candidateEntries = uniqueGeometryTypes.size > 0 && geometryAwareEntries.length > 0
    ? geometryAwareEntries.filter((entry) => entry.geometryTypes?.some((geometryType) => uniqueGeometryTypes.has(geometryType)))
    : entries;

  return candidateEntries.length > 0 ? candidateEntries : entries;
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

const hexToRgb = (hex: string): [number, number, number] | undefined => {
  const normalized = normalizeColorHex(hex);
  if (!normalized) return undefined;
  return [
    Number.parseInt(normalized.slice(1, 3), 16),
    Number.parseInt(normalized.slice(3, 5), 16),
    Number.parseInt(normalized.slice(5, 7), 16)
  ];
};

const rgbToHex = ([red, green, blue]: [number, number, number]): string => {
  return `#${[red, green, blue]
    .map((value) => Math.max(0, Math.min(255, Math.round(value))).toString(16).padStart(2, "0"))
    .join("")
    .toUpperCase()}`;
};

const toStringList = (value: unknown): string[] => {
  if (Array.isArray(value)) {
    return value.map(toStringValue).filter((item): item is string => Boolean(item));
  }
  const stringValue = toStringValue(value);
  return stringValue ? [stringValue] : [];
};

const toNumberList = (value: unknown, length: number): number[] => {
  const values = Array.isArray(value)
    ? value.map(toNumberValue).filter((item): item is number => item !== undefined)
    : [];
  if (values.length !== length) return Array.from({ length }, () => 1 / Math.max(1, length));

  const total = values.reduce((sum, item) => sum + item, 0);
  if (total <= 0) return Array.from({ length }, () => 1 / Math.max(1, length));
  return values.map((item) => item / total);
};

const resolveCatalogColor = (
  colorKeys: string[],
  colors: StyleColorEntry[],
  mix?: unknown
): string | undefined => {
  const palette = new Map(colors.map((color) => [color.key.toLowerCase(), color.value]));
  const resolvedColors = colorKeys
    .map((key) => normalizeColorHex(palette.get(key.toLowerCase())))
    .filter((value): value is string => Boolean(value));

  if (resolvedColors.length === 0) return undefined;
  if (resolvedColors.length === 1) return resolvedColors[0];

  const weights = toNumberList(mix, resolvedColors.length);
  const mixed = resolvedColors.reduce<[number, number, number]>((sum, color, index) => {
    const rgb = hexToRgb(color);
    if (!rgb) return sum;
    const weight = weights[index] ?? 0;
    return [
      sum[0] + rgb[0] * weight,
      sum[1] + rgb[1] * weight,
      sum[2] + rgb[2] * weight
    ];
  }, [0, 0, 0]);

  return rgbToHex(mixed);
};

const getEditInstruction = (args: EditMapStyleArgs): string => {
  return [
    args.instruction,
    args.message,
    args.query,
    args.request
  ].map(toStringValue).filter(Boolean).join(" ");
};

const getEditNumber = (
  args: EditMapStyleArgs,
  keys: Array<keyof EditMapStyleArgs>
): number | undefined => {
  for (const key of keys) {
    const value = toNumberValue(args[key]);
    if (value !== undefined) return value;
  }

  return undefined;
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

const buildColorPatchFromPaint = (
  paint: Record<string, unknown>,
  layerType: string,
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

  if (layerType === "circle") return { "circle-color": color };
  if (layerType === "line") return { "line-color": color };
  if (layerType === "fill") return { "fill-color": color };
  if (layerType === "fill-extrusion") return { "fill-extrusion-color": color };
  if (layerType === "symbol") return { "icon-color": color, "text-color": color };
  if (layerType === "background") return { "background-color": color };
  if (layerType === "heatmap") return { "heatmap-color": buildHeatmapColorRamp(color) };

  return {};
};

const getPaintPatchForLayerType = (
  layerType: string,
  args: EditMapStyleArgs,
  color?: string,
  existingPaint: Record<string, unknown> = {}
): Record<string, unknown> => {
  const explicitPaint = pickRecord(args.paint);
  const opacity = getEditNumber(args, ["opacity"]);
  const normalizedOpacity = opacity !== undefined && opacity > 1 ? opacity / 100 : opacity;
  const colorPatch = buildColorPatchFromPaint(existingPaint, layerType, color);

  if (layerType === "circle") {
    const radius = getEditNumber(args, ["radius", "size"]);
    return {
      ...explicitPaint,
      ...colorPatch,
      ...(radius !== undefined ? { "circle-radius": radius } : {}),
      ...(normalizedOpacity !== undefined ? { "circle-opacity": normalizedOpacity } : {})
    };
  }

  if (layerType === "line") {
    const width = getEditNumber(args, ["width", "size"]);
    return {
      ...explicitPaint,
      ...colorPatch,
      ...(width !== undefined ? { "line-width": width } : {}),
      ...(normalizedOpacity !== undefined ? { "line-opacity": normalizedOpacity } : {})
    };
  }

  if (layerType === "fill") {
    return {
      ...explicitPaint,
      ...colorPatch,
      ...(normalizedOpacity !== undefined ? { "fill-opacity": normalizedOpacity } : {})
    };
  }

  if (layerType === "fill-extrusion") {
    return {
      ...explicitPaint,
      ...colorPatch,
      ...(normalizedOpacity !== undefined ? { "fill-extrusion-opacity": normalizedOpacity } : {})
    };
  }

  if (layerType === "symbol") {
    return {
      ...explicitPaint,
      ...colorPatch,
      ...(normalizedOpacity !== undefined ? { "icon-opacity": normalizedOpacity, "text-opacity": normalizedOpacity } : {})
    };
  }

  if (layerType === "heatmap") {
    return {
      ...explicitPaint,
      ...colorPatch,
      ...(normalizedOpacity !== undefined ? { "heatmap-opacity": normalizedOpacity } : {})
    };
  }

  if (layerType === "raster") {
    return {
      ...explicitPaint,
      ...colorPatch,
      ...(normalizedOpacity !== undefined ? { "raster-opacity": normalizedOpacity } : {})
    };
  }

  return {
    ...explicitPaint,
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

  const colorKeys = [
    ...toStringList(aiArgs.colorKeys),
    ...toStringList(aiArgs.colorKey)
  ];
  const requestedColorValue = normalizeColorHex(aiArgs.colorValue);
  let catalog = await handleStyleCatalogTool();
  let colors = catalog.success && Array.isArray(catalog.colors) ? catalog.colors : [];
  let resolvedColor = requestedColorValue || resolveCatalogColor(colorKeys, colors, aiArgs.mix);

  if (!resolvedColor && colorKeys.length > 0) {
    catalog = await refreshStyleCatalogTool();
    colors = catalog.success && Array.isArray(catalog.colors) ? catalog.colors : [];
    resolvedColor = resolveCatalogColor(colorKeys, colors, aiArgs.mix);
  }

  if (!resolvedColor && colorKeys.length > 0) {
    return {
      success: false,
      event: "map_style",
      colorKeys,
      message: `Color ${colorKeys.join(", ")} was not found in the style catalog colors.`
    };
  }

  const requestedLayerId = toStringValue(aiArgs.layerId) || toStringValue(pickRecord(aiArgs.params).layerId);
  const currentMapLayerId = toStringValue(currentStyle.layerId);
  const target = toStringValue(aiArgs.target)?.toLowerCase();

  const patchLayer = (layer: unknown) => {
    const layerRecord = pickRecord(layer);
    const layerType = toStringValue(layerRecord.type) || "";
    const targetMatches = !target
      || target === layerType
      || (target === "point" && layerType === "circle")
      || (target === "polygon" && layerType === "fill");

    if (!targetMatches) return layerRecord;

    const paintPatch = getPaintPatchForLayerType(layerType, aiArgs, resolvedColor, pickRecord(layerRecord.paint));
    const layoutPatch = pickRecord(aiArgs.layout);

    return {
      ...layerRecord,
      ...(Object.keys(layoutPatch).length > 0 ? { layout: { ...pickRecord(layerRecord.layout), ...layoutPatch } } : {}),
      ...(Object.keys(paintPatch).length > 0 ? { paint: { ...pickRecord(layerRecord.paint), ...paintPatch } } : {})
    };
  };
  const layers = currentLayers.map((layer) => {
    const layerRecord = pickRecord(layer);
    const layerIdMatches = !requestedLayerId
      || requestedLayerId === currentMapLayerId
      || requestedLayerId === toStringValue(layerRecord.id);

    return layerIdMatches ? patchLayer(layerRecord) : layerRecord;
  });

  return {
    ...currentStyle,
    success: true,
    event: "map_style",
    layerId: currentMapLayerId,
    layers,
    styleInstruction: getEditInstruction(aiArgs),
    ...(resolvedColor ? { appliedColor: resolvedColor } : {}),
    ...(colorKeys.length > 0 ? { colorKeys } : {})
  };
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
        iv: true
      }
    });
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
      iv: true
    },
    orderBy: { createdAt: "desc" }
  });
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
  layerConfigTemplate?: unknown;
}): string[] => {
  const template = pickRecord(config.layerConfigTemplate);
  const collectionQuery = pickRecord(template.collectionQuery);
  const itemTypeTerms = collectConfigStringValues(collectionQuery.itemType)
    .filter((value) => normalizeConfigMatchTerm(value).length > 4);

  return Array.from(new Set([
    config.intentName,
    ...collectConfigStringValues(template.type),
    ...collectConfigStringValues(template.handler),
    ...itemTypeTerms,
    ...collectConfigStringValues(template.keywords),
    ...collectConfigStringValues(template.aliases),
    ...collectConfigStringValues(template.matchTerms),
    ...collectConfigStringValues(template.searchTerms),
    ...collectConfigStringValues(template.intentKeywords)
  ]))
    .map(normalizeConfigMatchTerm)
    .filter((term) => term.length > 0);
};

const filterConfigsByQuery = <T extends {
  intentName: string;
  layerConfigTemplate?: unknown;
}>(
  configs: T[],
  query?: string
): T[] => {
  const normalizedQuery = query ? normalizeConfigMatchTerm(query) : "";
  if (!normalizedQuery) return configs;

  const matchedConfigs = configs.filter((config) => {
    const terms = collectConfigMatchTerms(config);
    return terms.some((term) => normalizedQuery.includes(term));
  });

  return matchedConfigs.length > 0 ? matchedConfigs : configs;
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

const buildMapOptionUrl = (
  baseUrl: string,
  template: string,
  apiKey: string,
  params: Record<string, unknown> = {}
): string => {
  const usedKeys = new Set<string>();
  const renderedTemplate = Object.entries(params).reduce((url, [key, value]) => {
    const cleanValue = toStringValue(value);
    if (!cleanValue || !url.includes(`{${key}}`)) return url;
    usedKeys.add(key);
    return url.replace(new RegExp(`{${key}}`, "g"), encodeURIComponent(cleanValue));
  }, template);
  const base = joinProviderUrl(baseUrl, renderedTemplate);
  const query = Object.entries(params)
    .filter(([key, value]) => !usedKeys.has(key) && toStringValue(value))
    .map(([key, value]) => `${encodeURIComponent(key)}=${encodeURIComponent(toStringValue(value) || "")}`)
    .join("&");
  return appendApiKeyQuery(query ? `${base}${base.includes("?") ? "&" : "?"}${query}` : base, apiKey);
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
        sourceLayer: findSourceLayer(record, id)
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

const buildVectorTileOptionsPayload = async (
  config: { intentName: string; provider: string; baseUrl: string; urlTemplate: string; layerConfigTemplate: unknown },
  aiArgs: MapToolArgs,
  apiKey: string
) => {
  const template = pickRecord(config.layerConfigTemplate);
  const optionKey = getVectorTileOptionKey(config.layerConfigTemplate);
  const layerType = getCollectionDetailType(config.layerConfigTemplate);
  const collectionQuery = pickRecord(template.collectionQuery);
  const paginationRequest = getMapOptionPaginationRequest(aiArgs, template,VECTOR_TILE_CHOICE_LIMIT);
  const collectionsUrl = buildMapOptionUrl(
    config.baseUrl,
    config.urlTemplate,
    apiKey,
    paginationRequest.enabled
      ? {
        ...collectionQuery,
        limit: paginationRequest.limit,
        offset: paginationRequest.offset
      }
      : collectionQuery
  );
  const collectionsPayload = await fetchVectorTileJson(collectionsUrl);
  const collections = extractVectorTileCollections(collectionsPayload);
  const pagination = buildMapOptionPaginationResult(collectionsPayload, paginationRequest, collections.length);
  const selectedLayerId = getVectorTileLayerId(aiArgs, optionKey);
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
      choices: buildVectorTileChoices(collections, layerType)
    };
    return {
      success: true,
      needInfo: true,
      missingKeys: [optionKey],
      options: [layerOption],
      choices: [layerOption],
      selectedValues: {},
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
  minzoom: number | undefined,
  maxzoom: number | undefined
): Promise<string | undefined> => {
  const cached = vectorTileGeometryCache.get(layerId);
  if (cached && cached.expiresAt > Date.now()) return cached.geometryType;

  const sampleUrl = buildVectorTileSampleUrl(tileTemplates, center, minzoom, maxzoom);
  if (!sampleUrl) return undefined;

  try {
    const bytes = await fetchVectorTileBytes(sampleUrl);
    const geometryType = vectorTileFeatureTypeToGeometryType(readFirstVectorTileFeatureType(bytes));
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
  const secureTiles = toStringArray(tileRecord.tiles)
    || toStringArray(tileRecord.tileUrls)
    || toStringArray(tileRecord.tile_urls);
  const tiles = secureTiles?.map(createVectorTilePublicUrl);
  const template = pickRecord(config.layerConfigTemplate);
  const geometryType = getDirectGeometryType(tileRecord)
    || getDirectGeometryType(template)
    || await inferVectorTileGeometryType(layerId, secureTiles, center, minzoom, maxzoom);
  const sourceLayer = findSourceLayer(tileRecord, layerId);

  return {
    success: true,
    payload: {
      event: "layer_catalog",
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
        ...(sourceLayer ? { sourceLayer } : {})
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
      layer: {
        styleId,
        type,
        url: selectedUrl,
      }
    }
  };
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
            description: "Values inferred from the user's message or selected from the UI. Put known values here before asking the user for more. Use only DB-backed enums/descriptions."
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
          enum: ["GISTDA", "VALLARIS"],
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
    description: "Edit the latest map style using the existing map_style as the base. Use this when the user wants to change color, size, width, opacity, paint, or layout without calling get_map_layer again. Colors should be provided as catalog colorKeys or a hex colorValue.",
    parameters: {
      type: "object",
      properties: {
        layerId: {
          type: "string",
          description: "The layerId to edit. If omitted, the latest layer is used."
        },
        target: {
          type: "string",
          enum: ["point", "circle", "line", "polygon", "fill", "symbol", "heatmap", "raster"],
          description: "The layer type to edit. If omitted, layers from the latest map_style are used."
        },
        instruction: {
          type: "string",
          description: "The original user instruction"
        },
        colorKey: {
          type: "string",
          description: "Primary color from the catalog, such as black, red, or gray"
        },
        colorKeys: {
          type: "array",
          items: { type: "string" },
          description: "Multiple catalog colors for mixing, such as [black, gray]"
        },
        mix: {
          type: "array",
          items: { type: "number" },
          description: "Color mixing weights, such as [0.75, 0.25]"
        },
        colorValue: {
          type: "string",
          description: "A validated hex color, such as #1F2937. Use when the user gives a specific color or the chatbot chooses a mixed color."
        },
        radius: {
          type: "number",
          description: "Circle radius for point/circle layers"
        },
        size: {
          type: "number",
          description: "Size value, used as circle radius or line width"
        },
        width: {
          type: "number",
          description: "line width"
        },
        opacity: {
          type: "number",
          description: "Opacity from 0-1 or 0-100"
        },
        paint: {
          type: "object",
          description: "MapLibre paint properties to merge directly"
        },
        layout: {
          type: "object",
          description: "MapLibre layout properties to merge directly"
        }
      },
      required: []
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
          enum: ["GISTDA", "VALLARIS"],
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
        layerConfigTemplate: true
      },
      orderBy: [
        { provider: "asc" },
        { intentName: "asc" }
      ]
    });
    const configs = filterConfigsByQuery(
      filterConfigsByProviders(allConfigs, allowedProviders),
      getMapQuery(aiArgs)
    );
    let intentName = aiArgs.intentName?.trim();
    let provider = normalizeProvider(aiArgs.provider) || undefined;

    if (!provider && allowedProviders.length === 1) {
      provider = allowedProviders[0];
    }

    if (!intentName) {
      const intentMatches = provider
        ? configs.filter((config) => sameProvider(config.provider, provider))
        : configs;
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
      const intentChoices = configs.map((config) => ({
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
      const userApiKey = allowedKeys.find((apiKey) => sameProvider(apiKey.provider, config.provider));
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

      return buildVectorTileOptionsPayload(config, aiArgs, decryptedApiKey);
    }

    if (isVallarisProvider(config.provider)) {
      const userApiKey = allowedKeys.find((apiKey) => sameProvider(apiKey.provider, config.provider));
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

      return buildVallarisOptionsPayload(config, aiArgs, decryptedApiKey);
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

    const userApiKey = (await resolveUserMapApiKeys(userId, headerApiKey))
      .find((apiKey) => sameProvider(apiKey.provider, config.provider));

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
      return buildVectorTileLayerPayload(config, aiArgs, decryptedApiKey);
    }

    if (isVallarisProvider(config.provider)) {
      return buildVallarisLayerPayload(config, aiArgs, decryptedApiKey);
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

    const finalUrl = replaceTemplateVariables(
      `${config.baseUrl}${config.urlTemplate}`,
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
  );
};

export const handleCheckMapAccess = async (userId: string, headerApiKey?: string, query?: string) => {
  try {
    const userKeys = await resolveUserMapApiKeys(userId, headerApiKey);

    if (userKeys.length === 0) {
      return {
        success: false,
        allowedProviders: [],
        configs: [],
        message: headerApiKey?.trim()
          ? "The x-api-key header does not match any active API key for this user."
          : "The user has not linked any map API keys. Please advise the user to configure their API keys in the settings."
      };
    }

    const allowedProviders = getUniqueProviders(userKeys.map((key) => key.provider));

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
    const configs = filterConfigsByQuery(
      filterConfigsByProviders(allConfigs, allowedProviders),
      query
    );

    if (configs.length === 0) {
      return {
        success: false,
        allowedProviders,
        configs: [],
        message: "The user has map API keys, but no active mapconfig exists for those providers."
      };
    }

    return {
      success: true,
      allowedProviders,
      configs: configs.map(({ layerConfigTemplate, ...config }) => config)
    };
  } catch (error) {
    console.error("Check Map Access Error:", error);
    return {
      success: false,
      allowedProviders: [],
      configs: [],
      message: "An error occurred while retrieving map access permissions from the database."
    };
  }
};
