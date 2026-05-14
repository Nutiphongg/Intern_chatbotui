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
  renderConfig?: unknown;
};

let styleCatalogCache: {
  expiresAt: number;
  sourceUrl: string;
  entries: StyleCatalogEntry[];
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

const extractStyleCatalogEntries = (payload: unknown): StyleCatalogEntry[] => {
  const catalog = pickRecord(payload);
  return Object.entries(catalog)
    .map(([key, value]): StyleCatalogEntry | undefined => {
      const record = pickRecord(value);
      if (Object.keys(record).length === 0) return undefined;
      const renderConfig = pickRecord(record.renderConfig);
      const layerType = toStringValue(renderConfig.layerType);

      return {
        key,
        description: toStringValue(record.description),
        layerType,
        renderConfig
      };
    })
    .filter((entry): entry is StyleCatalogEntry => Boolean(entry));
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
      styles: styleCatalogCache.entries
    };
  }

  try {
    const response = await fetch(sourceUrl, {
      headers: {
        Accept: "application/json"
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

    const payload = await response.json();
    const entries = extractStyleCatalogEntries(payload);
    styleCatalogCache = {
      sourceUrl,
      entries,
      expiresAt: Date.now() + STYLE_CATALOG_CACHE_TTL_MS
    };

    return {
      success: true,
      sourceUrl,
      styles: entries
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
    entry.layerType
  ].map(normalizeStyleMatchText).filter(Boolean);

  return entryTerms.reduce((score, entryTerm) => {
    const matchedLayerTerm = layerTerms.find((layerTerm) => {
      return layerTerm.includes(entryTerm) || entryTerm.includes(layerTerm);
    });
    return score + (matchedLayerTerm ? Math.min(entryTerm.length, matchedLayerTerm.length) : 0);
  }, 0);
};

const chooseStyleCatalogEntry = (
  entries: StyleCatalogEntry[],
  layer: Record<string, unknown>,
  presetKey?: string,
  instruction?: string
): StyleCatalogEntry | undefined => {
  if (presetKey) {
    const explicit = entries.find((entry) => entry.key === presetKey);
    if (explicit) return explicit;
  }

  const geometryLayerType = geometryTypeToMapLayerType(layer.geometryType)
    || geometryTypeToMapLayerType(layer.geometry)
    || geometryTypeToMapLayerType(layer.geomType);
  const candidateEntries = geometryLayerType
    ? entries.filter((entry) => entry.layerType === geometryLayerType)
    : entries;

  return (candidateEntries.length > 0 ? candidateEntries : entries)
    .map((entry) => ({
      entry,
      score: scoreStyleCatalogEntry(entry, layer, instruction)
    }))
    .sort((left, right) => right.score - left.score)[0]?.entry;
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
      message: catalog.message || "ไม่พบ style catalog ที่ใช้งานได้ครับ"
    };
  }

  const layer = getMapLayerRecord(layerPayload);
  const layerId = toStringValue(layer.layerId) || toStringValue(layer.id);
  const selectedEntry = chooseStyleCatalogEntry(catalog.styles, layer, options.presetKey, options.instruction);
  const renderConfig = pickRecord(selectedEntry?.renderConfig);
  const layerType = toStringValue(renderConfig.layerType);

  if (!layerId || !selectedEntry || !layerType) {
    return {
      success: false,
      event: "map_style",
      layerId,
      message: "ไม่สามารถเลือก style preset ที่เหมาะกับ layer นี้ได้ครับ"
    };
  }

  const cleanLayerId = safeMapStyleId(layerId);
  const sourceId = `source-${cleanLayerId}`;
  const sourceType = layerType === "raster" ? "raster" : "vector";
  const sourceLayer = toStringValue(layer.sourceLayer)
    || toStringValue(layer.source_layer)
    || toStringValue(layer.layerName)
    || toStringValue(layer.name)
    || layerId;

  const mapLayer: Record<string, unknown> = {
    id: `layer-${cleanLayerId}-${layerType}`,
    type: layerType,
    source: sourceId,
    ...(sourceType === "vector" ? { "source-layer": sourceLayer } : {}),
    ...(Object.keys(pickRecord(renderConfig.layout)).length > 0 ? { layout: pickRecord(renderConfig.layout) } : {}),
    ...(Object.keys(pickRecord(renderConfig.paint)).length > 0 ? { paint: pickRecord(renderConfig.paint) } : {})
  };

  return {
    success: true,
    event: "map_style",
    layerId,
    preset: selectedEntry.key,
    description: selectedEntry.description,
    sourceId,
    sourceType,
    layers: [mapLayer]
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
    return "สนใจข้อมูลประเภทไหน ย้อนหลังกี่วัน และต้องการรูปแบบแผนที่แบบไหนครับ";
  }

  const questionParts = keys.map((key) => {
    if (key === "hazard") return "สนใจข้อมูลประเภทไหน";
    if (key === "dayPath" || key === "days") return "ต้องการย้อนหลังกี่วัน";
    if (key === "type") return "ต้องการรูปแบบแผนที่แบบไหน";
    if (key === "layerId") return "ต้องการใช้ layer ไหน";
    if (key === "intentName") return "ต้องการแผนที่แบบไหน";
    if (key === "provider") return "ต้องการใช้ provider ไหน";
    return `ขอค่า ${key}`;
  });

  return `${questionParts.join(" และ")}ครับ`;
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
      ? `ผมเจอหลาย style ที่อาจเกี่ยวกับ "${context.query}" เลือกอันที่ต้องการก่อนครับ`
      : "ต้องการใช้ style ไหนของ VALLARIS ครับ";
  }

  return "ต้องการรูปแบบแผนที่แบบไหนครับ";
};

const buildMapOptionDescription = (
  code: MapQuestionCode,
  context: Record<string, string | undefined> = {}
): string => {
  if (code === "select_vallaris_style") {
    return context.query
      ? `เลือก style ที่ตรงกับ "${context.query}"`
      : "เลือก style ที่ต้องการใช้";
  }

  return context.styleTitle
    ? `เลือก type สำหรับ ${context.styleTitle}`
    : "เลือก type สำหรับ style ที่เลือก";
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

const VECTOR_TILE_CHOICE_LIMIT = 8;

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

const buildVectorTileUrl = (
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

const normalizeGeometryType = (value: unknown): string | undefined => {
  const geometry = toStringValue(value)?.toLowerCase();
  if (!geometry) return undefined;
  if (geometry.includes("point")) return "point";
  if (geometry.includes("line")) return "line";
  if (geometry.includes("polygon")) return "polygon";
  if (geometry.includes("raster") || geometry.includes("image") || geometry.includes("coverage")) return "raster";
  return geometry;
};

const geometryTypeToMapLayerType = (value: unknown): string | undefined => {
  const geometry = normalizeGeometryType(value);
  if (geometry === "point") return "circle";
  if (geometry === "line") return "line";
  if (geometry === "polygon") return "fill";
  if (geometry === "raster") return "raster";
  return undefined;
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
  const collectionsUrl = buildVectorTileUrl(
    config.baseUrl,
    config.urlTemplate,
    apiKey,
    pickRecord(template.collectionQuery)
  );
  const collections = extractVectorTileCollections(await fetchVectorTileJson(collectionsUrl));
  const selectedLayerId = getVectorTileLayerId(aiArgs, optionKey);
  const selectedCollection = selectedLayerId
    ? collections.find((collection) => collection.id === selectedLayerId)
    : undefined;

  if (collections.length === 0) {
    return {
      success: false,
      needInfo: false,
      missingKeys: [],
      options: [],
      complete: false,
      intentName: config.intentName,
      provider: config.provider,
      message: "ไม่พบรายการ vector tile layer จาก VALLARIS ครับ"
    };
  }

  if (!selectedCollection) {
    const layerOption: MapOptionInfo = {
      key: optionKey,
      required: true,
      source: "template",
      label: "Layer",
      description: `เลือก ${layerType} layer ที่ต้องการใช้`,
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
  config: { baseUrl: string; urlTemplate: string },
  aiArgs: MapToolArgs,
  apiKey: string
): Promise<{
  styles: EnrichedVallarisStyle[];
  matches: VallarisStyleMatch[];
  selectedStyle?: EnrichedVallarisStyle;
  selectedMatch?: VallarisStyleMatch;
  query?: string;
}> => {
  const styles = await getVallarisStyles(config, apiKey);
  const selectedStyleId = getSelectedVallarisStyleId(aiArgs);
  const query = getMapQuery(aiArgs);
  const matches = rankVallarisStyles(styles, query);
  const selectedStyle = selectedStyleId
    ? styles.find((style) => style.id === selectedStyleId)
    : undefined;

  if (selectedStyle) {
    return {
      styles,
      matches,
      selectedStyle,
      selectedMatch: matches.find((match) => match.style.id === selectedStyle.id),
      query
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
      query
    };
  }

  return { styles, matches, query };
};

const buildVallarisOptionsPayload = async (
  config: { intentName: string; provider: string; baseUrl: string; urlTemplate: string },
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
      message: "ไม่พบรายการ style จาก VALLARIS ครับ"
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
      message: "ไม่พบ link แผนที่ใน style ที่เลือกจาก VALLARIS ครับ"
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
      message: "ไม่สามารถเตรียม URL ภายในสำหรับแผนที่ VALLARIS ที่เลือกได้ครับ"
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
    return { success: false, error: "layerConfigTemplate.detailUrlTemplate สำหรับ collection detail ยังไม่ได้ตั้งค่าครับ" };
  }

  const tileDetailUrl = buildVectorTileUrl(config.baseUrl, detailUrlTemplate, apiKey, { id: layerId, layerId });
  const tilePayload = pickRecord(await fetchVectorTileJson(tileDetailUrl));
  const tileRecord = isRecord(tilePayload.data) ? tilePayload.data : tilePayload;
  const minzoom = toNumberValue(tileRecord.minzoom ?? tileRecord.minZoom);
  const maxzoom = toNumberValue(tileRecord.maxzoom ?? tileRecord.maxZoom);
  const center = toNumberArray(tileRecord.center);
  const bounds = toNumberArray(tileRecord.bounds);
  const tiles = toPublicStringArray(tileRecord.tiles)
    || toPublicStringArray(tileRecord.tileUrls)
    || toPublicStringArray(tileRecord.tile_urls);
  const template = pickRecord(config.layerConfigTemplate);
  const geometryType = getDirectGeometryType(tileRecord)
    || getDirectGeometryType(template);
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
          `ค่าของ ${key} ที่ต้องใช้เติม map URL/template`
        )
      ])
  );

  return {
    type: "function",
    function: {
      name: "map_options",
      description: "ใช้ทันทีเมื่อ user ขอแผนที่ เพื่อดึง/ตรวจตัวเลือกจาก mapconfig ใน DB ห้ามเดา choice เอง ถ้า provider เป็น VALLARIS ให้ส่งคำขอ user ใน query/message เพื่อให้ backend ดึง style list, enrich metadata/stylesheet, match styleId แล้วคืน map type links",
      parameters: {
        type: "object",
        properties: {
          query: {
            type: "string",
            description: "ข้อความล่าสุดของ user ที่ใช้อธิบายข้อมูลแผนที่ที่ต้องการ เช่น แผ่นดินไหว น้ำท่วม หรือชื่อ layer โดยเฉพาะสำหรับ VALLARIS style matching"
          },
          message: {
            type: "string",
            description: "alias ของ query"
          },
          intentName: createStringEnumProperty(
            configs.map((config) => config.intentName),
            "ชื่อ intent จาก mapconfig ที่ตรงกับคำขอแผนที่"
          ),
          provider: createStringEnumProperty(
            getUniqueProviders(configs.map((config) => config.provider)),
            "provider ที่ user มีสิทธิ์ใช้งาน"
          ),
          params: {
            type: "object",
            properties: paramsProperties,
            description: "ค่าที่ infer ได้จากคำพูด user หรือค่าที่ user เลือกจาก UI ต้องใส่ค่าที่รู้แล้วมาที่นี่ก่อนถาม user ต่อ โดยเลือกจาก enum/description ที่มาจาก DB เท่านั้น"
          },
          options: {
            type: "object",
            properties: paramsProperties,
            description: "alias ของ params"
          },
          variables: {
            type: "object",
            properties: paramsProperties,
            description: "ตัวแปรเพิ่มเติมสำหรับเติม template"
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
    description: "สร้าง URL และ layer payload จาก mapconfig กลางและ option values ที่เลือกแล้ว",
    parameters: {
      type: "object",
        properties: {
        query: {
          type: "string",
          description: "ข้อความล่าสุดของ user สำหรับค้นหา/เลือก style โดยเฉพาะ provider VALLARIS"
        },
        message: {
          type: "string",
          description: "alias ของ query"
        },
        intentName: {
          type: "string",
          description: "ชื่อ intent จาก mapconfig ที่ได้จาก map_access"
        },
        provider: {
          type: "string",
          enum: ["GISTDA", "VALLARIS"],
          description: "provider จาก map_access ที่ user มีสิทธิ์ใช้งาน"
        },
        params: {
          type: "object",
          description: "ค่าที่ chatbot infer เพื่อแทน placeholder ใน urlTemplate/layerConfigTemplate"
        },
        options: {
          type: "object",
          description: "alias ของ params สำหรับค่าที่ chatbot infer"
        },
        variables: {
          type: "object",
          description: "ตัวแปรเพิ่มเติมสำหรับแทน placeholder ใน template"
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
    description: "ดึง catalog style preset สำหรับแต่งแผนที่จาก STYLE_CATALOG_URL เช่น polygon, line, point, raster คืน key/description/layerType ให้ chatbot เลือกใช้ ห้ามเดา style เองถ้ามี catalog ให้เรียก tool นี้ก่อน",
    parameters: {
      type: "object",
      properties: {},
      required: []
    }
  }
};

export const mapOptionToolSchema = {
  type: "function",
  function: {
    name: "map_options",
    description: "ตรวจและคืนตัวเลือกแผนที่จาก layerConfigTemplate ใน DB เพื่อให้ user เลือกก่อนเรียก get_map_layer",
    parameters: {
      type: "object",
        properties: {
        query: {
          type: "string",
          description: "ข้อความล่าสุดของ user สำหรับค้นหา style/layer โดยเฉพาะ provider VALLARIS"
        },
        message: {
          type: "string",
          description: "alias ของ query"
        },
        intentName: {
          type: "string",
          description: "ชื่อ intent จาก mapconfig ถ้ารู้แล้ว"
        },
        provider: {
          type: "string",
          enum: ["GISTDA", "VALLARIS"],
          description: "provider จาก map_access ถ้ารู้แล้ว"
        },
        params: {
          type: "object",
          description: "ค่าที่ chatbot infer จากคำตอบ user แล้ว"
        },
        options: {
          type: "object",
          description: "alias ของ params สำหรับค่าที่ chatbot infer จากคำตอบ user แล้ว"
        },
        variables: {
          type: "object",
          description: "ตัวแปรเพิ่มเติมที่มีอยู่แล้ว"
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
        message: `ผู้ใช้ไม่มีสิทธิ์ใช้งาน provider ${provider} ครับ`
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
        message: `ไม่พบ mapconfig ที่เปิดใช้งานสำหรับ ${provider}:${intentName}`
      };
    }

    if (isVallarisProvider(config.provider) && isCollectionDetailConfig(config.layerConfigTemplate)) {
      const userApiKey = allowedKeys.find((apiKey) => sameProvider(apiKey.provider, config.provider));
      if (!userApiKey) {
        return {
          success: false,
          options: [],
          message: `ผู้ใช้ไม่มี API Key ที่ใช้งานได้สำหรับ provider ${config.provider} ครับ`
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
          message: "เกิดข้อผิดพลาดในการอ่าน API Key ของ VALLARIS"
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
          message: `ผู้ใช้ไม่มี API Key ที่ใช้งานได้สำหรับ provider ${config.provider} ครับ`
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
          message: "เกิดข้อผิดพลาดในการอ่าน API Key ของ VALLARIS"
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
      return { error: "AI ส่งข้อมูล map tool ไม่ครบ ต้องมี intentName และ provider ครับ" };
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
        error: `ไม่พบ mapconfig สำหรับคำสั่ง ${intentName} ของผู้ให้บริการ ${provider} ครับ`
      };
    }

    if (!config.isActive) {
      return {
        error: `mapconfig ${config.intentName} ของ ${config.provider} ถูกปิดใช้งานครับ`
      };
    }

    const userApiKey = (await resolveUserMapApiKeys(userId, headerApiKey))
      .find((apiKey) => sameProvider(apiKey.provider, config.provider));

    if (!userApiKey) {
      return {
        error: headerApiKey?.trim()
          ? `API Key ที่ส่งมาทาง header ไม่ตรงกับ provider ${config.provider} หรือไม่มีสิทธิ์ใช้งานครับ`
          : `ผู้ใช้ยังไม่ได้ผูก API Key สำหรับ ${config.provider} กรุณาตั้งค่า API Key ก่อนครับ`
      };
    }

    let decryptedApiKey = "";
    try {
      decryptedApiKey = decryptUserApiKey(userApiKey);
    } catch (error) {
      console.error("Decrypt map API key error:", error);
      return { error: "เกิดข้อผิดพลาดในการอ่าน API Key ของคุณ อาจมีการตั้งค่าผิดพลาด" };
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
        error: `ข้อมูลสำหรับสร้าง URL แผนที่ยังไม่ครบครับ URL/template ยังมีตัวแปรที่ไม่ได้แทนค่า: ${finalUrl}`
      };
    }

    let finalLayerConfig: Record<string, unknown>;
    try {
      finalLayerConfig = JSON.parse(configString);
    } catch (error) {
      console.error("Map layerConfigTemplate parse error:", error);
      return { error: "layerConfigTemplate ของ mapconfig ไม่ใช่ JSON ที่ใช้งานได้หลังแทนค่าครับ" };
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
    return { error: "ระบบฐานข้อมูลแผนที่ขัดข้องชั่วคราวครับ" };
  }
};

export const checkMapAccessSchema = {
  type: "function",
  function: {
    name: "check_user_map",
    description: "ตรวจสอบ provider API keys ของ user และดึง mapconfig กลางที่ user มีสิทธิ์ใช้งาน",
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



