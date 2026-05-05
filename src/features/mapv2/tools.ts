import { MapLayerService } from "./service";
import type {
  HazardType,
  MapLayerType,
  DayRange,
  GetMapLayerCatalogInput,
} from "./type";

const service = new MapLayerService();

type MapLayerToolInput = {
  message?: string;
  hazard?: HazardType;
  days?: DayRange;
  type?: MapLayerType;
  url?: string;
  apiKey?: string;
  vectorApiKey?: string;
};

const extractVectorTileUrl = (message: string): string | undefined => {
  const match = message.match(/https?:\/\/[^\s"'<>]+\/core\/api\/tiles\/[^\s"'<>]+/i);
  return match?.[0]?.replace(/[),.;]+$/, "");
};

const extractApiKeyFromUrl = (url?: string): string | undefined => {
  if (!url) return undefined;

  try {
    const parsedUrl = new URL(url);
    return parsedUrl.searchParams.get("api_key")?.trim()
      || parsedUrl.searchParams.get("apikey")?.trim()
      || parsedUrl.searchParams.get("apiKey")?.trim()
      || undefined;
  } catch {
    return undefined;
  }
};

export function parseMapIntent(message: string): GetMapLayerCatalogInput {
  const lower = message.toLowerCase();

  let days: DayRange = 7;;

  if (lower.includes("1 วัน") || lower.includes("1 day")) days = 1;
  if (lower.includes("3 วัน") || lower.includes("3 day")) days = 3;
  if (lower.includes("7 วัน") || lower.includes("7 day")) days = 7;
  if (lower.includes("30 วัน") || lower.includes("30 day")) days = 30;

  let hazard: HazardType = "viirs";

  if (
    lower.includes("ไฟป่า") ||
    lower.includes("ไฟไหม้") ||
    lower.includes("fire") ||
    lower.includes("hotspot") ||
    lower.includes("viirs")
  ) {
    hazard = "viirs";
  } else if (
    lower.includes("น้ำท่วม") ||
    lower.includes("flood") ||
    lower.includes("อุทกภัย")
  ) {
    hazard = "flood";
  } else if (
    lower.includes("ภัยแล้ง") ||
    lower.includes("แล้ง") ||
    lower.includes("drought") ||
    lower.includes("dri")
  ) {
    hazard = "dri";
  }
  let type: MapLayerType = "wms";
  if (lower.includes("tms") ) type = "tms";
  if (lower.includes("wmts")) type = "wmts";
  if (
    lower.includes("vector") ||
    lower.includes("เวกเตอร์") ||
    lower.includes("mvt") ||
    lower.includes("pbf") ||
    lower.includes("vallaris.dragonfly.gistda.or.th") ||
    lower.includes("/core/api/tiles/")
  ) {
    type = "vector";
  }

  const url = extractVectorTileUrl(message);

  return {
    hazard,
    days,
    type,
    url,
  };
}
export const get_map_layer_catalog = {
  name: "get_map_layer_catalog",
  description: "Get map layer WMS/WMTS/TMS/Vector Tiles for hazard visualization",

  parameters: {
    type: "object",
    properties: {
      message: { type: "string" },
      hazard: { type: "string", enum: ["viirs", "flood", "dri"] },
      days: { type: "number", enum: [1, 3, 7, 30] },
      type: { type: "string", enum: ["wms", "wmts", "tms", "vector"] },
      url: { type: "string" },
      vectorApiKey: { type: "string" },
    },
    required: [],
  },

  execute: async (input: MapLayerToolInput) => {
    const parsedIntent = input.message ? parseMapIntent(input.message) : undefined;
    const intent: GetMapLayerCatalogInput = {
      hazard: (input.hazard ?? parsedIntent?.hazard ?? "viirs") as HazardType,
      days: (input.days ?? parsedIntent?.days ?? 7) as DayRange,
      type: (input.type ?? parsedIntent?.type ?? "wms") as MapLayerType,
      url: input.url ?? parsedIntent?.url,
      apiKey: input.vectorApiKey ?? parsedIntent?.apiKey,
    };

    const { apiKey: _apiKey, ...safeIntent } = intent;
    console.log("[MAP LAYER INTENT]", safeIntent);

    const result = await service.getLayerCatalog(intent, input.apiKey, input.vectorApiKey);

    return {
      event: "layer_catalog",
      layer: result,
    };
  },
};
