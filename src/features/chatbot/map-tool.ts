import { gistdaDisasterService, gistdaViirsService } from "../map/service";
import { Errors } from "../../lib/errors";
import type {
  GetDisasterPointInput,
  GetDisasterPointResult,
  GetViirsHotspotsInput,
  GetViirsHotspotsResult
} from "../map/interface";
import type { BBox, DisasterLayerKind, ViirsConfidence, ViirsDayRange } from "../map/type";

export type ChatMapToolResult = {
  event: "hotspots";
  query: GetViirsHotspotsInput;
  result: GetViirsHotspotsResult;
  geojsonUrl: string;
} | {
  event: "disaster";
  query: GetDisasterPointInput;
  result: GetDisasterPointResult;
  geojsonUrl?: string;
};

const DEFAULT_HOTSPOT_LIMIT = 500;

const hasAny = (value: string, keywords: string[]): boolean => {
  return keywords.some((keyword) => value.includes(keyword));
};

const cleanTextValue = (value: string): string => {
  return value.trim().replace(/^["'“”]+|["'“”]+$/g, "");
};

const parseDays = (message: string): ViirsDayRange => {
  const explicit = message.match(/(?:ย้อนหลัง|last|past)?\s*(1|3|7|30)\s*(?:วัน|day|days)/i);
  if (explicit) return Number(explicit[1]) as ViirsDayRange;

  if (hasAny(message, ["เดือน", "month", "30d"])) return 30;
  if (hasAny(message, ["สัปดาห์", "อาทิตย์", "week", "7d"])) return 7;
  if (hasAny(message, ["3 วัน", "3 days", "3d"])) return 3;

  return 1;
};

const parseConfidence = (message: string): ViirsConfidence | undefined => {
  if (hasAny(message, ["high", "สูง", "ความมั่นใจสูง"])) return "high";
  if (hasAny(message, ["nominal", "กลาง", "ปานกลาง"])) return "nominal";
  if (hasAny(message, ["low", "ต่ำ", "ตํ่า", "ความมั่นใจต่ำ", "ความมั่นใจตํ่า"])) return "low";
  return undefined;
};

const parseLimit = (message: string): number | undefined => {
  const match = message.match(/(?:limit|จำนวน|เอา|แสดง)\s*(\d{1,5})/i);
  if (!match) return undefined;

  const value = Number(match[1]);
  if (!Number.isInteger(value) || value < 1) return undefined;
  return Math.min(value, 10_000);
};

const parseBBox = (message: string): BBox | undefined => {
  const match = message.match(/bbox\s*[:=]?\s*(-?\d+(?:\.\d+)?)\s*,\s*(-?\d+(?:\.\d+)?)\s*,\s*(-?\d+(?:\.\d+)?)\s*,\s*(-?\d+(?:\.\d+)?)/i);
  if (!match) return undefined;

  return [
    Number(match[1]),
    Number(match[2]),
    Number(match[3]),
    Number(match[4]),
  ];
};

const parseLanduse = (message: string): string | undefined => {
  const match = message.match(/(?:landuse|ประเภทพื้นที่)\s*[:=]?\s*([ก-๙a-zA-Z0-9_\-\s]+)/i);
  return match?.[1]?.trim();
};

const parseLatLon = (message: string): { lat: number; lon: number } | undefined => {
  const named = message.match(/lat\s*[:=]?\s*(-?\d+(?:\.\d+)?)\s*(?:,|\s+)\s*(?:lon|lng)\s*[:=]?\s*(-?\d+(?:\.\d+)?)/i);
  if (named) return { lat: Number(named[1]), lon: Number(named[2]) };

  const comma = message.match(/(?:พิกัด|coord|coordinate|coordinates)\s*[:=]?\s*(-?\d+(?:\.\d+)?)\s*,\s*(-?\d+(?:\.\d+)?)/i);
  if (!comma) return undefined;

  const first = Number(comma[1]);
  const second = Number(comma[2]);

  if (Math.abs(first) <= 90 && Math.abs(second) <= 180) {
    return { lat: first, lon: second };
  }
  if (Math.abs(second) <= 90 && Math.abs(first) <= 180) {
    return { lat: second, lon: first };
  }

  return undefined;
};


const detectDisasterKind = (message: string): DisasterLayerKind | undefined => {
  const normalized = message.toLowerCase();

  if (hasAny(normalized, ["ภัยแล้ง", "แล้ง", "drought"])) return "drought_recurrence";
  if (hasAny(normalized, ["น้ำท่วมซ้ำซาก", "flood recurrence", "flood_recurrence"])) return "flood_recurrence";
  if (hasAny(normalized, ["น้ำท่วม", "flood", "อุทกภัย"])) return "flood_extent";

  return undefined;
};
const PROVINCE_ALIASES: Record<string, string> = {
  phisanulok: "พิษณุโลก",
  khonkaen: "ขอนแก่น",
  phetchabun: "เพชรบูรณ์"
  
};

const parseProvince = (message: string): string | undefined => {
  const lower = message.toLowerCase();

  for (const [alias, province] of Object.entries(PROVINCE_ALIASES)) {
    if (lower.includes(alias.toLowerCase())) return province;
  }

  const match = message.match(/(?:จังหวัด|province|povince)\s*([ก-๙a-zA-Z\s]+)/i);
  return match?.[1] ? cleanTextValue(match[1]) : undefined;
};

const buildViirsGeoJsonUrl = (query: GetViirsHotspotsInput): string => {
  const params = new URLSearchParams();

  params.set("days", String(query.days));
  if (query.limit !== undefined) params.set("limit", String(query.limit));
  if (query.offset !== undefined) params.set("offset", String(query.offset));
  if (query.bbox) params.set("bbox", query.bbox.join(","));
  if (query.province) params.set("province", query.province);
  if (query.amphoe) params.set("amphoe", query.amphoe);
  if (query.tambol) params.set("tambol", query.tambol);
  if (query.pv_idn !== undefined) params.set("pv_idn", String(query.pv_idn));
  if (query.ap_idn !== undefined) params.set("ap_idn", String(query.ap_idn));
  if (query.tb_idn !== undefined) params.set("tb_idn", String(query.tb_idn));
  if (query.confidence) params.set("confidence", query.confidence);
  if (query.landuse) params.set("landuse", query.landuse);

  return `/geojson/viirs?${params.toString()}`;
};

const buildDisasterGeoJsonUrl = (query: GetDisasterPointInput): string => {
  const kind = query.kind.replace(/_/g, "-");
  const params = new URLSearchParams({
    lat: String(query.lat),
    lon: String(query.lon),
  });

  return `/map/disasters/${kind}?${params.toString()}`;
};

export const shouldUseHotspotsTool = (message: string): boolean => {
  const normalized = message.toLowerCase();
  const mapWords = ["map", "แผนที่", "render", "geojson", "layer"];
  const hotspotWords = ["hotspot", "hotspots", "จุดความร้อน", "ความร้อน", "ไฟป่า", "ไฟไหม้", "viirs", "gistda"];

  return hasAny(normalized, hotspotWords) || (
    hasAny(normalized, mapWords) && hasAny(normalized, ["ไฟ", "ความร้อน", "hot"])
  );
};

export const buildHotspotsInputFromMessage = (message: string): GetViirsHotspotsInput => {
  const normalized = message.toLowerCase();

  return {
    days: parseDays(normalized),
    limit: parseLimit(normalized) ?? DEFAULT_HOTSPOT_LIMIT,
    offset: 0,
    bbox: parseBBox(normalized),
    confidence: parseConfidence(normalized),
    landuse: parseLanduse(message),
    province: parseProvince(message),
  };
};

export const runHotspotsToolFromMessage = async (message: string): Promise<ChatMapToolResult | undefined> => {
  const disasterKind = detectDisasterKind(message);
  if (disasterKind) {
    const coordinates = parseLatLon(message);
    if (!coordinates) {
      throw Errors.badRequest("ต้องระบุพิกัด lat/lon เพื่อดูข้อมูลน้ำท่วมหรือภัยแล้งจาก GISTDA");
    }

    const query = {
      kind: disasterKind,
      lat: coordinates.lat,
      lon: coordinates.lon,
    };
    const result = await gistdaDisasterService.getPoint(query);

    return {
      event: "disaster",
      query,
      result,
      geojsonUrl: buildDisasterGeoJsonUrl(query),
    };
  }

  if (!shouldUseHotspotsTool(message)) return undefined;

  const query = buildHotspotsInputFromMessage(message);
  const result = await gistdaViirsService.getHotspots(query);

  return {
    event: "hotspots",
    query,
    result,
    geojsonUrl: buildViirsGeoJsonUrl(query),
  };
};
