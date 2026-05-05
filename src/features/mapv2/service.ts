import { GetMapLayerCatalogInput, MapLayerCatalogResult } from "./type";
import { env } from "../../lib/env";
//usermapconfig
import {prisma} from "../setup/prisma";
import { CreateMapConfigPayload,CreateApiKeyPayload} from '../mapv2/interface';
import { encrypt } from "./encryption";
import { ulid } from "ulid";


const normalizeConfiguredUrl = (value: string): string => {
  return value
    .trim()
    .replace(/;+\s*$/, "")
    .replace(/^["']|["']$/g, "")
    .replace(/\/+$/, "");
};

const BASE = normalizeConfiguredUrl(env.GISTDA_API_BASE_URL);
const VALLARIS_TILE_URL = normalizeConfiguredUrl(env.VALLARIS_URL);

export class MapLayerService {
  async getLayerCatalog(
    input: GetMapLayerCatalogInput,
    apiKey?: string,
    vectorApiKey?: string
  ): Promise<MapLayerCatalogResult> {
    const { hazard, days, type } = input;
    const dayPath = days === 1 ? "1day" : `${days}days`;
    const url = this.buildLayerUrl(hazard, dayPath, type, input.url);

    if (type === "tms") {
      return {
        hazard,
        days,
        type,
        url,
      };
    }

    if (type === "vector") {
      return this.getVectorTileCatalog(input, url, apiKey, vectorApiKey);
    }

    const resolvedApiKey = apiKey?.trim();

    if (!resolvedApiKey) {
      throw new Error("X-API-Key header env is required");
    }

    const requestUrl = this.buildRequestUrl(url, type);

    const response = await fetch(requestUrl, {
      headers: {
        "API-Key": resolvedApiKey,
        "X-API-Key": resolvedApiKey,
        Accept: "application/json, application/xml, text/xml",
      },
    });

    if (!response.ok) {
      throw new Error(
        `Map layer catalog request failed: ${response.status} ${response.statusText}`
      );
    }

    const raw = await response.text();
    const payload = this.parseJson(raw);
    const fallbackLayerName = `${hazard}_${dayPath}_${type}`;
    const layerName = this.extractString(payload, ["layerName", "layer_name", "name", "id", "_id"])
      || this.extractLayerName(raw)
      || fallbackLayerName;

    return {
      hazard,
      days,
      type,
      url,
      layerName,
    };
  }

  private async getVectorTileCatalog(
    input: GetMapLayerCatalogInput,
    url: string,
    apiKey?: string,
    vectorApiKey?: string
  ): Promise<MapLayerCatalogResult> {
    const { cleanUrl, apiKeyFromUrl } = this.removeApiKeyFromUrl(url);
    const resolvedApiKey = vectorApiKey?.trim()
      || input.apiKey?.trim()
      || apiKey?.trim()
      || apiKeyFromUrl
    if (!resolvedApiKey) {
      throw new Error("X-Vector-API-Key header or api_key query is required");
    }

    const requestUrl = this.buildVectorRequestUrl(cleanUrl, resolvedApiKey);
    const response = await fetch(requestUrl, {
      headers: {
        "API-Key": resolvedApiKey,
        "X-API-Key": resolvedApiKey,
        Accept: "application/json",
      },
    });

    if (!response.ok) {
      throw new Error(
        `Vector tile catalog request failed: ${response.status} ${response.statusText}`
      );
    }

    const raw = await response.text();
    const payload = this.parseJson(raw);

    return {
      hazard: input.hazard,
      days: input.days,
      type: input.type,
      url: cleanUrl,
      basename: this.extractString(payload, ["basename", "baseName"]),
      minzoom: this.extractNumber(payload, ["minzoom", "minZoom"]),
      maxzoom: this.extractNumber(payload, ["maxzoom", "maxZoom"]),
      center: this.extractNumberArray(payload, ["center"]),
      bounds: this.extractNumberArray(payload, ["bounds"]),
      tiles: this.extractStringArray(payload, ["tiles"])?.map((tileUrl) => this.removeApiKeyFromUrl(tileUrl).cleanUrl),
    };
  }

  private buildLayerUrl(
    hazard: string,
    dayPath: string,
    type: GetMapLayerCatalogInput["type"],
    inputUrl?: string
  ): string {
    if (type === "vector") {
      return normalizeConfiguredUrl(inputUrl || VALLARIS_TILE_URL);
    }

    return [BASE, hazard, dayPath, type]
      .map((part) => String(part).replace(/^\/+|\/+$/g, ""))
      .join("/");
  }

  private buildRequestUrl(url: string, type: GetMapLayerCatalogInput["type"]): string {
    if (type === "wms") {
      return `${url}?service=WMS&version=1.3.0&request=GetCapabilities`;
    }

    if (type === "wmts") {
      return `${url}?service=WMTS&version=1.0.0&request=GetCapabilities`;
    }

    return url;
  }

  private removeApiKeyFromUrl(url: string): { cleanUrl: string; apiKeyFromUrl?: string } {
    try {
      const parsedUrl = new URL(url);
      const apiKeyFromUrl = parsedUrl.searchParams.get("api_key")?.trim()
        || parsedUrl.searchParams.get("apikey")?.trim()
        || parsedUrl.searchParams.get("apiKey")?.trim()
        || undefined;

      parsedUrl.searchParams.delete("api_key");
      parsedUrl.searchParams.delete("apikey");
      parsedUrl.searchParams.delete("apiKey");

      return {
        cleanUrl: this.normalizeTileTemplateUrl(parsedUrl.toString()),
        apiKeyFromUrl,
      };
    } catch {
      return { cleanUrl: normalizeConfiguredUrl(url) };
    }
  }

  private normalizeTileTemplateUrl(url: string): string {
    return normalizeConfiguredUrl(url)
      .replace(/%7B/gi, "{")
      .replace(/%7D/gi, "}");
  }

  private buildVectorRequestUrl(url: string, apiKey: string): string {
    const parsedUrl = new URL(url);
    parsedUrl.searchParams.set("api_key", apiKey);
    return parsedUrl.toString();
  }

  private parseJson(raw: string): unknown {
    try {
      return JSON.parse(raw);
    } catch {
      return undefined;
    }
  }

  private extractString(value: unknown, keys: string[]): string | undefined {
    if (!value || typeof value !== "object") return undefined;

    if (Array.isArray(value)) {
      for (const item of value) {
        const match = this.extractString(item, keys);
        if (match) return match;
      }
      return undefined;
    }

    const record = value as Record<string, unknown>;
    for (const key of keys) {
      const directValue = record[key];
      if (typeof directValue === "string" && directValue.trim()) {
        return directValue.trim();
      }
    }

    for (const nestedValue of Object.values(record)) {
      const match = this.extractString(nestedValue, keys);
      if (match) return match;
    }

    return undefined;
  }

  private extractNumber(value: unknown, keys: string[]): number | undefined {
    if (!value || typeof value !== "object") return undefined;

    if (Array.isArray(value)) {
      for (const item of value) {
        const match = this.extractNumber(item, keys);
        if (match !== undefined) return match;
      }
      return undefined;
    }

    const record = value as Record<string, unknown>;
    for (const key of keys) {
      const directValue = record[key];
      if (typeof directValue === "number" && Number.isFinite(directValue)) {
        return directValue;
      }
      if (typeof directValue === "string" && directValue.trim()) {
        const parsed = Number(directValue);
        if (Number.isFinite(parsed)) return parsed;
      }
    }

    for (const nestedValue of Object.values(record)) {
      const match = this.extractNumber(nestedValue, keys);
      if (match !== undefined) return match;
    }

    return undefined;
  }

  private extractNumberArray(value: unknown, keys: string[]): number[] | undefined {
    const arrayValue = this.extractArray(value, keys);
    if (!arrayValue) return undefined;

    const numbers = arrayValue
      .map((item) => (typeof item === "number" ? item : Number(item)))
      .filter((item) => Number.isFinite(item));

    return numbers.length === arrayValue.length ? numbers : undefined;
  }

  private extractStringArray(value: unknown, keys: string[]): string[] | undefined {
    const arrayValue = this.extractArray(value, keys);
    if (!arrayValue) return undefined;

    const strings = arrayValue
      .filter((item): item is string => typeof item === "string" && Boolean(item.trim()))
      .map((item) => item.trim());

    return strings.length ? strings : undefined;
  }

  private extractArray(value: unknown, keys: string[]): unknown[] | undefined {
    if (!value || typeof value !== "object") return undefined;

    if (Array.isArray(value)) {
      for (const item of value) {
        const match = this.extractArray(item, keys);
        if (match) return match;
      }
      return undefined;
    }

    const record = value as Record<string, unknown>;
    for (const key of keys) {
      const directValue = record[key];
      if (Array.isArray(directValue)) return directValue;
    }

    for (const nestedValue of Object.values(record)) {
      const match = this.extractArray(nestedValue, keys);
      if (match) return match;
    }

    return undefined;
  }

  private extractLayerName(xml: string): string | undefined {
    const names: string[] = [];
    const regex = /<Name>(.*?)<\/Name>/g;
    let match: RegExpExecArray | null;

    while ((match = regex.exec(xml)) !== null) {
      if (match[1]?.trim()) {
        names.push(match[1].trim());
      }
    }

    // ปกติ Name แรกอาจเป็น service root ให้เลือกตัวท้าย/ตัว layer
    return names[names.length - 1];
  }
}
//usermapconfig

export const createMapConfig = async (data: CreateMapConfigPayload) => {
  // 1. ตรวจสอบ API Key (ถ้ามีการระบุเข้ามา)
  if (data.apiKeyId) {
    const apiKey = await prisma.userapikey.findUnique({
      where: { id: data.apiKeyId }
    });

    // เช็คว่ามี Key ไหม และเป็นของ User คนนี้จริงๆ หรือเปล่า
    if (!apiKey || apiKey.userId !== data.userId) {
      throw new Error('API Key not correct');
    }

    // [ไฮไลท์!] เช็คว่า Provider ของ Key ตรงกับ Provider ของ Config หรือไม่
    if (apiKey.provider !== data.provider) {
      throw new Error(`ไม่สามารถใช้ API Key ของ ${apiKey.provider} กับ Service ${data.provider} ได้`);
    }
  }

  // 2. เช็คว่า User เคยสร้าง Config ของ Intent และ Provider นี้ไปแล้วหรือยัง
  const existingConfig = await prisma.usermapconfig.findUnique({
    where: {
      userId_intentName_provider: {
        userId: data.userId,
        intentName: data.intentName,
        provider: data.provider
      }
    }
  });

  if (existingConfig) {
    throw new Error(`คุณมีการตั้งค่า ${data.intentName} สำหรับ ${data.provider} อยู่แล้ว`);
  }
  const id_config = ulid();
  // 3. บันทึกลง Database
  const newConfig = await prisma.usermapconfig.create({
    data: {
      id:id_config,
      userId: data.userId,
      intentName: data.intentName,
      provider: data.provider,
      baseUrl: data.baseUrl,
      urlTemplate: data.urlTemplate,
      // ถ้าไม่ได้ส่งมาให้เซฟเป็น Object ว่างๆ
      layerConfigTemplate: data.layerConfigTemplate || {}, 
      apiKeyId: data.apiKeyId || null
    }
  });

  return newConfig;
};
// userapikey
export const createApiKey = async (data: CreateApiKeyPayload) => {
  // 1. เช็คว่า User เคยตั้งชื่อ Key นี้ซ้ำใน Provider เดียวกันหรือไม่
  const existingKey = await prisma.userapikey.findUnique({
    where: {
      userId_provider_keyName: {
        userId: data.userId,
        provider: data.provider,
        keyName: data.keyName
      }
    }
  });

  if (existingKey) {
    throw new Error(`you have API Key name "${data.keyName}"  ${data.provider} `);
  }

  // 2. [พระเอกออกโรง] นำ keyValue ที่ Frontend ส่งมาไปเข้ารหัส 
  const { iv, encryptedKey } = encrypt(data.keyValue);
  const  id_apikey = ulid();
  // 3. บันทึกลง Database (เก็บแค่ตัวที่เข้ารหัสแล้วกับ IV)
  const newApiKey = await prisma.userapikey.create({
    data: {
      id: id_apikey,
      userId: data.userId,
      provider: data.provider,
      keyName: data.keyName,
      encryptedKey: encryptedKey,
      iv: iv
    }
  });

  // 4. [สำคัญด้าน Security] ส่งกลับไปให้ Frontend เฉพาะข้อมูลที่ปลอดภัย 
  // ห้ามส่ง encryptedKey หรือ keyValue กลับไปเด็ดขาด!
  return {
    id: id_apikey,
    provider: newApiKey.provider,
    keyName: newApiKey.keyName,
    createdAt: newApiKey.createdAt
  };
};
export const mapLayerService = new MapLayerService();

