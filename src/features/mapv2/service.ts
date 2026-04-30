import { GetMapLayerCatalogInput, MapLayerCatalogResult } from "./type";


const BASE = "https://api-gateway.gistda.or.th/api/2.0/resources/maps";

export class MapLayerService {
  async getLayerCatalog(input: GetMapLayerCatalogInput, apiKey?: string): Promise<MapLayerCatalogResult> {
    const { hazard, days, type } = input;
    const dayPath = days === 1 ? "1day" : `${days}days`;
    const url  = `${BASE}/${hazard}/${dayPath}/${type}`;

    if (type === "tms") {
      return {
        hazard,
        days,
        type,
        url,
      };
    }

    const resolvedApiKey = apiKey?.trim();

    if (!resolvedApiKey) {
      throw new Error("X-API-Key header is required");
    }

    const requestUrl = this.buildRequestUrl(url, type);

    const response = await fetch(requestUrl, {
      headers: {
        "API-Key": resolvedApiKey,
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

  private buildRequestUrl(url: string, type: GetMapLayerCatalogInput["type"]): string {
    if (type === "wms") {
      return `${url}?service=WMS&request=GetCapabilities`;
    }

    if (type === "wmts") {
      return `${url}?service=WMTS&request=GetCapabilities`;
    }

    return url;
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
export const mapLayerService = new MapLayerService();
