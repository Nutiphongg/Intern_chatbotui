import { t, Static } from 'elysia';

export type MapLayerType = "wms" | "wmts" | "tms" | "vector";
export type HazardType = "viirs" | "flood" | "dri";
export type DayRange = 1 | 3 | 7 | 30;

export type GetMapLayerCatalogInput = {
  hazard: HazardType; 
  days: DayRange;
  type: MapLayerType;
  url?: string;
  apiKey?: string;
}

export type MapLayerCatalogResult = {
  hazard: string;
  days: number;
  type: MapLayerType;
  url: string;
  layerName?: string;
  basename?: string;
  minzoom?: number;
  maxzoom?: number;
  center?: number[];
  bounds?: number[];
  tiles?: string[];
}
//userconfig map
// 1. สร้าง Schema ไว้ใช้ดักจับข้อมูลผิดๆ (Runtime Validation)
export const CreateMapConfigBody = t.Object({
  intentName: t.String({ description: 'view_map' }),
  provider: t.Union([ t.Literal('GISTDA'), t.Literal('VALLARIS')]),
  baseUrl: t.Union([t.Literal('https://api-gateway.gistda.or.th'),t.Literal('https://vallaris.dragonfly.gistda.or.th')]),
  urlTemplate: t.Union([t.Literal('/api/2.0/resources/maps/{hazard}/{dayPath}/{type}'),t.Literal('/core/api/tiles/1.0-beta/tiles/{layerId}/{z}/{x}/{y}')]),
  layerConfigTemplate: t.Optional(t.Any())
});

// 2. [พระเอกอยู่ตรงนี้!] สกัด Type ออกมาให้ Frontend หรือส่วนอื่นๆ เรียกใช้งาน (Static Type)
// Frontend สามารถ import ตัวนี้ไปดูโครงสร้างได้เลย หน้าตาจะเหมือน Interface 100%
export type CreateMapConfigDTO = Static<typeof CreateMapConfigBody>;

//userapikey
export const CreateApiKeyBody = t.Object({
  provider: t.Union([
    t.Literal('GISTDA'), 
    t.Literal('VALLARIS')
  ]),
  keyName: t.String({ 
    default: 'Production Key' 
  }),
  keyValue: t.String({ }) 
});

// สกัด Interface ให้ไฟล์อื่นนำไปใช้
export type CreateApiKeyDTO = Static<typeof CreateApiKeyBody>;
