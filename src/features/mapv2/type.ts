import { t, Static } from 'elysia';

export type MapLayerType = string;
export type HazardType = string;
export type DayRange = number;

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
// 
//userapikey
export const CreateApiKeyBody = t.Object({
  provider: t.String({ minLength: 1 }),
  keyName: t.String({ 
    minLength: 1,
    default: 'Production Key'
  }),
  keyValue: t.String({ minLength: 1 })
});

// สกัด Interface ให้ไฟล์อื่นนำไปใช้
export type CreateApiKeyDTO = Static<typeof CreateApiKeyBody>;

export const apiKeyParamsSchema = t.Object({
  apiKeyId: t.String({ error: "ต้องระบุ apiKeyId ใน URL" })
});
export type ApiKeyParams = Static<typeof apiKeyParamsSchema>;

export const updateApiKeyBodySchema = t.Object({
  keyName: t.Optional(t.String({ minLength: 1 })),
  isActive: t.Optional(t.Boolean())
});
export type UpdateApiKeyDTO = Static<typeof updateApiKeyBodySchema>;

export const apiKeySummarySchema = t.Object({
  id: t.String(),
  provider: t.String(),
  keyName: t.String(),
  maskedKey: t.String(),
  isActive: t.Boolean(),
  createdAt: t.Any()
});
export type ApiKeySummaryDTO = Static<typeof apiKeySummarySchema>;

export const createdApiKeySchema = t.Object({
  id: t.String(),
  provider: t.String(),
  keyName: t.String(),
  isActive: t.Boolean(),
  createdAt: t.Any()
});
export type CreatedApiKeyDTO = Static<typeof createdApiKeySchema>;

export const apiKeyDetailSchema = t.Object({
  id: t.String(),
  provider: t.String(),
  keyName: t.String(),
  maskedKey: t.String(),
  isActive: t.Boolean(),
  createdAt: t.Any(),
  apiKey: t.String()
});
export type ApiKeyDetailDTO = Static<typeof apiKeyDetailSchema>;

export const deleteApiKeyResponseSchema = t.Object({
  message: t.String()
});
export type DeleteApiKeyDTO = Static<typeof deleteApiKeyResponseSchema>;

export type MapToolArgs = {
  intentName?: string;
  provider?: string;
  params?: unknown;
  options?: unknown;
  selectedOptions?: unknown;
  variables?: unknown;
  [key: string]: unknown;
};

export type MapOptionChoice = {
  label: string;
  value: string;
  description?: string;
  url?: string;
  type?: string;
  styleId?: string;
  styleTitle?: string;
  templated?: boolean;
  mediaType?: string;
  rel?: string;
};

export type MapOptionInfo = {
  key: string;
  required: boolean;
  source: "template" | "map_access";
  label?: string;
  description?: string;
  choices?: MapOptionChoice[];
};

export type MapConfigForTools = {
  intentName: string;
  provider: string;
  urlTemplate: string;
  layerConfigTemplate: unknown;
};