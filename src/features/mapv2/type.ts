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
