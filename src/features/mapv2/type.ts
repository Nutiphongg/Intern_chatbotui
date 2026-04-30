export type MapLayerType = "wms" | "wmts" | "tms" | "vector";
export type HazardType = "viirs" | "flood" | "dri";
export type DayRange = 1 | 3 | 7 | 30;

export type GetMapLayerCatalogInput = {
  hazard: HazardType; 
  days: DayRange;
  type: MapLayerType;
}

export type MapLayerCatalogResult = {
  hazard: string;
  days: number;
  type: MapLayerType;
  url: string;
  layerName?: string;
}
